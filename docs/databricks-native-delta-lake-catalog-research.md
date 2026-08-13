# Apache Doris Native Delta Lake Catalog 调研

> 以 Databricks Unity Catalog 作为第一个 catalog adapter
>
> 状态：调研结论与实现路线建议，不是最终接口承诺
>
> 调研更新：2026-08-13
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Doris 原生 Delta Lake Catalog 和 Databricks Delta 外部访问；Databricks Iceberg、External Location 问题见[独立调研](databricks-iceberg-unity-catalog-research.md)。

## 1. 结论摘要

1. **Doris 要成为一个外部 Delta client。** 目标不是增加一个 Databricks 专用文件 scanner，而是实现通用的 native `deltalake` catalog；Unity Catalog 是第一个 catalog adapter，以后可以扩展 path、HMS、Glue 等 metadata source。

2. **Doris 当前没有 native Delta Lake Catalog。** 现有公开方案依赖实验性的 Trino Delta Lake connector plugin。它可以作为过渡方案和功能参考，但没有形成 Doris 自己的 Unity 表发现、temporary credentials、Delta snapshot/protocol、deletion vector 和 catalog commits 闭环。

3. **Databricks managed Delta 的标准外部访问主线是 Unity REST，而不是猜测对象存储路径。** Doris 通过 UC 按 `catalog.schema.table` 发现表、获取能力和短期 table credentials，再使用 Delta 协议读取 `_delta_log`、checkpoint 和 Parquet。显式 path 模式只面向 external/unmanaged 数据及用户独立提供的存储权限。

4. **catalog-managed Delta 不能被实现成普通 `_delta_log` path scanner。** 开启 catalog commits 后，Unity Catalog 是提交协调和表状态的事实来源。正确读取需要处理 catalog 持有的 log tail；正确写入需要通过 catalog commits 协调版本和冲突，不能绕过 UC 直接抢占下一个 JSON log 版本。

5. **建议优先复用 Delta Kernel，不要重新手写不断演进的 Delta protocol，也不要采用已弃用的 Delta Standalone。** 但 Kernel 不会自动补齐 Unity REST、Doris 身份模型和三云凭证。Java Kernel 放 FE 还是 Rust Kernel FFI 放 BE，需要通过 PoC 验证 `transformPhysicalData`、deletion vector 和 FE/BE snapshot 一致性的边界。

6. **第一阶段应先交付正确的 native read，再逐级开放 write。** read 至少覆盖 checkpoint、reader features、column mapping、schema evolution、deletion vectors、time travel、catalog-held log tail 和 AWS/Azure/GCP 临时凭证；write 先做 external Delta，再做仍受 Databricks 预览状态约束的 catalog-managed write。

7. **default storage 是当前明确例外。** Databricks 不为其中的 UC 表提供外部 Delta/Iceberg FileIO 和 credential vending。Doris 即使能列出表，也不能凭长期云密钥把这类 managed table 变成可直读路径。

## 2. 先把四种 Delta 模式分开

| 术语 | 含义 | Doris 访问要求 |
| --- | --- | --- |
| Managed Delta | UC 管理表位置和生命周期 | 按表名经 Unity REST 获取表状态和 table credentials |
| External Delta | UC 登记用户管理的外部路径 | 可经 Unity REST；显式 path 模式需要独立存储权限 |
| Catalog-managed Delta | 启用 `catalogManaged` table feature，由 catalog commits 协调提交 | 读取 catalog-held log tail；写入必须参与 catalog commits |
| Path Delta | 用户显式给出 URI，绕开 UC 的 catalog discovery | 仅适用于 external/unmanaged 数据，不作为 managed 模式 fallback |

**Managed Delta 与 catalog-managed Delta 不是同义词。** 前者描述表的生命周期；后者描述事务提交协调机制。一个 managed table 是否启用 catalog commits，必须根据 table feature/capability 判断。

### 2.1 当前 Databricks 外部访问矩阵

下表是截至本调研日期的官方公开口径；Preview/Beta 状态可能变化，实施前需要重新核对。

| 表/访问模式 | 外部读 | 外部写/建表 | 关键约束 |
| --- | --- | --- | --- |
| Managed Delta，经 Unity REST | 是 | Create/Write 为 Public Preview | 写入仅适用于启用 catalog commits 的表；外部客户端和 feature 有限制 |
| External Delta，经 Unity REST | 是 | 是 | UC 可提供 table/path credentials；写入需符合 external table 权限和并发要求 |
| External/unmanaged Delta path | 是 | 取决于 Doris format engine | 用户自行提供 storage location 和权限，不继承完整 UC 治理 |
| Delta with Iceberg reads / `IcebergCompatV3` | 经 Iceberg REST 只读 | 不通过该路径写 Delta | 当前 Unity REST Delta client 不支持；路由到 Iceberg 文档覆盖的只读链路 |
| Default-storage managed Delta | 外部 FileIO 不支持 | 不支持 | 可考虑 JDBC/ODBC、能力受限的 OpenSharing 等由 Databricks 参与的数据路径 |
| Compatibility Mode 副本 | 只读 | 否 | 面向不支持 REST 的旧客户端，不是 native Delta Catalog 主线 |
| Delta Sharing / OpenSharing | 只读分享 | 否 | 适合分享，不等价于完整 UC catalog browse/read/write |

## 3. Doris 作为外部 Delta client 需要做什么

外部 Delta client 包含控制面和格式/数据面两部分，缺一不可：

```text
                              控制面
Doris FE ── OAuth/PAT ─────────────────────────> Unity Catalog
   │        list/get table、table ID、location、capability
   │        temporary table/path credentials
   │        catalog-held log tail / catalog commits
   │
   │                   Delta snapshot / scan planning
   ├────────────────── Delta Kernel
   │
   │ scan ranges + logical/physical mapping + DV + credential handle
   ▼
Doris BE native Parquet reader ─────────────────> S3 / ADLS / GCS
                              数据面
```

### 3.1 Unity REST：表发现与能力判断

Doris 首先用三段式名称访问 Unity Catalog，而不是让用户输入 managed table 的底层 URI。Unity client 至少需要：

- 列举 catalog、schema 和 table；
- 获取 table ID、location、data source format 和 Delta metadata；
- 获取 external-engine read/write capability；
- 识别普通 managed、external 和 catalog-managed table；
- 处理 OAuth/PAT、token refresh、权限错误和 API capability 演进。

外部访问要求 metastore 开启 external data access。读取通常需要 `EXTERNAL USE SCHEMA`、`USE CATALOG`、`USE SCHEMA` 和 `SELECT`；写入还需要 `MODIFY`，建表需要相应 schema 权限。只有 external/path 操作才涉及 `EXTERNAL USE LOCATION` 等 location 权限。

### 3.2 Temporary table/path credentials

Unity Catalog credential vending 签发短期、范围受限的云存储凭证：

- AWS：STS session credentials；
- Azure：ADLS SAS；
- GCP：短期 OAuth token。

table credential 以 table ID 和 `READ`/`READ_WRITE` 为范围；path credential 使用 `PATH_READ`、`PATH_READ_WRITE`、`PATH_CREATE_TABLE` 等操作，服务于 external location/path，不能拿来绕过 managed table。

Doris 需要把下面的信息作为一个整体管理：

```text
principal + table/path + operation + cloud + credential material + expiration
```

凭证不是“初始化 catalog 时拿一次”就结束。查询排队、长查询、retry 和写入可能跨过默认 TTL；执行层必须得到 expiration/refresh handle，并在刷新时重新校验权限和 table capability。

### 3.3 Delta 格式语义

Delta 的核心元数据位于 `_delta_log`，包括 JSON commits 和 Parquet checkpoints。native read 不只是列出 Parquet 文件，还需要正确处理：

- protocol 与 reader table features；
- classic/v2 checkpoints 和 log replay；
- snapshot isolation 与 version/timestamp time travel；
- schema evolution、name/id column mapping；
- partition values、statistics/data skipping、residual filters；
- deletion vectors；
- physical data 到 logical data 的转换。

只实现“读取几个 Parquet 文件”不能叫 Delta 支持。未知的 required reader feature 必须在读取文件前明确拒绝，不能返回看似成功但错误的结果。

### 3.4 Catalog commits 为什么是正确性边界

传统 Delta writer 通常从对象存储读取当前 log version，写数据文件，然后尝试创建下一个 JSON commit。开启 [catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits) 后，提交协调转移到 Unity Catalog：

```text
外部 Delta client
    │ 读取 catalog-held log tail / 提交事务
    ▼
Unity Catalog（版本、冲突、事务事实来源）
    │
    ▼
对象存储中的 _delta_log 与 data files
```

因此：

- read 如果只扫描对象存储 `_delta_log`，可能遗漏 catalog 中尚未发布的 log tail，得到过期或错误 snapshot；
- write 如果直接创建 `_delta_log/<next-version>.json`，会绕过 catalog 的版本分配、冲突检查和多表事务；
- catalog-managed table 的 read 与 commit adapter 必须是 native Delta Catalog 的一等能力。

截至本调研日期，catalog commits 核心能力已经 GA，但 external access、外部 managed Delta create/write、特定 client integration 仍有 Beta/Public Preview/experimental 状态差异，不能把这些状态合并成一个“全部 GA”结论。managed write 应由显式实验开关和服务端 capability 保护。

### 3.5 治理策略与 default storage

普通 credential vending 不能执行 row filter/column mask：把文件凭证交给 Doris 后，Doris 并不知道 Databricks 策略的过滤结果。Databricks 的 cross-engine ABAC 通过 server-side planning 解决此问题，但要求特定 client/version。Doris 在实现该协议前，应明确拒绝这些策略表，不能凭文件权限绕过治理。

[Default storage](https://docs.databricks.com/aws/en/storage/default-storage) 当前不支持外部 Delta/Iceberg FileIO 和 credential vending。此限制来自 Databricks 的存储访问模型，不是给 Doris 增加 account key/SAS 就能解除的客户端配置问题。

## 4. Doris 当前能力与缺口

### 4.1 现有方案为什么不是 native

Doris 当前 [Delta Lake Catalog 文档](https://doris.apache.org/docs/4.x/lakehouse/catalogs/delta-lake-catalog/) 把能力标为实验性，并要求把兼容版本的 Trino Delta Lake plugin 部署到每个 FE/BE。仓库示例使用：

```sql
CREATE CATALOG delta_lake PROPERTIES (
  "type" = "trino-connector",
  "trino.connector.name" = "delta_lake"
);
```

这条路径的问题不是“完全不能读 Delta”，而是它没有形成目标架构：

- 部署和版本受外部 Trino plugin 约束；
- 不是 Doris native scan/planning pipeline；
- 当前 Doris FE/BE 没有独立 Delta snapshot、protocol、DV 实现；
- 没有 Unity temporary credentials 和 catalog commits 闭环；
- 没有 Databricks managed/external Delta 的三云认证矩阵；
- 公开方案不承诺 native write-back。

### 4.2 要补齐的能力层

| 层次 | 需要新增的主要能力 |
| --- | --- |
| Catalog SPI | 通用 `deltalake` catalog；Unity 作为第一个 adapter，未来支持 path/HMS/Glue |
| Unity control plane | table discovery、capability、OAuth、temporary credentials、catalog-held log tail/commits |
| Delta protocol | checkpoint/log、reader features、snapshot/time travel、column mapping、DV、schema evolution |
| FE planning/cache | statement snapshot pinning；以 catalog/principal/table ID/version 为 key；凭证不跨 principal |
| BE execution | native Parquet、DV row selection、logical/physical mapping、missing/default column 和 residual filter |
| Credential lifecycle | AWS STS、Azure SAS、GCP OAuth；expiration、refresh、retry、权限撤销和 fail-closed |
| Writer/committer | staged files、optimistic concurrency、external commit、catalog-managed catalog commit |
| Certification | Databricks/Spark 对照；三云；managed/external；feature 和 negative matrix |

## 5. 竞品调研：只看 Databricks Delta

### 5.1 总表

| 产品 | Delta 实现与 UC 路径 | Managed/External | 写入与 commits | 对 Doris 的启示 |
| --- | --- | --- | --- | --- |
| Apache Spark + Delta/UC connector | Delta Spark 4.3+ 配合 Unity Catalog connector 0.5+ | external R/W；managed read；managed create/write 为 Preview | catalog-managed write 走 catalog commits；OAuth M2M 支持长任务刷新 | Databricks 官方参考数据流和正确性基线 |
| Starburst Enterprise | 原生 Delta connector + Unity Catalog adapter | external 功能完整；普通 managed 只读；catalog-managed 有实验能力 | external DML 完整度高；catalog-managed DML 受 Preview/experimental 限制 | 最接近 Doris 目标：native log、credential refresh、feature matrix、catalog commits |
| ClickHouse OSS/Cloud | DataLakeCatalog + Delta engine，主要非 Azure 路径迁往 Rust Delta Kernel | 官方 UC 指南主要支持 external location；managed Delta 不支持 | Delta INSERT/UC create 等文档状态有冲突；无 managed catalog commits 证据 | 采用 Kernel 的工程方向正确；列 catalog 不代表 managed data 可读 |
| Snowflake | Delta Sharing、Delta Direct、Iceberg reads 等多条只读路径 | 以分享、path 或兼容 metadata 为主 | 未发现等价的 native UC managed Delta write catalog | 证明只读集成有多种路线，但不能代替 native managed Delta client |
| Doris 当前 | 实验性 Trino compatibility plugin | 没有认证的 Unity managed Delta native 路径 | 无 native catalog commits | 先完成 native read 正确性，再逐级开放 write |

### 5.2 Apache Spark：官方参考客户端

Spark core 本身不内置 Unity Catalog client。Databricks 当前参考组合是 Delta Spark + Unity Catalog Spark connector，并把 Apache Spark 列为 Unity REST 的正式支持客户端。它形成完整分工：

```text
UC connector：表发现、权限、临时凭证、catalog interaction
Delta Spark：Delta protocol、snapshot、read/write semantics
Cloud filesystem：使用 scoped temporary credentials 访问文件
```

这应成为 Doris cross-engine correctness 的基准，但不意味着 Doris 必须采用 Spark 执行。Doris 可以用 Delta Kernel 加 native Parquet data path 实现相同协议语义。

### 5.3 Starburst Enterprise：最接近目标的商业实现

Starburst Enterprise 文档明确区分：

- external Delta 可 read/create/insert/update/delete/merge；
- 未启用 catalog commits 的普通 managed Delta 只读；
- catalog-managed Delta 可读取，并在开关下实验性支持部分 DML；
- S3/GCS vended credentials 可按 table ID/location 限定，并在查询期间刷新。

这说明 `catalog.type=unity` 只是入口。商业级实现还必须同时具备 Delta transaction log、table features、credential lifecycle、server-side planning 和 catalog commits。Starburst 与 Databricks 页面在 Preview/experimental 用词和更新时间上存在差异，Doris 应以运行时 capability 和真实互操作测试为准。

upstream Trino 的通用 Delta connector 使用 HMS/Glue 等 metadata source，不会因为 Doris 已能加载 Trino plugin 就自动获得 Starburst Enterprise 的 Unity adapter、managed credential vending/refresh 或 catalog commits。

### 5.4 ClickHouse：Delta Kernel 的工程信号与边界

ClickHouse 曾自行解析 Delta transaction log，后将主要路径迁移到官方 Rust Delta Kernel，以减少协议演进、deletion vectors 和 schema evolution 的维护负担。这支持 Doris 优先复用 Kernel 的判断。

但 ClickHouse 也显示出两个边界：

- Azure 路径因 known issues 不是 Kernel 的无条件覆盖；
- Unity Catalog 指南主要支持 external storage locations，并明确不支持该路径下的 managed Delta。

其总支持矩阵、writing guide 和 Unity 指南对 create/insert/Beta 的说法并不完全一致。保守结论应是 external read 已有公开支持；不能把它推导成 managed Delta、完整 DML 或 catalog commits 已支持。

### 5.5 Snowflake：只读替代路线不是 native Delta

Snowflake 对 Databricks Delta 主要提供 Delta Sharing、Delta Direct/object-store path 和 Delta-to-Iceberg reads 等只读路径。这些适合分享、迁移或只读分析，但没有证据表明它提供等价的 Unity managed Delta native write catalog。

它对 Doris 的价值是帮助区分两个目标：如果只要尽快读取一份数据，Sharing/兼容副本/path 可以是产品选项；如果目标是按 UC 表名访问 managed Delta 并最终互操作写入，就仍需 native Unity + Delta protocol + catalog commits。

## 6. 推荐的 Doris 目标架构

### 6.1 总体原则

1. **Delta format 与 catalog adapter 解耦**：Kernel 负责 Delta protocol；Unity client 负责表发现、授权、credentials 和 commits。
2. **数据面保持 Doris native**：尽量复用 BE vectorized Parquet reader、filter、projection 和 cache。
3. **capability 驱动**：按 table protocol/features 与 UC capability 接受或拒绝，不能只靠 Databricks 版本号猜测。
4. **managed 模式 fail-closed**：Unity 请求或 credential vending 失败时，不静默猜路径或回退到范围更大的长期凭证。
5. **statement snapshot 一致**：同一 SQL 的 planning、retry 和所有 scan ranges 绑定同一 Delta snapshot/version。
6. **read 与 write 分级**：先以 cross-engine 结果一致性证明 read，再开放风险更高的 commit/DML。

### 6.2 Catalog 接口方向

下面仅表示设计方向，不是最终 SQL 接口：

```sql
CREATE CATALOG dbx_delta PROPERTIES (
  "type" = "deltalake",
  "deltalake.catalog.type" = "unity",
  "unity.uri" = "https://<workspace-host>",
  "unity.auth.type" = "oauth2",
  "unity.oauth2.client-id" = "...",
  "unity.oauth2.client-secret" = "..."
);
```

| 模块 | 职责 |
| --- | --- |
| `DeltaLakeCatalog` SPI | 通用 Delta catalog 生命周期和 table abstraction |
| Unity adapter | catalog/schema/table；ID/location/capability；OAuth；temporary credentials；catalog commits |
| Delta Kernel adapter | protocol negotiation；checkpoint/log；snapshot/time travel；file planning；statistics；DV descriptor |
| FE metadata/cache | principal-aware cache；statement snapshot pin；credential handle 和 expiration |
| Scan planner | file ranges、partition、logical/physical schema、DV、residual、version |
| BE reader | native Parquet + Kernel 要求的 transform/DV/missing-column semantics |
| Writer/committer | staged data、external Delta commit、catalog-managed commit、retry/idempotency |

### 6.3 Delta Kernel：Java 还是 Rust

[Delta Kernel](https://docs.delta.io/delta-kernel/) 是面向 connector 的官方 Java/Rust 库，覆盖 scan planning、schema transform、data skipping、deletion vectors 等协议细节。不要采用已经 [deprecated 的 Delta Standalone](https://docs.delta.io/delta-standalone/)。

Kernel 并不等于完整 Databricks connector：Doris 仍需实现 Unity REST/auth/credential vending。Java 的 Unity catalog-managed client 仍带实验性边界；Rust Unity integration/FFI 的内建云凭证覆盖也不是三云完整成品。

| PoC 方案 | 优点 | 必须证明的风险 |
| --- | --- | --- |
| FE Java Kernel planning，向 BE 序列化 transform descriptor | 贴合现有 FE catalog/planner；可评估复用 Java UC 组件 | Kernel state 能否稳定序列化；大 snapshot FE 内存；BE 是否会重复实现 `transformPhysicalData` 并产生协议漂移 |
| BE 嵌入 Rust Kernel FFI，FE 做 Unity control plane | Kernel 靠近 native data path；避免跨层复制复杂 transform | Rust/C++ ABI 和构建体积；FE/BE snapshot 一致；Arrow/Block 转换；Azure/GCP credentials 仍需补齐 |

Kernel API 不只返回文件列表。读完 physical Parquet 后还可能要求 `Scan.transformPhysicalData` 生成 logical data 和 deletion selection vector。架构冻结前必须证明这一转换由谁执行；不能为了复用 BE Parquet reader 而重新手写一套会漂移的 Delta protocol。

### 6.4 第一版 native read 的最低正确性集合

- classic 与 v2 checkpoints；
- reader protocol/features 协商，未知 required feature 明确拒绝；
- name/id column mapping 与 schema evolution；
- deletion vectors；
- partition pruning、statistics/data skipping 与 residual filter；
- snapshot isolation、version/timestamp time travel；
- Unity managed/external table credentials；
- catalog-managed table 的 catalog-held log tail；
- AWS S3、Azure ADLS、GCS；
- generated/identity/default columns、variant/type widening 等 feature 有正确语义或精确拒绝。

CDF、streaming、OPTIMIZE/VACUUM、Delta Sharing 和高级写入可以后续迭代，不阻挡第一版 snapshot read。

### 6.5 Write 开放顺序

1. external Delta append；
2. external Delta CTAS/CREATE；
3. external Delta DELETE/UPDATE/MERGE；
4. catalog-managed Delta append/create，通过 catalog commits；
5. catalog-managed DML；
6. 评估维护操作和更多 table features。

每一阶段都需要 conflict、concurrency、idempotency 和 orphan-file 测试。不能复用“写 `_delta_log/<next-version>.json`”的 path writer 处理 catalog-managed table。Databricks 仍处于 Preview 的能力必须由显式实验开关保护。

## 7. 身份、权限、凭证与安全边界

### 7.1 两种身份模式

| 模式 | UC 看到的 principal | 优点 | 限制 |
| --- | --- | --- | --- |
| Catalog service principal | 所有 Doris 查询共用服务身份 | 部署和 refresh 简单 | UC 不知道具体 Doris 用户；UC ACL 不能代替 Doris RBAC |
| Delegated user/session identity | 每个 Doris session 的 UC user/token | 保留 UC 用户级授权和审计 | token exchange、cache isolation、expiry/retry 更复杂 |

metadata 的非敏感部分可在满足权限语义时共享；temporary credentials 不得跨 principal 共享。cache key 至少包含远端 catalog、identity mode、principal、table ID 和 snapshot/version。

### 7.2 安全与正确性要求

- OAuth secret、PAT、SAS/session token 不进入 edit log、profile、普通日志或 `SHOW CREATE CATALOG` 明文；
- temporary credential 保持最小 scope/TTL，过期前刷新，权限撤销后不能继续获取新凭证；
- vending/Unity managed 模式 fail-closed，不回退到 guessed URI 或更宽的 static credential；
- `unity.uri` 是与现有 REST catalog 类似的 FE 出站 URL，沿用高权限 `CREATE CATALOG` 和现有 egress/SSRF 信任边界；
- FE 到 BE 在现有威胁模型中属于受信内部链路，但仍应最小化凭证范围、寿命和日志暴露；
- 新 connector 实现前，应按 `threat-model.md` 的 new catalog connector trigger 复核威胁模型和凭证数据流。

## 8. 分阶段交付计划

### M0：协议、Kernel 与三云 PoC

- 建立 AWS/Azure/GCP Databricks compatibility lab；
- 捕获经过结构化脱敏的 Unity table/capability/credential/catalog-tail fixtures；
- 对比 Java Kernel FE 与 Rust Kernel BE 两种方案；
- 用 Databricks/Spark 生成 checkpoint、column mapping、DV、schema evolution 样本；
- 验证 default storage 和策略表的 negative behavior。

### M1：Native external/managed Delta read

- 新增 `deltalake` catalog SPI 和 Unity adapter；
- table discovery、OAuth、temporary credentials 和 statement snapshot；
- checkpoint/log replay、protocol/features、column mapping、DV；
- BE native Parquet data path 和三云 read；
- 对 catalog-managed table 读取 catalog-held log tail。

### M2：Read hardening 与性能

- credential refresh、长查询、retry、FE failover、权限撤销；
- time travel、schema evolution、feature rejection；
- principal-aware metadata/credential cache；
- planning latency、log/checkpoint 规模、文件数、DV 比例和 cold/warm cache baseline。

### M3：External Delta write

- append、CTAS/create，再扩展 DELETE/UPDATE/MERGE；
- optimistic concurrency、idempotency、orphan cleanup；
- 与 Databricks/Spark writer 并发互操作验证。

### M4：Catalog-managed write

- 通过 catalog commits 实现 managed append/create；
- 再开放受支持的 DML；
- 以服务端 capability 和 Preview 状态加实验开关；
- 验证 conflict、多表/事务边界和失败恢复。

### M5：高级治理与生态

- cross-engine ABAC server-side planning；
- CDF、更多 Delta table features 和 Delta Sharing；
- delegated identity、private connectivity 和完整审计。

## 9. 验收标准

1. 同一 table version 上，Doris 与 Databricks/Spark 的 row count、schema、null/default 和 DV 删除结果一致；
2. catalog-managed table 含 catalog-held log tail 时，Doris 读取正确 snapshot；
3. column mapping、v2 checkpoint、DV、schema evolution 和 time travel 有正向回归；
4. 每个未知/不支持的 required reader feature 都在读取数据文件前明确拒绝；
5. 三云 managed/external Delta 均无需在 catalog 中保存长期 storage credential；
6. credential expiry、权限撤销、FE failover 和 BE retry 不造成跨用户访问或错误 snapshot；
7. default storage、策略表和不支持的 table feature 给出可操作且不泄密的错误；
8. write 阶段与 Databricks/Spark 并发提交无 lost update，失败重试幂等；
9. 无 catalog/storage secret 出现在 edit log、profile、普通日志和错误文本；
10. 建立 planning、large log/checkpoint、文件规模、DV 比例、cache 与 refresh 的性能基线。

## 10. PoC 必须关闭的未知项

1. Databricks 三云和不同 storage 配置下的 table capability、temporary credential 与 refresh 差异；
2. Java Kernel Unity adapter 对当前 catalog-managed reads/commits 的覆盖和兼容窗口；
3. Rust Kernel FFI/Unity integration 的三云 credential、refresh 和 transform 边界；
4. `Scan.transformPhysicalData` 能否可靠序列化到 Doris BE，还是必须在 BE 内执行；
5. 普通 managed 与 catalog-managed table 的 feature/capability 判定，以及 catalog-held tail API 的版本稳定性；
6. managed Delta Public Preview 对第三方客户端允许的 table features、DDL/DML 和并发限制；
7. cross-engine ABAC server-side planning 的协议、性能和支持对象；
8. Azure/GCP private endpoint 与 FE/BE 网络拓扑；
9. catalog service principal 与 delegated identity 的产品默认值及迁移方式。

## 11. 主要官方资料

### Apache Doris

- [Doris Delta Lake Catalog（Trino Connector compatibility）](https://doris.apache.org/docs/4.x/lakehouse/catalogs/delta-lake-catalog/)

### Databricks 与 Delta Lake

- [External access overview](https://docs.databricks.com/aws/en/external-access)
- [Unity REST access for Delta clients](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Enable external access](https://docs.databricks.com/aws/en/external-access/admin)
- [Credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Temporary table credentials API](https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials)
- [Temporary path credentials API](https://docs.databricks.com/api/workspace/temporarypathcredentials/generatetemporarypathcredentials)
- [Get table API](https://docs.databricks.com/api/workspace/tables/get)
- [Catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits)
- [Default storage limitations](https://docs.databricks.com/aws/en/storage/default-storage)
- [Cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- [Delta Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads)
- [Compatibility Mode](https://docs.databricks.com/aws/en/external-access/compatibility-mode)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
- [Delta Kernel Unity integration](https://docs.delta.io/kernel/rust/unity_catalog/overview.html)
- [Delta Kernel catalog-managed reads](https://docs.delta.io/kernel/rust/catalog_managed/reading.html)
- [Delta Kernel Rust FFI](https://docs.delta.io/kernel/rust/ffi/overview.html)
- [Delta Kernel Java `UCCatalogManagedClient`](https://github.com/delta-io/delta/blob/master/kernel/unitycatalog/src/main/java/io/delta/kernel/unitycatalog/UCCatalogManagedClient.java)
- [Delta transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Delta Standalone deprecation](https://docs.delta.io/delta-standalone/)

### 竞品

- [Starburst Enterprise Delta Lake with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-delta-lake-unity.html)
- [Starburst Enterprise Delta Lake connector](https://docs.starburst.io/481-e/connector/delta-lake.html)
- [Trino Delta Lake connector](https://trino.io/docs/483/connector/delta-lake.html)
- [ClickHouse DataLakeCatalog](https://clickhouse.com/docs/reference/engines/database-engines/datalake)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse DeltaLake engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/deltalake)
- [ClickHouse integration with Rust Delta Kernel](https://clickhouse.com/blog/integrating-rust-delta-kernel)
- [Snowflake Delta Sharing catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing)
- [Snowflake Object Store / Delta Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage)
