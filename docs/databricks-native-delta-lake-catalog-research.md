# Apache Doris Native Delta Lake Catalog 调研

> 以 Databricks Unity Catalog 作为第一个 catalog adapter
>
> 状态：调研结论与实现路线建议，不是最终接口承诺
>
> 调研更新：2026-08-14
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Doris 原生 Delta Lake Catalog 和 Databricks Delta 外部访问；Databricks Iceberg、External Location 问题见[独立调研](databricks-iceberg-unity-catalog-research.md)。

## 1. 结论摘要

1. **Doris 要成为一个外部 Delta client。** 目标不是增加一个 Databricks 专用文件 scanner，而是实现通用的 native `deltalake` catalog；Unity Catalog 是第一个 catalog adapter，以后可以扩展 path、HMS、Glue 等 metadata source。

2. **Doris 当前没有 native Delta Lake Catalog。** 现有公开方案依赖实验性的 Trino Delta Lake connector plugin。它可以作为过渡方案和功能参考，但没有形成 Doris 自己的 Unity 表发现、temporary credentials、Delta snapshot/protocol、deletion vector 和 catalog commits 闭环。

3. **Databricks managed Delta 的标准外部访问主线是 Unity REST，而不是猜测对象存储路径。** Doris 通过 UC 按 `catalog.schema.table` 发现表、获取能力和短期 table credentials，再使用 Delta 协议读取 `_delta_log`、checkpoint 和 Parquet。显式 path 模式只面向 external/unmanaged 数据及用户独立提供的存储权限。

4. **catalog-managed Delta 不能被实现成普通 `_delta_log` path scanner。** 开启 catalog commits 后，Unity Catalog 是提交协调和表状态的事实来源。正确读取需要处理 catalog 持有的 log tail；正确写入需要通过 catalog commits 协调版本和冲突，不能绕过 UC 直接抢占下一个 JSON log 版本。

5. **建议优先复用 Delta Kernel，不要重新手写不断演进的 Delta protocol，也不要采用已弃用的 Delta Standalone。** Delta 4.3 已提供 Java `delta-kernel-unitycatalog`，Rust Kernel 也有 Unity Catalog/catalog-managed companion crates，可以复用一部分 UC 与 commit 流程；但它们不会自动补齐 Doris 身份模型、三云生产适配和 FE/BE 执行边界。Java Kernel 放 FE 还是 Rust Kernel FFI 放 BE，需要通过 PoC 验证 `transformPhysicalData`、deletion vector 和 statement snapshot 一致性。

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

本节把证据分为三层：产品正式文档表示厂商当前承诺的支持范围；开源仓库 `master` 只能说明在研实现；根据协议和源码作出的判断会显式标注为“推断”。不能把 `master`、Beta/Preview 或一个合并格式的支持矩阵直接写成稳定产品能力。

### 5.1 总表

| 产品 | Delta 实现与 UC 路径 | Managed/External | 写入与 commits | 对 Doris 的启示 |
| --- | --- | --- | --- | --- |
| Apache Spark + Delta/UC connector | 常规路径是 Delta Spark 4.3+ 配合 Unity Catalog connector 0.5+；不是以 Delta Kernel 为主实现 | external R/W；managed read；managed create/write 为 Preview | catalog-managed write 走 catalog commits；OAuth M2M 支持长任务刷新 | Databricks 官方参考数据流和正确性基线 |
| Starburst Enterprise | Trino 自研 Delta 格式栈 + SEP 商业 Unity Catalog adapter | external 支持文档列出的 CRUD/DML；普通 managed 只读；catalog-managed 有实验能力 | catalog-managed DML 受 Preview/experimental 限制 | 最接近 Doris 目标：native log、credential refresh、feature matrix、catalog commits |
| ClickHouse OSS/Cloud | DataLakeCatalog + DeltaLake engine + Rust Delta Kernel FFI；ClickHouse 原生读取 Parquet | 正式 UC 指南只承诺 external read；`master` 可发现 managed 类型并取得 READ credential，但未见 catalog-managed log-tail 集成 | 路径 Delta INSERT 为 Beta；native Unity adapter 未见 READ_WRITE/catalog commits | Kernel 与 native reader 的分层最值得参考，同时要避免把凭证前置链路等同于 managed 正确读取 |
| Snowflake | Delta Direct 原生解析 Delta log；Delta Sharing、Iceberg reads 是另外两条协议路径 | Sharing 可覆盖被分享的 managed/external；Direct 只认 path；UniForm 可读 managed/external | 当前公开 Delta 路径均只读；没有 Unity REST Delta catalog commits | 格式解析可借鉴，但不能替代 native Unity adapter |
| Doris 当前 | 实验性 Trino compatibility plugin | 没有认证的 Unity managed Delta native 路径 | 无 native catalog commits | 先完成 native read 正确性，再逐级开放 write |

### 5.2 Apache Spark：官方参考客户端

Spark core 本身不内置 Unity Catalog client。Databricks 当前参考组合是 Apache Spark 4.0/4.1、Delta Spark 4.3+ 与 Unity Catalog Spark connector 0.5+，并把 Apache Spark 列为 Unity REST 的正式支持客户端。它形成完整分工：

```text
unitycatalog-spark/UCSingleCatalog：Spark catalog 接入、name/namespace/table resolution、UC 调用入口
unitycatalog-hadoop：credential-scoped filesystem、三云 vended credential 续期
Delta Spark：Delta protocol、snapshot、read/write semantics，以及 UC managed 操作路由
Unity 服务端：鉴权、授权裁决和 catalog commit validation
```

Delta Spark 4.3 对 UC managed Delta 默认使用新的 UC Delta REST API，并把 load、create/CTAS、replace、DML 和 metadata update 路由到该 API。它同时提供了基于 Delta Kernel 的 DSv2 connector，但发布说明将这条路径标为 `Experimental`。因此，不能把 Spark 的常规生产路径描述成“Spark 直接使用 Delta Kernel”；这里承担完整 Delta 语义的是 Delta Spark。

这应成为 Doris cross-engine correctness 的基准，但不意味着 Doris 必须采用 Spark 执行。Doris 可以用 Delta Kernel 加 native Parquet data path 实现相同协议语义。

### 5.3 Starburst Enterprise：最接近目标的商业实现

Starburst Enterprise 的公开基础是 upstream Trino Delta connector。Trino 自己解析 `_delta_log`、checkpoint 和 snapshot，公开依赖中没有 Delta Kernel；SEP 在此基础上提供商业 Unity Catalog adapter。SEP 私有扩展没有公开源码，因此只能说公开证据支持“Trino 自研格式栈 + SEP Unity 扩展”，不能反向断言私有部分绝不复用 Kernel。

Starburst Enterprise 文档明确区分：

- external Delta 可 read/create/insert/update/delete/merge/drop；
- 未启用 catalog commits 的普通 managed Delta 只读；
- catalog-managed Delta 可 read/insert/update/delete/merge/drop，但不支持从 Starburst 创建，表需先在 Databricks 创建；该能力仍受实验开关和 Preview 限制；
- S3/GCS vended credentials 可按 table ID/location 限定，并在查询期间刷新；当前文档不能据此泛化到 Azure。

这说明选择 Unity 作为 metastore/catalog 只是入口。商业级实现还必须同时具备 Delta transaction log、table features、credential lifecycle 和 catalog commits；Doris 若要支持 Databricks row filter/column mask，还必须另外满足 server-side planning 的客户端要求。Starburst 与 Databricks 页面在 Preview/experimental 用词和更新时间上存在差异，Doris 应以运行时 capability 和真实互操作测试为准。

upstream Trino 的通用 Delta connector 使用 HMS/Glue 等 metadata source，不会因为 Doris 已能加载 Trino plugin 就自动获得 Starburst Enterprise 的 Unity adapter、managed credential vending/refresh 或 catalog commits。

### 5.4 ClickHouse：Delta Kernel 的工程信号与边界

ClickHouse 采用“Catalog 控制面 + 原生 Delta 表引擎 + 进程内 Delta Kernel”的分层方案。`DataLakeCatalog` 连接 Unity Catalog，负责 namespace/table 发现、表路径与短期存储凭证；`DeltaLake` 引擎通过 C++/Rust FFI 调用 Rust Delta Kernel，解析协议并构造 snapshot/scan，向 ClickHouse 暴露有效文件、统计信息、deletion vector 和 schema transform 信息；Parquet 读取、相关下游处理及查询执行仍由 ClickHouse 完成。Kernel 静态编入 ClickHouse 二进制，并非独立服务。

`DataLakeCatalog`、Unity adapter 和 Delta Kernel 集成本身可以在 ClickHouse OSS 中看到。公开资料未显示 ClickHouse Cloud 使用另一套 Delta reader；目前明确的 Cloud 增量主要包括托管连接 UI、Shared Catalog、无状态计算和 Cloud 专有分布式缓存。因此不能只凭 Cloud 产品名推导出额外的 Unity managed Delta 语义。

按公开产品文档，`DeltaLake` 引擎可附着并查询已有 Delta 表；INSERT 是 Beta/需开关，仅支持 S3、GCS，Azure 写入、创建空表以及 DELETE/UPDATE/MERGE 不支持。ClickHouse 的 Unity 专项指南目前只说明 external-storage 表，并把 native Delta 章节明确写成“Read Delta”。通用 support matrix 虽把 “Unity Catalog / Delta, Iceberg / Create、INSERT Beta” 合并为一行，但没有区分 native Unity-Delta 与经 Iceberg REST 接入的 Iceberg，因此不足以证明 Unity-Delta 写入已经受支持。

current `master` 源码比 Unity 指南更新：native Unity adapter 已接受 `TABLE_DELTA`（managed）和 `TABLE_DELTA_EXTERNAL`，并能获取 S3 temporary credentials 或 Azure SAS。ClickHouse Cloud 26.4 在 `Experimental Features` 下加入 Azure Delta Kernel，current master 也已启用该路径；但旧 support matrix 仍写 Azure 禁用 Kernel，存在尚未收敛的版本化文档冲突。凭证请求仍固定为 `operation=READ`，凭证重取 callback 明确只支持 S3，且未见 Unity `READ_WRITE`、建表或 managed table 所需 catalog commits。以上是源码与特定版本事实，不能外推为所有稳定版或 ClickHouse Cloud 环境的正式承诺。

这些源码只能证明表发现、location 和初始 READ credential 等前置链路。普通 managed 表是否能在具体发行版中正确读取仍需 PoC；对于 catalog-managed 表，尚未看到 ClickHouse 获取 Unity 持有的 log tail/latest catalog version 的证据，不能认定其已经满足协议正确读取。ClickHouse 自身尚未正式文档化 managed Delta，Databricks 支持客户端名单也没有 ClickHouse，因此应标为“待具体版本和云环境联调”。managed write 不能只靠路径级 Delta commit，还需要 Unity catalog commits；Azure 长查询中的 SAS 自动刷新也仍有源码层面的缺口。

### 5.5 Snowflake：有 native Delta log 读取，但没有 native Unity REST Delta catalog

Snowflake 的四条路径解决的是不同问题，不能合并称为 native Databricks Delta catalog：

- **Delta Sharing**：连接 provider 显式创建的 share，而不是浏览任意 Unity Catalog。它可以消费被分享的 managed/external Delta，使用 sharing server 返回的 vended credentials；对应的 catalog-linked database 明确只读，不能在其中 create table，也不能对共享表执行 insert/update，并且不涉及 catalog commits。
- **Delta Direct / object store**：Snowflake 直接读取对象存储中的 `_delta_log`、checkpoint 和 Parquet，再将其注册为 Snowflake Iceberg table。它确实具备 native Delta log 解析能力，但没有 Unity control plane、按 UC 表名发现、UC credential vending 或 catalog commits。该路径要求显式 storage path/external volume，且官方明确不支持从 Unity Catalog 的 Delta table definition 创建；Delta Direct table 只读。`ALLOW_WRITES=TRUE` 只允许写派生的 Iceberg metadata，不是 Delta data/log write。
- **Iceberg reads / UniForm**：managed/external Delta 开启 Iceberg reads 后，由 Databricks 生成 Iceberg metadata；Snowflake 经 Unity Iceberg REST，并可使用 UC vended credentials（也可配置 external volume），作为 Iceberg client 读取。Databricks 矩阵明确两类 Delta 都是 read-only。这条链路访问 UC，但不是 native Delta protocol。
- **Legacy Delta external table**：`CREATE EXTERNAL TABLE ... TABLE_FORMAT=DELTA` 在 refresh 时解析 `_delta_log`，但官方已标注未来 deprecated，Delta 自动刷新不支持，需要手工 refresh；它同样是 path/stage 只读能力。Compatibility Mode 也只是为 managed table 生成只读副本后让这类 reader 读取，并非访问原 managed table。

Databricks 官方 integration matrix 中 Snowflake 的 Unity REST API 列为空，只有 Iceberg REST catalog 支持。因此截至当前公开资料，Snowflake 没有等价的 native Unity REST managed Delta + temporary credentials + catalog commits 实现；Snowflake 对 Unity Catalog 的可写能力是 Iceberg，不应外推为 Delta write。Snowflake 是闭源 SaaS，官方没有披露 Delta Direct 使用 Delta Kernel、delta-rs、自研解析器还是内部 fork，因而不能用它证明 Doris 应该或不应该引入某个格式库。

它对 Doris 的价值是帮助区分两个目标：Delta Direct 证明“解析 Delta log 后接入自身 metadata/execution”可行；如果目标是按 UC 表名访问 managed Delta 并最终互操作写入，仍需 native Unity control plane、credential lifecycle 和 catalog commits。

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

Java 方面，Delta 4.3 已发布 `delta-kernel-unitycatalog` artifact。其中实验性的 `UCCatalogManagedClient` 能参与 catalog-held commit 读取和 staged commit/finalize 流程，但需要注入具体 `UCClient`；连接、认证、request handling 和 credential lifecycle 仍由 connector 集成。

Rust 方面，独立的 `delta-kernel-unity-catalog`、`unity-catalog-delta-rest-client` 等 companion crates 已覆盖 name-to-table ID/path、temporary credentials 和 catalog get/commit API；但 `TemporaryTableCredentials` 当前只暴露 AWS，credential expiry 后的 Engine 重建、冲突 rebase/retry 和 connector identity/cache 仍由集成方负责。建表所需的 staging reservation 和最终 finalize endpoint 尚未由该 REST client 暴露，也需要 connector-owned UC client。Rust FFI 暴露面是否足以让 Doris 不再增加一层 bridge，同样必须通过 PoC 证明。

因此 Doris 不应预设 Unity/catalog-commit adapter 必须全部从零实现，也不能把任一 Kernel artifact 当成开箱即用的三云 Databricks connector。PoC 应同时验证 Java 与 Rust 的现成组件和版本边界。

Kernel 仍不等于完整 Doris Databricks connector：Doris 需要把官方组件接入自己的 OAuth/身份模型、principal-aware cache、三云 credential lifecycle、FE/BE snapshot pinning、native reader/writer 和错误语义。相关 Unity 组件及外部写入能力仍存在版本与 Preview 边界，不能因为 artifact 已发布就跳过三云和并发互操作认证。

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
4. catalog-managed Delta CREATE：先向 Unity reserve staging table，取得 table ID/location 和 `READ_WRITE` credential；写入 version 0；再向 Unity finalize table。version 0 不走 commits API；
5. catalog-managed Delta append/write：version 1 及以后通过 staged commit、Unity ratify/publish；
6. catalog-managed DML；
7. 评估维护操作和更多 table features。

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

- 实现 managed CREATE 的 reserve/write-version-0/finalize，以及后续 append 的 catalog commits；Rust companion REST client 的 staging/finalize 缺口需要单独适配；
- 再开放受支持的 DML；
- 以服务端 capability 和 Preview 状态加实验开关；
- 验证 conflict、失败恢复和单表事务边界；对未实现的多表原子事务明确拒绝。

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
- [Delta Lake 4.3.0 release（Delta Spark、Kernel Unity artifact）](https://github.com/delta-io/delta/releases/tag/v4.3.0)
- [Delta Kernel Unity integration](https://docs.delta.io/kernel/rust/unity_catalog/overview.html)
- [Delta Kernel Rust Unity reads and credential refresh](https://docs.delta.io/kernel/rust/unity_catalog/reading.html)
- [Delta Kernel Rust Unity table creation](https://docs.delta.io/kernel/rust/unity_catalog/creating_tables.html)
- [Delta Kernel catalog-managed reads](https://docs.delta.io/kernel/rust/catalog_managed/reading.html)
- [Delta Kernel Rust FFI](https://docs.delta.io/kernel/rust/ffi/overview.html)
- [Delta Kernel Java `UCCatalogManagedClient`](https://github.com/delta-io/delta/blob/master/kernel/unitycatalog/src/main/java/io/delta/kernel/unitycatalog/UCCatalogManagedClient.java)
- [Delta transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Delta Standalone deprecation](https://docs.delta.io/delta-standalone/)

### 竞品

- [Starburst Enterprise Delta Lake with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-delta-lake-unity.html)
- [Starburst Enterprise Delta Lake connector](https://docs.starburst.io/481-e/connector/delta-lake.html)
- [Trino Delta Lake connector](https://trino.io/docs/483/connector/delta-lake.html)
- [Trino Delta transaction log implementation](https://github.com/trinodb/trino/blob/master/plugin/trino-delta-lake/src/main/java/io/trino/plugin/deltalake/transactionlog/TransactionLogAccess.java)
- [Trino Delta connector dependencies](https://github.com/trinodb/trino/blob/master/plugin/trino-delta-lake/pom.xml)
- [ClickHouse DataLakeCatalog](https://clickhouse.com/docs/reference/engines/database-engines/datalake)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse data lake support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix)
- [ClickHouse DeltaLake engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/deltalake)
- [ClickHouse integration with Rust Delta Kernel](https://clickhouse.com/blog/integrating-rust-delta-kernel)
- [ClickHouse DataLakeCatalog control/data-plane layering](https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud)
- [ClickHouse Unity adapter source](https://github.com/ClickHouse/ClickHouse/blob/master/src/Databases/DataLake/UnityCatalog.cpp)
- [ClickHouse Delta Kernel scan source](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/ObjectStorage/DataLakes/DeltaLake/TableSnapshot.cpp)
- [ClickHouse current master Delta Kernel cloud selection](https://github.com/ClickHouse/ClickHouse/blob/master/src/Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.cpp)
- [ClickHouse Cloud 26.4 Azure Delta Kernel release note](https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/cloud/reference/01_changelog/02_release_notes/26_4.md)
- [Snowflake Delta Sharing catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing)
- [Snowflake Object Store / Delta Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage)
- [Snowflake Delta Direct table semantics](https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-delta)
- [Snowflake Delta Direct read-only limitation](https://docs.snowflake.com/en/release-notes/2026/other/2026-07-02-delta-direct-variant-data-type)
- [Snowflake Delta-based Iceberg metadata generation](https://docs.snowflake.com/en/user-guide/tables-iceberg-metadata)
- [Snowflake legacy Delta external table](https://docs.snowflake.com/en/sql-reference/sql/create-external-table)
- [Snowflake Unity Catalog through Iceberg REST](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity)
- [Databricks Iceberg REST table-type matrix](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Databricks Unity Catalog integrations matrix](https://docs.databricks.com/aws/en/external-access/integrations)
