# Apache Doris 对 Databricks Iceberg 的支持完善调研

> 状态：调研结论与实现路线建议，不是最终接口承诺
>
> 调研更新：2026-08-14
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Databricks Unity Catalog 中的 Iceberg 外部访问及 Doris 现有 Iceberg 能力；native Delta Lake Catalog 见[独立调研](databricks-native-delta-lake-catalog-research.md)。

## 本文回答的三个问题

| 问题 | 直接回答 | 展开位置 |
| --- | --- | --- |
| 为什么 Doris 教程需要 Databricks External Location？ | 教程用 External Location 承载 customer-managed managed storage，避开当前不支持外部 FileIO/vending 的 default storage；它不是把表变成 External Table。 | 第 3.1 节 |
| 已有 Databricks 内部/managed 表能否不复制数据而直接访问？ | 可以，但必须按表名经过 Unity Catalog Iceberg REST；目标表要具备 external-engine capability，底层 storage 要能 vending，Doris 还要支持返回的云凭证。不能绕过 UC 猜测对象存储 URI。 | 第 3.2、3.3、4 节 |
| Doris 已有 Iceberg 支持还要完善什么？ | 不需要重造 Databricks 专用 Iceberg Catalog；应在现有 Iceberg REST 主干上补齐三云凭证、expiry/refresh、fail-closed、capability 校验、真实环境测试、治理策略和诊断。 | 第 5、7、8、9 节 |

此外，第 6 节单独比较 Snowflake、Spark、Starburst Enterprise/Trino 和 ClickHouse OSS/Cloud，重点区分商业版能力、开源基线、版本边界与官方资料中的冲突。

## 1. 结论摘要

1. **已有 Databricks catalog 中符合外部访问条件的 managed Iceberg 表，可以由 Doris 按原表名直接访问，不要求为 Doris 再创建一份 catalog、external table 或复制数据。** “直接”是 Doris 通过 Unity Catalog 的 Iceberg REST API 发现表并取得临时凭证，然后直读对象存储；不是绕过 UC 读取 `__unitystorage` URI。

2. **Doris 教程中的 External Location 不等于 External Table。** 教程把 External Location 选作新 catalog 的 managed storage root，未指定 `LOCATION` 创建的 Iceberg 表仍是 managed table。这样做的关键目的是把 managed storage 放到支持外部 FileIO 和 credential vending 的 customer-managed storage，而不是要求 Doris 只能访问 external table。

3. **是否需要新建 External Location，取决于已有表的存储条件，不取决于它是不是 managed table。** 已有 catalog/schema 若继承了可 vending 的 customer-managed storage，通常不需要新建；如果表位于 Databricks default storage，当前不支持外部 Iceberg/Delta FileIO 和 credential vending，则不能靠给 Doris 配一个长期存储密钥绕过限制。

4. **Doris 已有正确的 Iceberg REST 主干，但尚不能把“Databricks 三云 managed Iceberg”整体标为 production-ready。** 当前具备 REST、OAuth、access delegation 和 native Iceberg scan/write 基础；缺口包括真实环境门禁、凭证过期/刷新、vending 失败时 fail-closed、Databricks capability 校验，以及 AWS/Azure/GCP 的完整认证。

5. **欧洲 Azure 现场不是“未新建 External Location”导致失败的优先解释。** 现场已成功列库/列表，`loadTable` 又返回 `abfss://.../__unitystorage/...` metadata 和 table-scoped ADLS SAS，证明 UC 控制面、表解析和 credential vending 已经走通。当前 Doris 会过滤 Databricks 返回的 `adls.sas-token.*`，也没有形成 Azure Iceberg FileIO/SAS 到执行层的完整闭环，这是高置信候选根因；由于缺少原始 `SELECT` 错误和 FE/BE 日志，尚不能写成最终根因。

6. **竞品的成熟方案都把 Unity Catalog 当控制面，而不是要求用户复制 managed table。** Snowflake 的商业实现最完整，已经通过 catalog-linked database、vended credentials 和 GA write 支持形成双向集成；Starburst Enterprise 481-e 在开源 Trino 的 REST/vending 基础上增加 external write 和 Databricks server-side scan planning；Spark 是 Databricks 官方开源参考客户端；ClickHouse 的 UC 集成仍处于 Beta/Experimental，当前 UC 指南的可靠范围主要是 external-storage tables。

## 2. 先把五个概念分开

| 概念 | 是什么 | 不是什么 |
| --- | --- | --- |
| Unity Catalog | Databricks 的表目录、权限、治理和临时凭证控制面 | 表格式或对象存储 |
| Managed table | UC 管理表的位置和生命周期 | “数据一定存放在 Databricks 计算节点” |
| External table | UC 登记用户管理的数据路径 | 使用了 External Location 的所有表 |
| External Location | UC 中“云存储路径 + Storage Credential + 权限”的治理对象 | External Table 的同义词 |
| Iceberg REST Catalog | 外部 Iceberg client 按表名获取 metadata、capability 和临时凭证的开放协议 | 原始对象存储路径访问 |

在 customer-managed storage 场景中，managed table 的数据仍可位于客户自己的 S3、ADLS 或 GCS，只是位置和生命周期由 UC 管理。Databricks 新的 default storage 是 fully managed storage，外部 FileIO 能力不同，不能把两种 managed storage 混为一谈。

## 3. 直接回答：为什么需要 External Location，Databricks 内部表能否直接访问

**问题一：为什么需要一个 Databricks External Location？**

External Location 不是 Doris 协议层的强制要求，也不是为了把表创建成 external table。Doris 教程需要它，是因为教程要主动准备一块**确定支持外部 FileIO 和 credential vending 的 customer-managed storage**，再把它设为 Databricks catalog 的 managed storage。这样创建出来的表仍是 managed Iceberg，但 UC 可以在 Doris 按表名访问时，为其签发限定表路径和有效期的临时云凭证。

**问题二：Databricks 中已有的大量内部/managed 表，有方案让 Doris 不复制数据而直接访问吗？**

有。只要已有 managed Iceberg 表具备 external-engine capability，并且实际位于支持外部 FileIO/vending 的存储中，Doris 就可以直接连接**已有 Unity Catalog 和原表名**，通过 Iceberg REST 取得 metadata 和临时凭证，再读取原来的 S3、ADLS 或 GCS 文件。此时不需要新建 External Location、不需要把表改成 external table，也不需要复制数据。

但是，“managed table”只表示 UC 管理表的位置和生命周期，不能单独证明它能被外部客户端访问。Databricks managed table 还要按底层存储分成两类：

| 已有 managed table 的实际存储 | Doris 能否原地直接访问 | 是否需要另建 External Location |
| --- | --- | --- |
| 支持外部 FileIO/vending 的 customer-managed storage | 可以；使用现有 UC catalog、原表名和 UC 签发的短期凭证 | 不需要 |
| 当前不支持外部 FileIO/vending 的 Databricks default storage | 不能通过 Iceberg REST + 外部 FileIO 原地读取 | 单独新建 External Location 不会改变已有表的位置；如必须由 Doris 直读，需要把兼容数据迁移/复制到 customer-managed storage，或改用 JDBC/ODBC、受限 OpenSharing 等非直读方案 |
| 存储类型或 capability 不明确 | 不能只看 Catalog Explorer 中的 `MANAGED` 判断 | 先检查 table capability，并用一次真实 `loadTable` 验证是否返回可用的临时凭证 |

因此最短结论是：

> External Location 是官方教程用来构造“可向外部引擎签发凭证的 managed storage”的手段，不是 Doris 访问所有 managed table 的前置条件。已有表能否直接访问，取决于 external-engine capability、底层存储是否支持 FileIO/vending，以及 Doris 能否消费返回的云凭证。

### 3.1 Doris 教程为什么创建 External Location

Doris 当前的 [Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/) 做了三件关键操作：

1. 在 Databricks 创建 External Location；
2. 创建 catalog 时取消 `Use default storage`，把该 External Location 选为 managed storage；
3. 创建没有显式 `LOCATION` 的 `USING iceberg` 表。

按照 Databricks 的 [managed storage location 规则](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)，catalog/schema 的 managed storage root 位于一个 External Location 内，实际表数据进入 UC 生成的 `__unitystorage/...` 隔离目录。没有指定 `LOCATION`，所以表仍然是 managed Iceberg：

```text
External Location（管理员侧路径与凭证治理）
    └── catalog/schema managed storage root
        └── __unitystorage/.../managed Iceberg table
            └── Iceberg REST 按表名返回 metadata + 短期凭证
```

它不是：

```text
External Location -> External Iceberg Table -> Doris 长期持有 AK/SK 读路径
```

教程选择 customer-managed storage 的现实原因是：[default storage](https://docs.databricks.com/aws/en/storage/default-storage) 当前不支持外部 Iceberg/Delta client 访问底层 metadata、manifest 和 data files，也不支持 Unity REST/Iceberg REST credential vending。

### 3.2 已有 catalog 能否不新建 External Location

| 目标表场景 | 是否要为 Doris 新建 External Location | 正确访问方式 |
| --- | --- | --- |
| Managed Iceberg 已位于支持 vending 的 customer-managed catalog/schema storage | **不需要** | 使用现有 UC catalog 名，Iceberg REST + OAuth/PAT + vended credentials |
| 表位于当前不支持外部 FileIO/vending 的 default storage | 不能原地靠 Doris 凭证解决；要外部 FileIO，需把兼容数据放到符合条件的 customer-managed storage | 迁移后仍按表名走 Iceberg REST；不应让 Doris 猜 managed 路径 |
| External/unmanaged table，且用户明确选择 path 模式 | 可能需要 External Location/path credential 或 Doris 自有存储权限 | 显式 path 模式，不冒充 UC managed-table 访问 |

第一种场景最终应通过目标表 capability 和一次真实 `loadTable` 验证。只看 catalog UI 无法证明底层存储可 vending。

读取已有 managed table 通常需要 metastore 开启 external data access，并授予 `EXTERNAL USE SCHEMA`、`USE CATALOG`、`USE SCHEMA` 和 `SELECT`。`EXTERNAL USE LOCATION` 主要用于 external/path 操作，不是读取 managed table 的普通前置权限。

### 3.3 “按表名直接访问”究竟是什么

正确链路是：

```text
                         控制面
Doris ── catalog.schema.table ──> Unity Catalog Iceberg REST
  ▲                                      │
  │                 metadata、capability、短期 SAS/STS/OAuth
  └──────────────────────────────────────┘
  │
  │                      数据面
  └──────────────────────────────> ADLS / S3 / GCS
                读取 Iceberg metadata、manifest、Parquet
```

因此“直接”的含义是：不复制数据、不把查询交给 Databricks SQL Warehouse 执行、不重建 external table，并在 UC 授权后由 Doris 直读数据文件。它不意味着拿到 `abfss://.../__unitystorage/...` 或 `s3://...` 后绕开 UC。raw URI 会绕过权限、审计、table capability、行列策略和生命周期管理，也不能作为 managed 模式失败后的静默降级。

## 4. Databricks Iceberg REST 的协议与支持边界

Databricks Iceberg REST endpoint 为：

```text
https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest
```

客户端发送 `X-Iceberg-Access-Delegation: vended-credentials` 后，`loadTable` 可返回 metadata location、表能力以及短期云凭证。官方示例的凭证默认有效期约一小时：

- AWS：STS access key、secret key、session token；
- Azure：ADLS SAS；
- GCP：短期 OAuth token。

短查询成功不能证明实现完整。排队、长查询、retry 或写入跨越 TTL 时，需要携带 expiration 并刷新凭证。

### 4.1 第一步：识别 Unity Catalog 暴露的对象

这一层只判断表以什么形式通过 Iceberg REST 暴露，不讨论它存在哪里。

| UC 中的对象 | 它实际是什么 | Doris 应提供的能力 |
| --- | --- | --- |
| Managed Iceberg | UC 原生管理位置、metadata 和生命周期的 Iceberg 表 | 主要目标是读写；具体操作仍以服务端返回的 capability 为准 |
| Foreign Iceberg | UC 从外部 Iceberg catalog 登记的表，生命周期不由 UC 管理 | 保守按只读支持；credential vending 的官方表述存在差异，需要 live test |
| Delta table with Iceberg reads | 本质仍是 Delta 表，但 Databricks 为外部 Iceberg client 异步生成兼容 metadata | 只读；不等于 Doris 已经支持 native Delta，也不能通过 Iceberg 路径写 Delta |

### 4.2 第二步：检查这张表能否由 Doris 直接读写

知道对象形式后，还必须分别检查存储、临时凭证和治理策略。下面这些是访问条件或阻断因素，不是新的表类型。

| 检查项 | 观察到的情况 | Doris 应如何处理 |
| --- | --- | --- |
| 底层存储 | customer-managed storage 支持外部 FileIO/vending | 继续请求临时凭证，并检查 Doris 是否支持对应的 AWS、Azure 或 GCP 凭证 |
| 底层存储 | Databricks default storage | 当前不能通过 Iceberg REST + FileIO 直读；明确提示改用 JDBC/ODBC、受限 OpenSharing 或迁移副本 |
| 服务端 capability | 不支持当前 read/write/DDL 操作 | 在规划或 DDL 阶段拒绝，不能假设通用 Iceberg 能力在 Databricks 中都可用 |
| 临时凭证 | UC 未返回凭证，或 Doris 不认识返回的云凭证 | fail-closed，并指出是 vending 或 FileIO 兼容问题，不能静默回退长期凭证 |
| 行列策略 | 表带有 UC row filter/column mask | 普通文件凭证不足以执行策略；在支持 cross-engine ABAC/server-side planning 前明确拒绝 |

因此 Doris 的判断顺序应该是：

```text
识别 managed / foreign / Delta with Iceberg reads
        ↓
检查底层 storage 是否支持外部 FileIO/vending
        ↓
检查服务端 capability 和 UC 治理策略
        ↓
检查 Doris 能否消费返回的云凭证
        ↓
决定允许读、允许写，还是给出明确拒绝
```

Databricks managed Iceberg 也不是通用 Iceberg 能力的无条件超集。截至本调研日期，官方限制涉及文件格式、delete 表达、branches/tags、partition transform、部分类型和 UC 控制的 table properties。Doris 的 create/alter/write 必须按远端 capability 和官方支持矩阵校验，而不是因为 Doris 支持通用 Iceberg 就承诺所有语法。

## 5. Doris 当前能力与具体缺口

### 5.1 已有基础

当前 Doris 已有：

- Iceberg REST URI、OAuth token/credential、M2M token refresh、user session/token exchange；
- nested namespace、view 和 `iceberg.rest.vended-credentials-enabled`；
- `X-Iceberg-Access-Delegation: vended-credentials`；
- 从 Iceberg `Table.io()` / `SupportsStorageCredentials` 提取临时凭证的代码；
- native Iceberg scan、sink、delete 和 merge 基础。

主要代码入口：

- `IcebergRestProperties.java`；
- `IcebergVendedCredentialsProvider.java`；
- `AbstractVendedCredentialsProvider.java`；
- `VendedCredentialsFactory.java`；
- `IcebergScanNode.java`；
- `IcebergUnityCatalogRestCatalogTest.java`。

所以不建议另造 `databricks_iceberg` catalog。应继续使用标准 `type=iceberg` + `iceberg.catalog.type=rest`，把 Databricks 做成 capability、诊断和认证 profile。

### 5.2 优先补齐项

| 优先级 | 当前情况 | 风险与目标 |
| --- | --- | --- |
| P0 | Databricks live test 整类被禁用，且存在固定环境/吞异常问题 | 建立三云 gated live suite 和可重复 contract fixtures |
| P0 | 临时凭证在 scan/sink 初始化时物化，未形成 expiry 到执行期 refresh 闭环 | 长查询、排队和 retry 跨 TTL 时主动刷新 |
| P0 | 凭证提取异常/空结果会回退 base storage properties | 显式启用 vending 时 fail-closed；不得静默扩大权限边界 |
| P0 | Databricks managed Iceberg 的能力子集没有 provider-aware 校验 | 在规划/DDL 阶段给出语义化拒绝 |
| P0 | Azure `adls.sas-token.*` 未形成转换和 FileIO/BE 使用闭环 | 支持 table-scoped ADLS SAS、host、expiry 与 refresh |
| P1 | AWS、Azure、GCP 没有统一认证矩阵 | 每云覆盖 read/write、expiry、retry、endpoint/private network |
| P1 | catalog service principal 与 delegated user 的行为不够明确 | cache key 包含 principal，明确 UC ACL 与 Doris RBAC 的边界 |
| P1 | 教程混淆 External Location 与 External Table | 用本调研第 3 节重写解释与排障 |

这里需要修正一个容易误读的说法：**Doris 不是完全不支持 Azure。** Doris 已有 Azure account key/OAuth 等静态存储配置；当前缺失的是 Databricks Iceberg REST 返回的 table-scoped `adls.sas-token.*` 及其 expiration/refresh 链路。

### 5.3 欧洲 Azure 现场的证据链

现场对已有 UC catalog 开启外部访问后：

```text
SHOW DATABASES / SHOW TABLES 成功
→ REST loadTable 成功
→ 返回 abfss://.../__unitystorage/... metadata
→ config 返回 adls.sas-token.<host> 和 expiration
→ SELECT 读取数据失败
```

这已经证明：

1. REST 鉴权和 namespace/table 控制面可用；
2. 目标是 existing managed Iceberg，不需要为了 Doris 重新建表；
3. UC 已签发限定表路径的 Azure SAS，`storage-credentials: []` 不等于没有凭证；
4. “未新建 External Location”不是优先根因。

当前代码审计发现：

- `CredentialUtils` 接受 `azure.` 等前缀，但不接受 Databricks/Iceberg 使用的 `adls.`，SAS 会被过滤；
- FE 依赖包含 `iceberg-core`、`iceberg-aws`，未包含 `iceberg-azure`；
- 当前 Azure storage properties/BE client 主要覆盖 account key/OAuth 等既有形态，没有 table-scoped SAS 的完整传递与刷新；
- vending 转换失败可能静默回退，使错误延迟表现为 manifest/Parquet 鉴权失败。

这是高置信候选根因，不是最终诊断。关闭问题仍需要 Doris 版本/commit、完整 `SELECT` 错误、FE/BE 日志，以及 Doris 到目标 ADLS endpoint 的网络/防火墙验证。

### 5.4 中国区输入的证据等级

“2026 年 6 月某中国区 workspace 未部署 Iceberg REST、无 ETA”只能记录为特定现场和时间点的反馈，不能泛化为当前全部中国区能力。Azure 中国官方文档在 2026-07-24 已给出 Iceberg REST endpoint 和中国云专用域名。产品测试应记录 cloud、region、workspace、HTTP 状态和支持工单，按 workspace 探测能力，而不是静态判断整个区域支持或不支持。

## 6. 竞品：只看 Databricks Iceberg

本节只把各产品通过 Iceberg REST 访问 Databricks 的能力计入比较。Delta Sharing、Delta Direct、native Delta connector 等不在本篇的 Iceberg 支持结论内。

### 6.1 支持矩阵与版本边界

| 产品/版本 | Catalog 与身份入口 | 表与存储范围 | 凭证/FileIO | 写入与治理 | 成熟度和商业边界 |
| --- | --- | --- | --- | --- | --- |
| Snowflake 商业 SaaS | Unity Iceberg REST catalog integration；OAuth 或 bearer token；catalog-linked database 自动发现 UC namespace/table | AWS、Azure、GCP 上符合条件的 UC Iceberg；按已有 UC catalog 接入 | `ACCESS_DELEGATION_MODE=VENDED_CREDENTIALS` 时由 UC 下发临时凭证，不需要 Snowflake external volume；也可选择 external volume | catalog-linked database 默认可读写；支持 INSERT/UPDATE/CREATE 等受支持操作 | externally managed Iceberg writes 与 catalog-linked database 于 2025-10-17 GA；本次比较中产品化最完整 |
| Apache Spark + Apache Iceberg | `SparkCatalog` 直连 `/api/2.1/unity-catalog/iceberg-rest`；PAT/OAuth | Managed Iceberg R/W、Foreign Iceberg R、Delta with Iceberg reads R，取决于 UC capability | 必须加载 Iceberg runtime 和 AWS/Azure/GCP 对应 cloud bundle；支持 REST vending | 写操作由 Iceberg client 按 REST capability 执行 | 开源参考实现，不是裸 Spark core 内置 UC；是 Doris 三云正确性基线 |
| Starburst Enterprise 481-e STS | SEP Iceberg connector + UC Iceberg REST + OAuth | 按 SEP 文档分类支持 external R/W、managed R，覆盖 AWS/Azure/GCP | 开启 `iceberg.rest-catalog.vended-credentials-enabled`；worker 读取对象存储 | 新增 external write；可自动采用 Databricks server-side scan planning 执行 row filter/column mask | 481-e 是商业 STS；external write 与 server-side planning 是该版本新增能力 |
| Trino OSS 483 | 通用 Iceberg REST + OAuth | UC 官方配置要求 `iceberg.security=read_only` | 通用 REST 支持 vended credentials；481 起支持 AWS/GCS/Azure 可刷新凭证 | UC 路径只读；没有 SEP 的 Databricks server-side planning 产品能力 | 是 Starburst Enterprise 的开源基线，不能把 SEP 商业能力反推给 Trino/Doris plugin |
| ClickHouse OSS / ClickHouse Cloud | 开源 `DataLakeCatalog`；Cloud 25.8 起将 Glue/Unity 集成作为 Beta | 当前 UC 指南只承诺使用 external storage locations 的 Delta/Iceberg；不承诺 managed storage | 依赖 UC vending 后直读文件；指南认为其 managed-storage 路径拿不到所需凭证 | 支持矩阵把 UC Read/Create/INSERT 标为 Beta，但 UC 指南只完整展示 external read，写支持需 PoC | OSS 提供 catalog/format engine；Cloud 增加 Shared Catalog、distributed cache、parallel execution 等托管能力，但不改变 UC managed-table 契约 |
| Doris 当前 | 标准 Iceberg REST + OAuth/access delegation | 已有通用 Iceberg scan/write；Databricks managed/foreign/Delta-Iceberg-reads 尚未完成认证 | AWS 有基础；Azure `adls.sas-token.*` 和 GCP 未形成已认证闭环；执行期 refresh 待补 | 通用 Iceberg 有写基础，但 Databricks capability gate 不完整 | 正确路线是增强现有实现，不建立平行的 Databricks 专用 Iceberg 栈 |

### 6.2 Snowflake：商业实现的完整形态

Snowflake 使用 [Unity Catalog REST catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity) 对接已有 Databricks catalog。核心配置是：

```text
CATALOG_SOURCE = ICEBERG_REST
CATALOG_URI = <workspace>/api/2.1/unity-catalog/iceberg-rest
CATALOG_NAME = <existing-uc-catalog>
ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
```

然后通过 catalog-linked database 自动同步 UC schema/table。它没有要求把已有 managed Iceberg 复制成 Snowflake 表，也没有要求客户为 Snowflake 创建一张 external table。vended-credentials 模式要求 DBX metastore 开启 external data access，并给 Snowflake service principal 授予 `EXTERNAL USE SCHEMA`、`SELECT`、`USE CATALOG`、`USE SCHEMA`；UC 负责发放临时存储凭证。如果不用 vending，Snowflake 也支持 external volume，但那是另一种由消费端管理存储权限的部署模式。

Snowflake 文档明确写出 catalog-linked database 默认支持读写；externally managed Iceberg writes 与 catalog-linked database 已于 [2025-10-17 GA](https://docs.snowflake.com/en/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga)。这说明商业级 Databricks Iceberg 集成的完整形态是：

```text
自动 catalog 同步 + OAuth/PAT + vended credentials
+ 原生 Iceberg read/write + 权限预检 + 私网/运维能力
```

对 Doris 最直接的启示不是照搬 Snowflake 对象模型，而是：标准 Iceberg REST 已足以访问已有 UC managed Iceberg；消费端应把 credential lifecycle、catalog 自动同步、诊断和远端/本地 RBAC 边界做成产品能力。

### 6.3 Spark：Databricks 官方开源参考链路

Databricks 的 [Iceberg client 文档](https://docs.databricks.com/aws/en/external-access/iceberg) 给出了 Spark 的官方配置。实际组合并不是“Spark core 自动懂 Unity Catalog”，而是：

```text
Apache Spark
  + Iceberg Spark runtime
  + iceberg-aws/azure/gcp bundle
  + SparkCatalog
  + Databricks Iceberg REST endpoint
```

cloud-specific bundle 是重要信号：catalog 层能成功 `loadTable`，不代表数据层一定能消费 S3 STS、Azure SAS 或 GCP OAuth。Doris 当前 Azure 案例正好卡在这一层。Spark 因此适合作为 Doris contract/live test 的对照客户端：同一个 UC table、同一个 principal、同一种 vending 响应，Spark 成功而 Doris 失败时，问题可以进一步收敛到 Doris credential/FileIO 或 execution path。

### 6.4 Starburst Enterprise 与 Trino OSS：商业增量在哪里

[Starburst Enterprise 481-e STS](https://docs.starburst.io/latest/connector/starburst-iceberg-unity.html) 和 [481-e release notes](https://docs.starburst.io/latest/release/release-481-e.html) 明确增加了两项 Databricks Iceberg 能力：

- 使用 Unity Catalog 时写 external tables；
- Databricks server-side scan planning。

server-side planning 由 UC 决定要读取哪些文件，是执行 row filter/column mask 的必要路径。SEP 会在 catalog advertises capability 时自动选择它，并暴露 `serverSideScanCount`/`clientSideScanCount` 指标。代价是更高的 split planning latency，并限制 statistics/join reorder、time travel 和 Iceberg metadata tables。

这不是 upstream Trino 的无条件能力。[Trino 483 metastore 文档](https://trino.io/docs/current/object-storage/metastores.html) 虽然支持标准 REST、OAuth、vended credentials，并且 Trino 481 已增加 AWS/GCS/Azure refreshable vending，但连接 Databricks UC 时仍要求 `iceberg.security=read_only`。因此 Starburst Enterprise 的价值主要是 provider-aware 支持、商业版本认证、server-side planning 和新增的 external write，不只是把开源 connector 换一个名字。

版本也必须写清：当前 latest STS 是 481-e，而 latest LTS 是 480-e.7。不能把 481-e STS 新能力直接当成所有 LTS 部署已具备的契约；选型时要按客户实际 SEP 版本核对。

### 6.5 ClickHouse OSS 与 ClickHouse Cloud：能力宣传与指南边界

ClickHouse 的 `DataLakeCatalog` 是开源 catalog engine；ClickHouse Cloud 在 25.8 将 Glue/Unity integration 作为 Beta，并增加 Shared Catalog、distributed cache、userspace page cache 和 parallel execution 等托管能力。它可以根据 catalog metadata 自动选择 Iceberg/Delta table engine，这一点值得 Doris 借鉴：catalog discovery 与 format execution 应解耦。

但当前 [Unity Catalog 指南](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog) 明确把范围限定为使用 external storage locations 的 Delta/Iceberg 表，并把功能标为 experimental；managed Databricks storage 不在其承诺范围。与此同时，[ClickHouse support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix) 又把 Unity Read/Create/INSERT 标为 Beta。两份官方资料没有按 Iceberg/Delta、external/managed 拆清写能力，不能据此宣传“UC managed Iceberg 已支持完整写入”。保守产品结论应是：external-storage catalog read 已有公开路径；create/insert 和 managed table 必须以具体版本、云和 PoC 结果为准。

这也说明 Cloud 的商业增量主要在托管、弹性和缓存执行层；当前没有证据表明 Cloud 另有一套绕开 UC storage/vending 限制的 managed-Iceberg 协议。

### 6.6 对 Doris 的共同启示

1. **不复制数据。** Snowflake、Spark、Starburst 和 ClickHouse 都先连接已有 catalog；没有成熟方案要求为消费端把 managed table 改成 external table。
2. **catalog control plane 与 data plane 分离。** 表名、权限、capability 和临时凭证来自 UC；文件由各产品自己的 Iceberg/Parquet 执行层读取。
3. **credential vending 是主路径。** 商业产品把短期、表范围凭证做成自动能力，而不是让用户长期配置 AK/SK。
4. **三云 FileIO 是产品能力，不是 REST 成功的自然结果。** Spark 明确要求 cloud bundle；Trino 也按版本补齐 Azure/GCS vending；Doris 必须分别认证。
5. **治理需要 server-side planning。** 仅有文件凭证不能执行 UC row filter/column mask；Starburst 的实现证明这是独立能力，并伴随性能和功能限制。
6. **商业支持必须带版本和支持矩阵。** Snowflake 已 GA；Starburst STS/LTS 不同；ClickHouse 仍 Beta/Experimental。Doris 不能用“能列表”代替 production-ready 定级。

### 6.7 如何理解“完成度”和“产品化程度”

这里的“完成度”和“产品化程度”不是指是否采用了不同协议，也不是简单比较开源与商业软件。Doris 与竞品使用的主链路相同：按表名访问 Unity Catalog，由 UC 返回 metadata、capability 和临时凭证，再由各自的 Iceberg/FileIO 执行层读取数据。差异在于这条链路覆盖了多少场景，以及能否作为稳定的产品承诺交付给客户。

**完成度**关注协议和执行链路是否在支持矩阵内端到端正确：

| 维度 | 功能完整应达到的状态 | Doris 当前差距 |
| --- | --- | --- |
| 云存储 | AWS STS、Azure SAS、GCP OAuth 均能从 REST 响应传递到执行层 | AWS 有基础；Azure table-scoped SAS 尚未闭环；GCP 未完整认证 |
| 凭证生命周期 | 保存 expiration，支持排队、长查询、retry 和 FE/BE 故障场景下的 refresh | 凭证主要在 scan/sink 初始化时物化，缺少执行期 refresh 闭环 |
| 失败语义 | 显式启用 vending 后，空凭证、格式错误、刷新失败均 fail-closed | 当前存在回退 base storage properties 的风险 |
| 表能力 | managed、foreign、Delta Iceberg reads、default storage 的读写能力按服务端 capability 校验 | Databricks provider-aware capability gate 不完整 |
| 治理策略 | 正确执行 UC row filter/column mask；不能执行时明确拒绝 | 尚未形成 cross-engine ABAC/server-side planning 支持 |
| 兼容验证 | 按云、表类型、操作和版本覆盖 contract、live、长查询及 negative tests | Databricks live test 和三云认证矩阵不完整 |

**产品化程度**关注客户是否可以在不了解 SAS、STS 和 FileIO 内部实现的情况下，安全、稳定地部署和运维：

| 产品化能力 | 应达到的状态 |
| --- | --- |
| 配置与预检 | 用户只提供 workspace、catalog 和 OAuth；创建 catalog 时检查 external data access、UC 权限、table capability 和 storage vending |
| 错误诊断 | 区分 catalog OAuth、UC ACL、credential vending、FileIO、对象存储网络和 token expiry，不把数据面错误延迟成模糊的文件读取失败 |
| 可观测性 | 展示不含 secret 的 credential source、scope、expiration、refresh outcome，以及控制面/数据面耗时 |
| 安全与隔离 | SAS/session token 不进入日志、profile、edit log 或 `SHOW CREATE CATALOG`；凭证和 metadata cache 按 principal/table/operation 隔离 |
| 部署与运维 | 覆盖 private endpoint、proxy、endpoint/firewall、权限撤销、版本升级和 FE failover |
| 支持承诺 | 给出带版本的 AWS/Azure/GCP × 表类型 × read/write 支持矩阵、限制和排障文档 |

当前欧洲 Azure 现场已经完成 `SHOW TABLES -> loadTable -> UC 返回 ADLS SAS`，但在 `SELECT` 数据面失败。这说明 Doris 的总体架构和控制面主干已经存在，当前 Databricks managed Iceberg 支持处于“部分链路可用、尚未完成三云端到端认证”的阶段。修复 Azure SAS 只能关闭当前首要功能缺口；还需要补齐 refresh、fail-closed、capability、治理、安全、诊断和兼容矩阵，才能标为 production-ready。

Snowflake 和 Starburst Enterprise 的领先主要体现在这些能力已经形成版本化支持范围、凭证生命周期、诊断或治理能力，而不是使用了另一套访问架构。ClickHouse 当前仍标为 Beta/Experimental，因此不能只根据“已提供 Unity Catalog 接口”判断其产品化程度高于 Doris。

## 7. 推荐目标设计

### 7.1 保持标准 Iceberg REST

建议为 Databricks 增加自动或显式 profile，但不新增 catalog 类型。profile 负责：

- endpoint、warehouse/catalog 和权限预检；
- external data access、`EXTERNAL USE SCHEMA`、table capability 诊断；
- default storage 不支持 vending 时给出可操作错误；
- managed、foreign、Delta Iceberg reads 的读写能力展示；
- Databricks partition/delete/type/property 限制校验；
- 记录 credential source、scope、expiry、refresh outcome 等不含 secret 的可观测信息。

### 7.2 建立共享的临时云凭证层

Iceberg 与未来 Delta 可以共享 credential abstraction，但不能共享格式实现。凭证层至少表示：

```text
principal + table/path + operation + cloud + credential material + expiration
```

它需要覆盖 AWS STS、Azure ADLS SAS、GCP OAuth；支持 refresh、权限撤销和 FE/BE retry；显式 vending 失败时 fail-closed。catalog OAuth secret、PAT、SAS/session token 不得明文进入 edit log、query profile、普通日志或 `SHOW CREATE CATALOG`。

## 8. 交付计划

### M0：文档与兼容实验室

- 修订 Unity Catalog 教程，解释 External Location、managed storage、managed table 和 vending；
- 建立 AWS/Azure/GCP Databricks workspace 与脱敏 API fixtures；
- 覆盖 managed/foreign Iceberg、Delta Iceberg reads 和 default storage negative case；
- 正式流水线使用 OAuth M2M，PAT 只用于诊断。

### M1：凭证正确性

- 修复 Azure ADLS SAS 和 GCP credential 的转换、传递与 refresh；
- expiration 进入 scan/credential handle；
- 显式 vending fail-closed；
- 长查询、排队、retry、FE failover 和权限撤销测试。

### M2：Databricks capability 与产品化

- managed Iceberg read/write 认证；
- foreign Iceberg 和 Delta Iceberg reads 的只读认证；
- DDL/DML capability gate、default storage 诊断；
- service-principal/delegated-user cache 隔离；
- 根据服务端支持评估 cross-engine ABAC planning。

## 9. 验收标准与待验证项

### 9.1 验收标准

1. 只配置 Databricks OAuth、不配置长期 S3/ADLS/GCS secret，即可查询和写入符合条件的 managed Iceberg；
2. 查询跨越临时凭证 TTL 后仍成功，或在不可续期场景给出确定且不泄密的错误；
3. managed、foreign、Delta Iceberg reads 的读写能力严格符合服务端 capability；
4. default storage 不支持 vending 时，在读取文件前给出可操作诊断；
5. AWS、Azure、GCP 均有 contract、live 和长查询测试；
6. 无 secret 出现在 edit log、profile、audit/general log、exception 或 `SHOW CREATE CATALOG`；
7. service principal 与 delegated user 的权限和 cache isolation 测试通过。

### 9.2 仍需通过 PoC/现场关闭

- 各云、各 workspace storage 配置下 capability 和 credential 响应差异；
- Azure/GCP refresh、private endpoint 与 FE/BE 网络拓扑；
- foreign Iceberg credential vending 的官方文档差异；
- Databricks managed Iceberg partition/type/property 限制的逐项映射；
- 欧洲 Azure 案例的完整错误与网络验证；
- 中国区不同 cloud/region/workspace 的实际可用性。

## 10. 主要官方资料

### Apache Doris

- [Doris Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/)
- [Doris Iceberg Catalog](https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog/)
- [Doris Iceberg REST Catalog](https://doris.apache.org/docs/3.x/lakehouse/metastores/iceberg-rest/)

### Databricks 与 Iceberg

- [External access overview](https://docs.databricks.com/aws/en/external-access)
- [Managed and external tables](https://docs.databricks.com/aws/en/data-governance/unity-catalog/managed-versus-external)
- [Managed storage locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)
- [Default storage limitations](https://docs.databricks.com/aws/en/storage/default-storage)
- [Managed Iceberg tables](https://docs.databricks.com/aws/en/iceberg/)
- [External access through Iceberg REST](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Iceberg REST on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg)
- [Iceberg REST on Azure China](https://docs.azure.cn/en-us/databricks/external-access/iceberg)
- [Iceberg REST on GCP](https://docs.databricks.com/gcp/en/external-access/iceberg)
- [Enable external access](https://docs.databricks.com/aws/en/external-access/admin)
- [Credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Unity Catalog privilege reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)
- [Cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- [Delta Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads)

### 竞品

- [Snowflake bidirectional access to Unity Catalog](https://docs.snowflake.com/en/user-guide/tutorials/tables-iceberg-set-up-bidirectional-access-to-unity-catalog)
- [Snowflake REST catalog integration for Unity Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity)
- [Snowflake externally managed Iceberg writes / catalog-linked database GA](https://docs.snowflake.com/en/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga)
- [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Starburst Enterprise 481-e Iceberg with Unity Catalog](https://docs.starburst.io/latest/connector/starburst-iceberg-unity.html)
- [Starburst Enterprise 481-e release notes](https://docs.starburst.io/latest/release/release-481-e.html)
- [Starburst Enterprise 480-e LTS release notes](https://docs.starburst.io/latest/release/release-480-e.html)
- [Trino 483 metastore and Iceberg REST configuration](https://trino.io/docs/current/object-storage/metastores.html)
- [Trino 481: Azure and refreshable vended credentials](https://trino.io/docs/current/release/release-481.html)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse open table format and catalog support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix)
- [ClickHouse Cloud DataLakeCatalog architecture](https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud)
