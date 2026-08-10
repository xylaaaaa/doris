# Apache Doris 对 Databricks 的互操作支持调研

> 状态：调研结论与实现路线建议，不是最终接口承诺
>
> 调研日期：2026-08-10
>
> Doris 代码基线：`6a0aeebd51efe1ad76a298cf7726545386529c10`
>
> 证据范围：Doris 当前代码与官方文档；Databricks、Delta Lake、Apache Iceberg、Snowflake、Apache Spark、Starburst、ClickHouse 的官方文档、公告或官方代码仓库。

## 1. 结论摘要

1. **对于具备 external-engine capability、且存储支持外部 FileIO/vending 的 Databricks managed table，Doris 可以直接访问；但“直接”必须是按表名通过 Unity Catalog 的开放 API 访问，不是绕过 Unity Catalog 读取对象存储 URI。** Databricks 明确要求 managed table 通过 catalog、schema、table 名访问；直接按路径访问会绕过治理，并可能破坏表。Iceberg 使用 Iceberg REST Catalog；Delta 使用 Unity REST API、短期 table credentials，并在 catalog-managed table 上处理 catalog commits。default storage 是当前明确例外。

2. **现有 Doris 教程中的 external location 不代表所创建的表是 external table。** 教程将 external location 选作 catalog 的 managed storage root，随后执行未指定 `LOCATION` 的 `CREATE TABLE ... USING iceberg`；该表仍是 managed Iceberg。external location 在这里是 Databricks 管理员侧的“云路径 + storage credential”治理对象，用来把 managed storage 放到 customer-managed storage。Unity Catalog 的 table credential vending 是另一项表级授权能力，还取决于 metastore external access、`EXTERNAL USE SCHEMA`、table capability 和底层 storage integration；Doris 调用者不需要 `EXTERNAL USE LOCATION`。

3. **不一定要为 Doris 新建一个专用 external location。** 如果目标 catalog/schema 已经有支持外部 credential vending 的 customer-managed storage root，Doris 只需通过 REST endpoint 和表名访问；这是由 Databricks 的 managed storage 继承规则推导出的结论。反之，Databricks 当前明确指出 workspace default storage 不支持外部 Iceberg/Delta client 的 FileIO 和 credential vending，因此教程取消 `Use default storage` 并选择 external location 是有现实原因的。

4. **Doris 的 Iceberg 技术方向正确，短板主要在产品化与兼容性闭环。** 当前代码已有 Iceberg REST、OAuth2、token refresh、user session、`X-Iceberg-Access-Delegation: vended-credentials`、临时存储凭证提取，以及 native Iceberg scan/write。需要补齐 Databricks 真实环境的持续测试、临时凭证续期、失败时 fail-closed、三云覆盖、Databricks capability/限制校验与文档纠偏。

5. **Doris 目前没有 native Delta Lake Catalog。** 当前公开方案是实验性的 Trino Connector compatibility plugin；仓库示例使用 `type=trino-connector` 与 `trino.connector.name=delta_lake`。它不能代替 Doris 自己的 Unity Catalog 元数据、权限、credential vending、Delta protocol 与 catalog commits 支持。

6. **建议新增通用的 native `deltalake` catalog，并以 Unity Catalog 作为第一个 catalog adapter。** 协议解析不应重新手写，也不应采用已经弃用的 Delta Standalone；应优先复用官方 Delta Kernel。Unity control plane 适合放在 FE，BE 继续使用 Doris native Parquet reader；Kernel 究竟以 Java planner 还是 Rust FFI 嵌入，需要用 PoC 验证 data transform 边界后再定。

7. **竞品给出的共同答案是“开放 catalog API + 临时凭证 + 原生表格式引擎”。** Snowflake 已把 Databricks managed Iceberg 做成 GA 的双向 catalog；Starburst Enterprise 对 Unity Catalog Delta 的支持最完整，并明确处理自动凭证刷新与 catalog-managed writes；Spark 是 Databricks 官方 Delta REST 接入的参考客户端；ClickHouse 的主要非 Azure 路径已从自研 Delta log 解析迁移到 Delta Kernel。没有一家成熟方案把“长期 AK/SK + 猜测 managed table 路径”作为主路径。

## 2. 先把四个概念分开

“是否需要 external location”容易混淆，是因为下面四个维度经常被当成一件事：

| 维度 | 可选项 | 含义 |
| --- | --- | --- |
| 表的生命周期 | managed table / external table | 谁管理数据文件的位置、生命周期与优化。customer-managed storage 上的 managed table 数据位于客户云账号并由 Unity Catalog 管理；default storage 是 Databricks fully managed storage，属于例外。 |
| 表格式 | Delta Lake / Iceberg | transaction log、snapshot、delete、schema evolution 等表语义。 |
| Databricks 存储治理对象 | managed location / external location / storage credential | Unity Catalog 如何授权和管理一个云路径。external location 是“路径 + storage credential”的 securable，不等于 external table。 |
| 外部引擎访问协议 | Iceberg REST / Unity REST / Delta Sharing / Compatibility Mode / raw path | Doris 如何发现表、取得临时凭证并读取数据。 |

Databricks 的 [managed tables](https://docs.databricks.com/aws/en/tables/managed) 和 [managed 与 external tables 对比](https://docs.databricks.com/aws/en/data-governance/unity-catalog/managed-versus-external) 都说明 managed table 是推荐的默认形态；在 customer-managed storage 场景，数据仍存放在客户云账号中。新的 default storage 则是 [Databricks fully managed object storage](https://docs.databricks.com/aws/en/storage/default-storage)，不能把前一种口径无条件套用到所有 managed table。两种场景的外部 FileIO 能力也不同。

### 2.1 Doris 教程为什么创建 external location

Doris 当前的 [Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/) 执行了三件关键操作：

1. 在 Databricks 中创建 external location；
2. 创建 catalog 时取消 `Use default storage`，把该 external location 选为 managed storage；
3. 创建 `USING iceberg` 且没有显式 `LOCATION` 的表。

根据 Databricks 的 [managed storage location 规则](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)，catalog 或 schema 的 `MANAGED LOCATION` 必须位于一个 external location 内，实际 managed table 会放到 Unity Catalog 生成的 `__unitystorage/...` 隔离路径。没有显式 `LOCATION` 的表仍是 managed table。

所以教程里的因果关系是：

```text
external location（管理员侧路径与凭证治理）
    └── catalog managed storage root
        └── __unitystorage/.../managed Iceberg table
            └── 满足 external-access/capability 条件时，
                Iceberg REST 按表名返回 metadata + 短期凭证
```

它不是：

```text
external location -> external Iceberg table -> Doris 只能用长期 AK/SK 读路径
```

Databricks 的 [default storage 限制](https://docs.databricks.com/aws/en/storage/default-storage) 明确指出：default storage 当前不支持外部 Iceberg/Delta client 直接访问 metadata、manifest 和 data files，不支持 FileIO，也不支持 Unity REST/Iceberg REST credential vending。因而，教程选择 customer-managed external location 并非多余步骤；它是在该环境中建立一个符合外部 FileIO 前提的 managed storage root。是否真正下发 table credential 仍由 UC 的 external-access 配置、权限与 table capability 决定。仅给 Doris 增加一个对象存储 secret 不能解除 default-storage 限制；可选路径是 Databricks compute 上的 JDBC/ODBC、能力受限的 OpenSharing，或把兼容数据输出到 customer-managed storage。

### 2.2 能不能不创建新的 external location

可以分三种情况回答：

| 场景 | 是否需要为 Doris 新建 external location | 正确访问方式 |
| --- | --- | --- |
| 目标 managed Iceberg 已位于可 credential-vend 的 catalog/schema managed storage 下 | **不需要**。Doris 消费端不需要知道底层路径或持有长期云凭证。 | Iceberg REST + OAuth/PAT + vended credentials。 |
| 目标 catalog 仅使用 workspace default storage，且当前不支持外部 credential vending | 需要管理员为新表建立符合条件的 customer-managed storage root；它通常由 external location 承载。修改 catalog managed location 只影响之后新建的 managed objects，现有表必须重建、复制或 clone，不会自动迁移。 | 迁移后的表仍按表名经 Iceberg REST 访问，而不是让 Doris 直读 managed 路径。 |
| external table，或用户明确选择 raw path fallback | 需要 Databricks path credentials、消费端 IAM，或其他能访问该路径的存储配置。 | 直接 Delta/Iceberg path；此路径不应冒充 managed-table direct access。 |

第一行的“不需要新建”是根据 Databricks 的 managed storage 逐级继承规则与 credential vending 行为得出的工程推论；最终仍应通过目标 table 的 capability 和一次真实 `loadTable`/temporary credential 请求确认，而不能只看 catalog UI。

### 2.3 Databricks managed table 的正式外部访问路径

下表汇总 Databricks 当前公开能力。Preview 状态会变化，实施前必须重新核对。

| 表类型 | 推荐协议 | 外部读 | 外部写/建表 | 关键说明 |
| --- | --- | --- | --- | --- |
| Managed Iceberg（支持外部 FileIO/vending 的 customer-managed storage） | Iceberg REST Catalog | 是 | 是 | Unity Catalog 可返回短期、表范围的存储凭证。Databricks 推荐 Iceberg client 1.9.2+；default storage 是明确例外。 |
| Foreign Iceberg | Iceberg REST Catalog | 是 | 否 | 只读；credential vending 与 metadata refresh 能力和 managed Iceberg 不同。 |
| Managed / external Delta with Iceberg reads | Iceberg REST Catalog | 是 | 否 | 这是 UniForm/Iceberg metadata 的只读桥接，不是 native Delta write。metadata 异步生成，可能落后于 Delta commit；当前 Unity REST Delta client 反而不支持这类表。 |
| Managed Delta | Unity REST API | 是 | Create/Write 为 Public Preview | 写入必须支持 catalog commits；不能假设对象存储中的 `_delta_log` 是完整、最新的唯一事实来源。 |
| External Delta | Unity REST API 或 path | 是 | 是 | REST 路径可使用 temporary table/path credentials；raw path 需要独立存储权限。 |
| Default-storage managed table | JDBC/ODBC；有限 OpenSharing | 是 | 由 Databricks compute 完成 | 外部 FileIO 和 credential vending 均不支持。 |
| Compatibility Mode copy | 指定路径上的兼容 metadata | 是 | 否 | Public Preview 的只读兼容副本，适合不支持 REST 的旧客户端，不是首选架构。 |
| Delta Sharing / OpenSharing | Sharing protocol | 是 | 否 | 适合跨组织受控分享，不等价于完整 Unity Catalog browsing/write。managed Iceberg sharing 仍为 Public Preview，且不能据此让 external Iceberg client 直接访问 default storage。 |

依据包括 Databricks 的 [外部数据访问总览](https://docs.databricks.com/aws/en/external-access)、[Iceberg 外部客户端](https://docs.databricks.com/aws/en/external-access/iceberg)、[Unity REST API 的 Delta 外部客户端](https://docs.databricks.com/aws/en/external-access/unity-rest)、[credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)、[Delta Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads) 和 [Compatibility Mode](https://docs.databricks.com/aws/en/external-access/compatibility-mode)。

## 3. Databricks 开放协议的关键语义

### 3.1 Iceberg REST Catalog

Databricks 的 Iceberg REST endpoint 为：

```text
https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest
```

外部客户端需要启用 metastore external data access，并获得 `EXTERNAL USE SCHEMA` 以及正常的 catalog/schema/table 权限。读取通常需要 `USE CATALOG`、`USE SCHEMA`、`SELECT`；写入需要 `MODIFY`，建表需要 `CREATE TABLE`。只有 path/external location 操作才需要 `EXTERNAL USE LOCATION`。认证支持 PAT 和 OAuth；生产环境应优先使用可自动刷新的 OAuth machine-to-machine 凭证。配置要求见 [Enable external access](https://docs.databricks.com/aws/en/external-access/admin)。

需要单独记录一个成熟度差异：managed Iceberg/Iceberg v3 已 GA，但截至 2026-08-04，权限参考仍把 `EXTERNAL USE SCHEMA` 标为 Public Preview，且它不包含在 `ALL PRIVILEGES` 中。建 catalog 的预检应显式检查它，不能假定管理员授予 `ALL PRIVILEGES` 后已经具备外部访问权限。依据见 [Unity Catalog privilege reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)。

客户端请求 `X-Iceberg-Access-Delegation: vended-credentials` 后，`loadTable` 响应可包含短期云凭证及过期时间。Databricks 文档给出的默认有效期约为一小时，因此“能成功跑一个短查询”并不能证明实现完整；查询、写入或重试跨过凭证 TTL 时必须刷新。

三云返回的凭证形态不同：AWS 为 STS session credentials，Azure 为 ADLS SAS，GCP 为短期 OAuth token。Doris 必须分别做 live test，不能以 S3 单测推断三云都兼容。

官方资料对 foreign Iceberg credential vending 还有冲突：2026-08-03 的 Iceberg REST 页面写明 foreign Iceberg 不提供 vending，2026-08-06 的 credential-vending 矩阵却列出 read-only table credential。在 Databricks 澄清或真实测试确认前，Doris 应采用保守支持矩阵：foreign Iceberg 只读，且不承诺由 UC vending 存储凭证；用户可能仍需配置外部 catalog/storage 的访问凭证。

Managed Iceberg、foreign Iceberg 和 Iceberg v3 已于 2026-05-21 GA，但 Databricks managed Iceberg 不是 Apache Iceberg 所有能力的无条件超集。当前官方限制包括：只使用 Parquet；不支持 Iceberg v2 position/equality delete files，而使用 v3 deletion vectors；不支持 branches/tags；不支持 expression partition transforms；部分类型和 nested required field 不支持；部分 table properties 由 Unity Catalog 控制。workspace 还必须启用 serverless compute、serverless 能访问 backing storage，创建 managed Iceberg 需要 predictive optimization。Doris 必须按服务端 capability 与 Databricks 支持矩阵校验，不能因为自身支持通用 Iceberg 就直接承诺全部语法。状态依据见 Databricks [2026 年 5 月 Release Notes](https://docs.databricks.com/aws/en/release-notes/product/2026/may)。

Unity Catalog 当前没有普通意义上的 external Iceberg table 类型：在 UC 内创建的是 managed Iceberg；由 Glue、HMS、Snowflake Horizon 等外部 catalog 管理并 federate 进 UC 的是 foreign Iceberg；external Delta 开启 Iceberg reads 后仍是 external Delta。表类型定义见 [Unity Catalog table types](https://docs.databricks.com/aws/en/tables/types)。

### 3.2 Unity REST、temporary credentials 与 managed Delta

native Delta direct access 至少包含三类交互：

1. 通过 Unity Catalog API 按全名发现表、读取 table ID、storage location、Delta metadata 与 capability；
2. 通过 temporary credential API 取得带过期时间、限定范围的云凭证：table API 使用 `READ`/`READ_WRITE`；path API 使用 `PATH_READ`/`PATH_READ_WRITE`/`PATH_CREATE_TABLE`，且只适用于 external location/external table path，不能用于 managed table；
3. 对 catalog-managed Delta，读取 catalog 中尚未发布到对象存储 log 的 commit tail，并把写提交交给 catalog commits。

Databricks [temporary table credentials API](https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials) 与 [temporary path credentials API](https://docs.databricks.com/api/workspace/temporarypathcredentials/generatetemporarypathcredentials) 的响应包含云凭证和 expiration time；[Get table API](https://docs.databricks.com/api/workspace/tables/get) 可请求 Delta metadata。具体 API 版本仍在演进，Doris 应优先跟随官方 client/Delta Kernel 的兼容层，而不是把未稳定的响应结构散落在业务代码中。

[Catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits) 把 catalog 变成 catalog-managed table 的提交事实来源。一个只读取对象存储 `_delta_log` 的实现可能遗漏 catalog 持有的未发布 log tail，得到过期甚至错误的 snapshot；写入也不能绕过 catalog 直接抢占版本号。这是 native Delta Catalog 与普通“Delta path scanner”的本质区别。

截至本调研日期，catalog commits 的核心协调能力已于 2026-05-08 GA，但 dedicated 页面仍把 external access 标为 Beta，外部 Delta client 对 managed Delta 的 Create/Write 则为 Public Preview；`EXTERNAL USE SCHEMA` 也仍是 Public Preview。这些状态不能混为一谈。native managed read 可以作为 Doris 的第一个 production-readiness 目标，但必须经过 Doris 三云、权限、credential lifetime 和 feature 认证后才能正式定级；managed write 应由显式实验开关保护，并以服务端 capability 为准。

当前 Unity REST Delta client 还有一个重要路由限制：不支持已经开启 Iceberg reads 或 `IcebergCompatV3` 的 Delta table。这类表应通过 Iceberg REST 走只读路径，native Delta M2 不能把它们算作已覆盖的 Unity REST read。

### 3.3 “直接访问”不能等同于“直接访问 URI”

Databricks 明确要求 managed table 通过三段式表名访问。raw URI 会绕过 Unity Catalog 的权限、审计、行列策略、table capability 与生命周期管理，并可能与 Databricks 后台维护并发冲突。因此 Doris 应把以下两种 catalog 模式在产品和报错中清楚区分：

- **Unity-managed 模式**：表名寻址，catalog 决定位置，临时凭证，遵守 capability/catalog commits；
- **Path 模式**：用户显式提供 Delta/Iceberg 路径和存储权限，只面向 external/unmanaged 数据。

不能在 Unity-managed 请求失败后静默降级为“猜路径 + 静态 AK/SK”。

### 3.4 Row filter、column mask 与 cross-engine ABAC

普通 credential vending 不能直接用于带 row filter/column mask 的表，因为把文件凭证交给客户端本身不能执行这些策略。Databricks 已提供 [cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac) Beta 路径：由 Databricks serverless 做 scan planning，对 managed、启用 catalog commits 的表执行受控只读，并要求特定 client/version。Doris 当前尚未实现该 server-side planning 协议，因此第一阶段应明确拒绝这些策略表，不能在拿到对象存储凭证后绕过策略读取；后续可把 server-side planning 作为独立治理能力接入。

## 4. Doris 当前能力与差距

### 4.1 已有 Iceberg REST 基础

基于当前 master 代码检查：

- `fe/pom.xml` 使用 Apache Iceberg `1.10.1`，高于 Databricks 当前推荐的最低客户端版本 1.9.2；
- `IcebergRestProperties` 已支持 REST URI、OAuth token/credential、M2M token refresh、user session/token exchange、nested namespace、view 和 `iceberg.rest.vended-credentials-enabled`；该 vending 开关默认是 `false`；
- 开启 vended credentials 后会发送 `X-Iceberg-Access-Delegation: vended-credentials`；
- `IcebergVendedCredentialsProvider` 从 Iceberg `Table.io()` 与 `SupportsStorageCredentials` 提取云凭证；
- `IcebergScanNode`、Iceberg sink/delete/merge sink 在初始化时把 table-level credentials 转成 BE 存储属性；
- Doris 官方 [Iceberg Catalog 文档](https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog/) 已覆盖 REST、vended credentials、Iceberg V1/V2/V3、delete files 与读写操作。

主要代码证据位于：

- `fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/IcebergRestProperties.java`；
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergVendedCredentialsProvider.java`；
- `fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/AbstractVendedCredentialsProvider.java`；
- `fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/VendedCredentialsFactory.java`；
- `fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java`；
- `fe/fe-core/src/test/java/org/apache/doris/datasource/property/metastore/IcebergUnityCatalogRestCatalogTest.java`。

这说明无需另造一个“Databricks Iceberg Catalog”。标准 Iceberg REST 应继续作为主实现；Databricks 只需要 provider profile、capability 检查、诊断信息与认证/测试增强。

### 4.2 Iceberg 需要优先补齐的缺口

| 优先级 | 代码/产品现状 | 风险 | 建议 |
| --- | --- | --- | --- |
| P0 | `IcebergUnityCatalogRestCatalogTest` 整个类被 `@Disabled`，包含空 token、固定 workspace、捕获后打印/吞异常。 | 当前没有可重复、会真正失败的 Databricks 兼容门禁。 | 建立按环境变量注入的 gated live suite；AWS/Azure/GCP 至少 nightly 跑核心矩阵。 |
| P0 | vended storage credentials 在 scan/sink 初始化时提取后转成 BE properties；代码检查未发现 expiration metadata 或执行期刷新协议。 | 默认约一小时的凭证可能在长查询、排队、retry 或长写入中途过期。 | 让 scan range/credential handle 携带 expiry；按阈值主动刷新，并增加超过 TTL 的测试。 |
| P0 | `AbstractVendedCredentialsProvider`/`VendedCredentialsFactory` 在提取异常或空结果时返回 base storage properties。 | 用户明确启用 vending 时可能静默回退到长期凭证；既掩盖配置错误，也改变权限边界。 | 显式 vending 模式 fail-closed；只有用户明确选择 static fallback 时才允许回退。 |
| P0 | 通用 Iceberg 能力与 Databricks managed Iceberg 的子集没有 provider-aware 校验。 | partition transform、delete、branch/tag、类型或 property 可在规划后才被远端拒绝。 | 在 create/alter/write 前协商 capability，给出 Databricks 语义化报错。 |
| P1 | 已有 credential key 转换支持多类云存储，但 Unity live test 与单测覆盖仍偏 AWS/S3。 | Azure SAS/ADLS 与 GCS credential 字段、endpoint、refresh 可能回归。 | 建立三云 contract fixtures 与真实环境 smoke test。 |
| P1 | service principal catalog identity 与 user-session delegated identity 都有代码基础，但对 UC 远端 ACL 的用户可见语义不够明确。 | 共享 service principal 时，UC 看见的是一个 Doris 身份；Doris 用户权限不能自然等同于 UC 用户权限。 | 产品上明确 catalog identity 模式；需要逐用户治理时使用 session/delegated token，并把 principal 纳入缓存键。 |
| P1 | 当前文档容易让读者把 external location、external table、managed storage 混为一谈。 | 用户以为必须复制数据、暴露路径或配置长期 AK/SK。 | 按本调研第 2 节重写教程，并加入 default storage 的限制与排障。 |

上表关于凭证刷新与 fallback 是对当前代码路径的检查结论，不代表已经在真实 Databricks 环境复现了故障；应由长查询 live test 验证。修复目标仍应是 fail-closed，而不是增加更多推测性的 defensive branch。

### 4.3 当前 Delta Lake 支持不是 native catalog

Doris 官方 [Delta Lake Catalog 文档](https://doris.apache.org/docs/4.x/lakehouse/catalogs/delta-lake-catalog/) 将现有能力标为实验性，并要求把兼容版本的 Trino Delta Lake plugin 部署到每个 FE/BE。仓库 `samples/datalake/deltalake_and_kudu` 也使用：

```sql
CREATE CATALOG delta_lake PROPERTIES (
  "type" = "trino-connector",
  "trino.connector.name" = "delta_lake"
);
```

当前 FE/BE 中没有独立的 Delta snapshot、protocol、deletion vector、Unity temporary credentials 或 catalog commits 实现。现有路径可作为功能试用和回归参考，但不满足“对 Databricks Delta 的 native 支持”：

- 部署和版本受外部 Trino plugin 约束；
- 不是 Doris native scan/planning pipeline；
- 没有 Databricks managed Delta 的 catalog-aware metadata/commit 闭环；
- 公开文档只承诺读取/数据集成，不承诺 native write-back。

## 5. 竞品调研

### 5.1 总表

| 产品与版本边界 | Databricks Iceberg | Databricks Delta | 临时凭证 | 写入 | 对 Doris 最有价值的启示 |
| --- | --- | --- | --- | --- | --- |
| Snowflake（商业 SaaS） | Unity Iceberg REST + catalog-linked database；managed Iceberg 双向访问已 GA。 | UniForm/Iceberg reads、Delta Sharing、Delta Direct 均以只读为主；没有证据表明其提供等价的 native UC managed Delta write catalog。 | `VENDED_CREDENTIALS` 可不配置 Snowflake external volume。 | Managed Iceberg 可写；Delta 路径主要只读。 | 标准 Iceberg REST 足够承载高质量商业集成；远端 UC ACL 与消费端 RBAC 边界必须明确。 |
| Apache Spark + Iceberg/Delta/UC connector（开源客户端） | Iceberg `SparkCatalog` 直连 UC Iceberg REST，managed Iceberg R/W。 | Unity Catalog Spark connector + Delta 4.3；external Delta R/W，managed Delta read，managed Create/Write 为 Preview。 | 支持 table/path credential vending 与 OAuth refresh。 | managed Delta write 必须走 catalog commits。 | 是 Doris native Delta 的官方参考数据流；不要把 Spark core 与附加 connector 混为一谈。 |
| Starburst Enterprise（商业版） | 481-e STS：UC Iceberg external R/W、managed R；480-e.6 LTS 官方契约仍为只读。 | 原生 Delta connector + Unity Catalog；普通 managed table 只读，catalog-managed table 支持实验性 DML。 | Delta S3/GCS storage credentials 自动刷新；Iceberg catalog OAuth token 可按 expiry 刷新，但 UC 专页未明确承诺 Iceberg storage credential refresh。 | external Delta 完整度高，但 location prefix 必须在 UC 绑定 credential；catalog-managed write 受 preview/experimental 限制。 | 最接近 Doris 目标能力：native log、credential lifecycle、server-side planning、feature matrix 和 catalog commits。 |
| ClickHouse OSS / ClickHouse Cloud | UC 指南只支持 external-location Iceberg；managed Iceberg 不支持。 | 非 Azure 路径主要使用 Delta Kernel，Azure 因 known issues 仍是例外；UC managed Delta 不支持。 | OSS 26.7 源码中 S3 credential 有 refresh callback；Azure 可取 SAS 但 native UC refresh 尚不完整；PAT 无自动刷新证据，不等同于所有 Cloud 版本 SLA。 | external read 有官方支持；catalog write 因总矩阵/指南冲突仍待 PoC；无 catalog-managed Delta commit。 | 从手写协议迁到 Delta Kernel 值得参考；同时证明“能列 catalog”不等于能访问 managed data。 |
| Doris 当前 | Iceberg REST + OAuth/vended credentials + native scan/write 已存在。 | 实验性 Trino Connector plugin。 | Iceberg 已能提取 vended credentials，但执行期刷新待补。 | Iceberg 有写基础；native Delta 无。 | 先把现有 Iceberg 做成经过认证的产品能力，再分阶段实现 native Delta。 |

“未找到官方证据”不是断言产品不支持；它表示本调研不以博客暗示或格式层能力推导 catalog-managed write。

### 5.2 Snowflake

Snowflake 的 [Databricks Unity Catalog 双向访问教程](https://docs.snowflake.com/en/user-guide/tutorials/tables-iceberg-set-up-bidirectional-access-to-unity-catalog) 使用 REST catalog integration 和 catalog-linked database 发现 managed Iceberg。`ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS` 时，Unity Catalog 下发临时云凭证，教程明确不需要 Snowflake external volume。Snowflake 的 [2025-10 GA 公告](https://docs.snowflake.com/en/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga) 表明 externally managed Iceberg writes 与 catalog-linked database 已 GA。

该能力包括 query、INSERT、CREATE/DROP，并在权限允许时执行 UPDATE/DELETE/MERGE 等写操作，但有 autocommit、多语句事务、equality-delete、远端 rename 等限制，详见 [externally managed Iceberg writes](https://docs.snowflake.com/en/user-guide/tables-iceberg-externally-managed-writes) 与 [catalog-linked database 限制](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)。

治理边界值得 Doris 借鉴：Snowflake 通常使用一个 UC service principal 访问远端，再用 Snowflake 自己的 RBAC 管理 Snowflake 用户；UC 的逐用户 ACL 不会自动映射成 Snowflake 用户权限。Doris 如果默认采用 catalog-level service principal，也必须明确相同事实。

Snowflake 对 Delta 的路径更分散：

- Databricks Delta 开启 Iceberg reads 后，可通过 Iceberg REST 只读，不能据此写 Delta；
- [Delta Sharing catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing) 适合只读共享；
- [Object Store/Delta Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage) 直接读取 `_delta_log`，需要 Snowflake external volume，且是只读、需要 refresh 的 path 模式；
- legacy `TABLE_FORMAT=DELTA` external table 面向所有账号可用、只读、不支持自动 refresh，并计划在未来弃用，见 [external tables](https://docs.snowflake.com/en/user-guide/tables-external-intro)。

因此 Snowflake 证明了 managed Iceberg 无需消费端长期存储凭证，却不能作为“native managed Delta 已经简单解决”的证据。

### 5.3 Apache Spark 与 Databricks Spark

Apache Spark core 提供 [CatalogPlugin API](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/CatalogPlugin.html)，但不内置 Unity Catalog 客户端。实际接入由以下开源组件组合完成：

- Apache Iceberg runtime/cloud bundle + `SparkCatalog`，直连 Databricks Iceberg REST；
- Delta Lake Spark 4.3+ + Unity Catalog Spark connector 0.5+，直连 Unity REST；
- Delta Sharing Spark connector，读取 share；
- 或 Delta OSS 直接读 external table/path，但这不是 managed UC table 的标准路径。

Databricks 当前 [Unity REST 外部 Delta 客户端文档](https://docs.databricks.com/aws/en/external-access/unity-rest) 把 Apache Spark 列为正式支持客户端，并要求 Spark 4.0/4.1、Delta Spark 4.3+、Unity Catalog connector 0.5+。external Delta 可读写建表；managed Delta 可读，Create/Write 依赖 catalog commits 且仍为 Public Preview。OAuth M2M 可自动刷新 token/credentials。

这条链路是 Doris 最应参考的正确性基线：表发现与授权由 UC 完成，Delta Spark 处理 protocol，存储访问使用短期 credentials，catalog-managed commit 回到 UC；Doris 可以另行评估用 Delta Kernel 实现对应的 protocol 能力。它也说明“Apache Spark 支持”不等于裸 Spark core 支持，商业 Databricks 服务仍提供 endpoint、治理、managed storage、credential vending、catalog commits 与后台维护。

### 5.4 Starburst Enterprise 与 upstream Trino

Starburst Enterprise 的 Unity Catalog Delta connector 是目前调研到最接近目标 Doris Delta Catalog 的商业实现。其文档明确区分：

- external Delta table 可执行 read/create/insert/update/delete/merge 等操作；
- 未启用 catalog commits 的普通 managed Delta table 为只读；
- catalog-managed Delta table 在启用相应 catalog property 后支持读取，并实验性支持 INSERT/UPDATE/MERGE/DELETE/DROP；
- S3/GCS vended credentials 按 table ID/location 限定范围，并在查询期间自动刷新。

external Delta write 还必须满足 Databricks 对 external client 的写入要求；使用 location credential vending 时，该 location prefix 必须已经在 Unity Catalog 中绑定 storage credential。这里的自动刷新结论只针对 Delta vended storage credentials。SEP Iceberg 的 catalog OAuth token 可在服务端给出 expiry 时刷新，但 [SEP metastore properties](https://docs.starburst.io/481-e/object-storage/metastores.html) 与 UC Iceberg 专页没有同样明确承诺 vended storage credential 会自动刷新。

catalog-managed Delta 表必须由 Databricks 创建，并满足 `delta.feature.catalogManaged=supported`、关闭 row tracking、使用 classic checkpoint 等当前限制。这说明完整支持不是简单增加 `catalog.type=unity`：connector 必须同时具备 native Delta transaction log、table feature、temporary credential 生命周期和 catalog commit 协议。Starburst 的公开状态与 Databricks 页面在“Private Preview / Public Preview / experimental”用词上存在更新时间差异，Doris 应把 managed write 视为不稳定能力，以 runtime capability 与真实兼容测试为准。

官方依据：[Starburst Enterprise 481-e Delta Lake connector with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-delta-lake-unity.html) 与 [481-e Delta Lake connector](https://docs.starburst.io/481-e/connector/delta-lake.html)。

Iceberg 能力需要按版本说清楚：[SEP 481-e UC Iceberg 文档](https://docs.starburst.io/481-e/connector/starburst-iceberg-unity.html) 按 SEP 自己的表分类支持 external table 读写、managed table 读取，并可使用 UC server-side scan planning 执行 row filter/column mask；[480-e.6 LTS 文档](https://docs.starburst.io/480-e/connector/starburst-iceberg-unity.html) 仍要求 `iceberg.security=read_only`。因此不能把 STS 的新能力无条件宣传成 LTS 能力。该模式由 catalog 宣告的 capability 自动触发，会牺牲统计信息、join reorder、time travel 和部分 metadata table 能力，必须评估治理收益与性能限制。

商业版与开源版边界也很重要：[Trino 483 metastore 文档](https://trino.io/docs/483/object-storage/metastores.html) 要求 UC Iceberg 使用 `iceberg.security=read_only`，但 Trino OSS 本身支持通用 Iceberg REST vended credentials；upstream [Trino 483 Delta connector](https://trino.io/docs/483/connector/delta-lake.html) 虽支持通用 Delta DML，但 metadata source 是 HMS/Glue，没有 Unity managed table、table-ID credential vending 或 catalog-managed commits。SEP 的 Unity Delta metastore、Unity Delta managed credential vending/refresh、catalog commits、OAuth passthrough、UC Iceberg external write 和 server-side planning 是实质性商业增量。Doris 当前使用 Trino compatibility plugin，也不会因此自动获得这些 SEP 能力。

### 5.5 ClickHouse

ClickHouse 的 [DataLakeCatalog](https://clickhouse.com/docs/reference/engines/database-engines/datalake) 可以连接 Unity Catalog endpoint，并对 catalog 中的 Iceberg/Delta 表进行发现。核心 engine 是开源的；ClickHouse Cloud 的 [catalog query 公告](https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud) 增加 Shared Catalog、distributed cache、弹性计算与托管体验，Unity/Glue integration 最初以 Beta 发布。没有官方证据显示 Cloud 另有一套 OSS 不具备的 managed-table 语义。

更重要的工程信号来自 ClickHouse 的 [Rust Delta Kernel 集成说明](https://clickhouse.com/blog/integrating-rust-delta-kernel)：ClickHouse 曾自己解析 Delta transaction log，之后把主要路径迁移到官方 Rust Delta Kernel，以减少协议演进、deletion vectors、schema evolution 等兼容成本。这与 Doris 应采用的方向一致。其 [support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix) 同时注明 Azure Blob Storage 因 known issues 禁用 Kernel，因此不能把该迁移描述成所有云的无条件覆盖。

必须同时看到它的边界：[ClickHouse Unity Catalog 指南](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog) 明确只支持使用 external storage locations 的 UC Delta/Iceberg 表，并写明 managed storage 当前不提供 ClickHouse 这条路径所需的 credential vending。因此 ClickHouse 当前的 UC managed Delta 和 managed Iceberg 都应标为不支持，不能因为能列出 metadata 就推断能读取文件。

ClickHouse 官方资料对 write 和成熟度标签都有冲突：[总支持矩阵](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix) 把 Unity 的 Read/Create/INSERT 标为 Beta，但 [writing data guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/getting-started/writing-data) 又说明表不能由 catalog managed、Delta write 仍在开发；2026-07-24 的 Unity 指南只展示 external read，仍称 experimental 并要求 `allow_experimental_database_unity_catalog`。保守结论是：external read 已有官方支持；external Iceberg catalog create/insert 和 external Delta INSERT 必须 PoC 后再宣称，不能写成 managed write；Delta DELETE/UPDATE/MERGE 与 catalog-managed commits 没有支持证据。格式层 [DeltaLake engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/deltalake) 到 26.7 才把 existing-table INSERT 标为 Beta，仍不能创建空 Delta table，Azure write 也不支持。

ClickHouse OSS 26.7 stable 源码还展示了一个很具体的 credential lifecycle 差距：[native Unity credential request](https://github.com/ClickHouse/ClickHouse/blob/v26.7.3.19-stable/src/Databases/DataLake/UnityCatalog.cpp#L135-L190) 固定使用 `operation=READ`；[refresh callback](https://github.com/ClickHouse/ClickHouse/blob/v26.7.3.19-stable/src/Databases/DataLake/UnityCatalog.cpp#L483-L499) 只覆盖 S3，Azure 可以取得初始 SAS 但该 callback 未覆盖，PAT 也没有自动 refresh 的官方承诺。[Iceberg REST path](https://github.com/ClickHouse/ClickHouse/blob/v26.7.3.19-stable/src/Databases/DataLake/RestCatalog.cpp) 则实现了重新 `loadTable` 获取 storage credential 和 OAuth refresh。这些源码事实不等同于所有 ClickHouse Cloud 服务版本的公开 SLA，但正是 Doris 不能只做一次性短查询测试的原因。

Delta Sharing 也不能从名称推断：Starburst/Trino 官方 connector 列表中没有找到 Databricks Delta Sharing protocol connector；ClickHouse DataLakeCatalog 虽列出 `catalog_type=delta_sharing`，但文档把它描述为 flat-namespace 的 Iceberg 路径，缺少成熟度与完整配置说明。本调研不把它们记为成熟的 Delta-format Sharing 支持。

## 6. 推荐的 Doris 目标架构

### 6.1 总体原则

1. Iceberg 保持标准：继续使用 `type=iceberg`、`iceberg.catalog.type=rest`；Databricks 是兼容 profile，不复制一套 Iceberg 实现。
2. Delta 原生化：新增通用 `deltalake` catalog，第一种 adapter 为 `unity`，后续可扩展 path/HMS/Glue 等 metadata source。
3. Catalog 与 format 解耦：Unity client 负责表发现、授权、temporary credentials、catalog commits；Delta Kernel 负责 transaction protocol、snapshot 和 scan semantics。
4. 数据面保持 Doris native：BE 读取 Parquet，做 vectorized filter、projection、data skipping，并应用 Delta deletion vector/physical schema mapping。
5. capability 驱动：按 table protocol/features 与 UC capability 接受或明确拒绝，不能靠版本号猜测。
6. managed 模式 fail-closed：不绕过 UC，不静默使用 guessed path 或共享长期凭证。

建议的数据流：

```text
                     control plane
Doris FE  ── OAuth ───────────────────────────────> Databricks Unity Catalog
   │          list/get table, capability,                 │
   │          temporary credentials, catalog commits      │
   │                                                      │
   ├── Iceberg REST client ── metadata + scan tasks <─────┤
   │                                                      │
   └── Delta Kernel ── snapshot/protocol/files/log tail <─┘
          │
          │ scan ranges + schema mapping + DV + credential expiry/handle
          v
Doris BE native Parquet reader ── scoped temporary credential ──> S3 / ADLS / GCS
                     data plane
```

### 6.2 Iceberg 路线：增强而不是分叉

建议增加一个自动或显式的 Databricks REST profile，用于：

- endpoint、warehouse/catalog 参数与权限诊断；
- 校验 external data access、`EXTERNAL USE SCHEMA`、table capability；
- 对 default storage 无 credential vending 给出可操作错误；
- 根据 Databricks managed Iceberg 限制校验 partition transform、delete、types 和 properties；
- 区分 managed Iceberg、foreign Iceberg、Delta Iceberg reads，准确显示只读/可写状态；
- 记录 credential source、scope、expiry、refresh 结果等不含 secret 的 observability 字段。

不建议新增 `type=databricks_iceberg`，否则会复制 Iceberg REST 的 auth、metadata、scan、write 与 future spec 支持。

### 6.3 Native Delta Lake Catalog

下面的属性仅表示设计方向，不是最终 SQL 接口：

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

模块职责建议如下：

| 模块 | 职责 |
| --- | --- |
| Unity catalog client | catalogs/schemas/tables 列举；table ID/location/capabilities；OAuth/PAT；temporary table/path credentials；catalog commit reads/writes。 |
| Delta Kernel adapter | protocol feature negotiation；checkpoint/log replay；snapshot/time travel；schema/partition transform；file planning；statistics/data skipping；deletion vector 描述。 |
| FE Delta metadata/cache | 以 catalog、principal、table ID、snapshot/version 为 cache key；不能跨用户共享 credentials；pin statement snapshot。 |
| Delta scan planner | 生成 Parquet scan ranges，携带 logical/physical column mapping、partition values、DV descriptor、snapshot version 与 credential expiry/handle。 |
| BE Delta reader | 复用 native Parquet reader；应用 DV row selection、column mapping、missing/default column 语义与 residual predicate。 |
| Delta writer/committer | 分阶段实现 staged files、optimistic concurrency、external Delta commit，以及 catalog-managed table 的 catalog commit。 |

[Delta Kernel](https://docs.delta.io/delta-kernel/) 是官方提供给 connector 的 Java/Rust 库，覆盖 scan planning、schema transforms、data skipping、deletion vectors 等协议细节。Java 已发布实验性的 `delta-kernel-unitycatalog`，可复用 catalog-managed snapshot/commit 逻辑，但 [`UCCatalogManagedClient`](https://github.com/delta-io/delta/blob/master/kernel/unitycatalog/src/main/java/io/delta/kernel/unitycatalog/UCCatalogManagedClient.java) 仍标注 `@Experimental`，并要求调用方提供 [`UCClient`](https://github.com/delta-io/delta/blob/master/storage/src/main/java/io/delta/storage/commit/uccommitcoordinator/UCClient.java)；Doris 仍需实现或绑定 UC REST、认证和 credential vending client。[Rust Kernel Unity integration](https://docs.delta.io/kernel/rust/unity_catalog/overview.html) 明确覆盖 catalog-managed log tail/commit 抽象，但当前 [UC reading](https://docs.delta.io/kernel/rust/unity_catalog/reading) 的内建 temporary credential model 只覆盖 AWS，Azure/GCP 仍需 Doris 补齐，credential refresh 也由 connector 负责。Rust Kernel 提供 [C/C++ FFI](https://docs.delta.io/kernel/rust/ffi/overview.html) 和 UC committer hook；table resolution 与 credential REST control plane 仍需 Doris 接入。不要采用已 [deprecated 的 Delta Standalone](https://docs.delta.io/delta-standalone/)。

Kernel 的官方 connector API 不只返回文件列表：读取 Parquet 后还要执行 `Scan.transformPhysicalData`，把 physical data 按当前 protocol/metadata 转成 logical data，并产生 deletion selection vector。因而不能因为 Doris BE 已有 Parquet reader，就在 FE 调一次 Kernel 后重新手写所有 transform。架构冻结前应比较两种 PoC：

| 方案 | 优点 | 必须证明的风险 |
| --- | --- | --- |
| FE Java Kernel planning + 序列化 scan/transform descriptor 给 BE | 贴合 Doris 现有 FE metadata/planner；可复用实验性 Java UC snapshot/commit 组件。 | Doris 仍需 UC REST/auth/credential client；Kernel API 稳定性、scan state 是否能稳定序列化；BE 是否会重复实现并漂移于 `transformPhysicalData`；大 snapshot 的 FE 内存与 planning latency。 |
| BE 嵌入 Rust Kernel FFI，FE 只做 UC control plane/权限 | Kernel 更靠近 native data path；ClickHouse 已验证 Rust Kernel 的工程方向。 | FFI 未替代 UC control plane；Rust UC 内建凭证目前仅 AWS；FE/BE snapshot 一致性、credential refresh/传递、Rust/C++ ABI、Arrow/column block 转换和构建体积。 |

第一版仍应保留 Doris native Parquet data path，但只有在 PoC 证明 Kernel 的 logical transform 可以完整、版本化地传给 BE 时，才选择 FE-only Kernel；否则应把 Kernel transform 下沉到 BE，而不是靠 Doris 自己追赶 Delta protocol。

### 6.4 第一版 Delta read 的最低正确性集合

只做到“读出几个 Parquet 文件”不能叫 native Delta 支持。MVP 至少需要：

- classic 与 v2 checkpoints；
- reader protocol/features 协商，未知 reader feature 明确报错；
- name/id column mapping 与 schema evolution；
- deletion vectors；
- partition pruning、file statistics/data skipping 与 residual filter；
- snapshot isolation、version/timestamp time travel；
- Unity managed/external table 的 temporary credentials；
- catalog-managed table 的 catalog-held log tail；
- AWS S3、Azure ADLS、GCS；
- 不支持的 generated/identity/default columns、variant/type widening 等 feature 有精确行为或精确拒绝。

CDF、streaming、OPTIMIZE/VACUUM、Delta Sharing 可以后续迭代，不能阻挡第一版 snapshot read。

### 6.5 Write 的开放顺序

建议按风险从低到高分阶段：

1. external Delta append；
2. external Delta CTAS/CREATE；
3. external Delta DELETE/UPDATE/MERGE；
4. catalog-managed Delta append/create，通过 catalog commits；
5. catalog-managed DML 与维护操作。

每个阶段都必须有 conflict/concurrency test，不能直接复用“写 `_delta_log/<next-version>.json`”的 path writer 处理 catalog-managed table。managed Create/Write 在 Databricks 仍为 Preview 时，应通过显式实验开关开放，并在建 catalog/table 时提示状态。

Databricks 还明确警告不要让不同 writer clients 同时修改 S3 上的同一 Delta table，否则可能损坏或丢失数据。外部客户端对 `delta.*`/`databricks.*` properties、table features、column rename、check constraints、generated/default/constraint columns 也有限制，且不能对 managed table 执行 `OPTIMIZE`、`VACUUM`、`ANALYZE`。Doris write milestone 必须逐项 capability-gate，而不能先暴露通用 DDL/DML 再依赖远端报错。

## 7. 身份、权限与凭证边界

本设计涉及外部 catalog 和临时云凭证。按 Doris `threat-model.md`，external catalog 是管理员信任的外部系统，但 Doris 用户之间的 RBAC 隔离与 cloud tenant 边界仍在安全模型内。

### 7.1 两种身份模式必须明确

| 模式 | UC 看见的 principal | 优点 | 限制 |
| --- | --- | --- | --- |
| Catalog service principal | 所有 Doris 查询共用一个 service principal | 部署简单、token refresh 稳定；与 Snowflake 常见模式一致。 | UC 不知道具体 Doris 用户；远端 ACL 不能替代 Doris RBAC。 |
| Delegated user/session identity | 每个 Doris session 的 UC user/token | 可以保留 UC 用户级授权与审计。 | token exchange、cache isolation、expiry/retry 更复杂。 |

无论哪种模式，temporary storage credential 必须至少绑定 principal、table ID/path、operation、cloud 与 expiration；metadata cache 可以共享无敏感内容，但 credential cache 不能跨 principal 共享。

### 7.2 安全与正确性要求

- catalog OAuth secret、access token、SAS/session token 不写入 edit log、query profile、普通日志或 `SHOW CREATE CATALOG` 明文；
- temporary credential 在过期前刷新，重试必须重新校验 table privilege/capability；
- 用户被撤权后不能因为 FE/BE cache 继续获得新的数据访问；
- 显式 vending 模式失败时 fail-closed，不回退到范围更大的 static credential；
- `CREATE CATALOG` 仍是高权限操作，URI/endpoint 受现有 external catalog SSRF 管理边界约束；
- FE 到 BE 的 credential 传递沿用受信内部网络假设，但要最小化范围、寿命和日志暴露；
- catalog cache key 必须包含远端 catalog、principal 和身份模式，防止跨用户 metadata/credential 混用。

## 8. 分阶段交付建议

### M0：建立真实兼容实验室与文档基线

- 修订 Doris Unity Catalog 教程，解释 external location、managed storage、managed table 与 credential vending；
- 建立 AWS/Azure/GCP Databricks workspaces 与自动清理的测试 catalog；
- 覆盖 managed/foreign Iceberg、managed/external Delta、Delta with Iceberg reads、default storage negative case；
- PAT 只做调试，正式流水线使用 OAuth M2M；
- 记录所有 API/capability 响应 fixture，但对 secret 做结构化脱敏。

### M1：把现有 Iceberg 支持做到 production-ready

- 将 disabled Databricks test 改成 gated live integration suite；
- 凭证 expiry/refresh、长查询和 retry；
- vended 模式 fail-closed；
- 三云 credential 映射；
- provider capability validation 与面向用户的诊断；
- managed Iceberg read/write、foreign Iceberg read、Delta Iceberg reads read-only 的认证矩阵。

### M2：Native Delta read

- 新增 `deltalake` catalog 与 Unity adapter；
- 集成 Delta Kernel、Unity table metadata、temporary credentials 与 catalog-held log tail；
- BE native Parquet + DV/column mapping；
- pruning、time travel、schema evolution、feature rejection；
- managed/external Delta 三云 read certification。
- 对启用 Iceberg reads/`IcebergCompatV3` 的 Delta table 路由到 Iceberg REST read-only，不计入 native Unity REST 覆盖。

### M3：Native Delta write

- 先 external Delta append/create/DML；
- 再以实验能力接入 catalog-managed create/write 与 catalog commits；
- conflict、idempotency、retry、orphan file 与权限撤销测试；
- 等 Databricks 状态稳定后再升级支持等级。

### M4：性能与高级生态能力

- metadata/scan planning cache、credential refresh 合并、并行 listing 与 data cache；
- CDF、Delta Sharing、更多 Delta table features；
- per-user delegated identity、private connectivity 与完整审计；
- 与 Databricks Runtime/Spark、Starburst、ClickHouse 的交叉一致性测试。

## 9. 验收标准

### 9.1 Iceberg

1. Doris 仅配置 Databricks OAuth，不配置长期 S3/ADLS/GCS secret，即可查询和写入符合条件的 managed Iceberg；
2. 查询跨越 temporary credential 默认 TTL 后仍成功，或在不可续期场景给出确定且不泄密的错误；
3. managed、foreign、Delta Iceberg reads 的 R/W 能力严格符合服务端 capability；
4. default storage 不支持 vending 时，在 planning 阶段给出可操作诊断；
5. 无 secret 出现在 edit log、profile、audit/general log、exception 与 `SHOW CREATE CATALOG`；
6. service-principal 与 delegated-user 两种模式的权限和 cache isolation 测试通过。

### 9.2 Delta

1. 同一 table version 上，Doris 与 Databricks/Spark 的 row count、schema、null/default、DV 删除结果一致；
2. catalog-managed table 含 catalog-held log tail 时，Doris 仍读取正确 snapshot；
3. column mapping、v2 checkpoint、deletion vector、schema evolution、time travel 有正向回归；
4. 每个未知/不支持的 required reader feature 都在读文件前明确拒绝；
5. 三云 managed/external Delta 均不需要把长期 storage credential 写入 catalog；
6. credential expiry、权限撤销、FE failover、BE retry 不造成跨用户访问或错误结果；
7. write 阶段的并发 commit 与 Databricks/Spark 互操作，无 lost update，失败重试幂等；
8. performance baseline 至少包含 planning latency、log/checkpoint 规模、文件数、DV 比例、cache cold/warm 与凭证刷新开销。

## 10. 需要在 PoC 中关闭的未知项

1. Databricks 各云与各 workspace storage 配置下，table capability flags 和 temporary credential 响应的实际差异；
2. Doris 当前把 vended credentials 物化到 BE properties 后，超过 TTL 的查询/写入具体失败点；
3. Delta Kernel Java 的 Unity Catalog adapter 对 Databricks 当前 catalog commits API 的覆盖与版本兼容窗口；
4. managed Delta Public Preview 对第三方客户端允许的 table features、DDL/DML 与并发限制；
5. Doris 对 cross-engine ABAC Beta server-side planning 的协议与性能验证，以及 views/materialized views 的可见性；在实现前必须拒绝策略表，不能把“能列出 table”当成“能安全绕过 policy 读文件”；
6. Azure/GCP vended credential 自动刷新、private endpoint 与 FE/BE 网络拓扑；
7. Databricks managed Iceberg 当前 partition transform/type/property 限制与 Doris DDL 的逐项映射；
8. catalog identity 与 delegated identity 的产品默认值及迁移方式。

这些未知项不妨碍确认总体架构，但会决定每个 milestone 的支持矩阵和是否可以标为 production-ready。

## 11. 官方资料索引

### Apache Doris

- [Doris Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/)
- [Doris Iceberg Catalog](https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog/)
- [Doris Iceberg REST Catalog](https://doris.apache.org/docs/3.x/lakehouse/metastores/iceberg-rest/)
- [Doris Delta Lake Catalog（Trino Connector compatibility）](https://doris.apache.org/docs/4.x/lakehouse/catalogs/delta-lake-catalog/)

### Databricks 与 Delta Lake

- [External access overview](https://docs.databricks.com/aws/en/external-access)
- [Managed tables](https://docs.databricks.com/aws/en/tables/managed)
- [Unity Catalog table types](https://docs.databricks.com/aws/en/tables/types)
- [Managed and external tables](https://docs.databricks.com/aws/en/data-governance/unity-catalog/managed-versus-external)
- [Managed storage locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)
- [Default storage limitations](https://docs.databricks.com/aws/en/storage/default-storage)
- [Managed Iceberg tables](https://docs.databricks.com/aws/en/iceberg/)
- [External access through Iceberg REST](https://docs.databricks.com/aws/en/external-access/iceberg)
- [External access through Iceberg REST on Azure](https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg)
- [External access through Iceberg REST on GCP](https://docs.databricks.com/gcp/en/external-access/iceberg)
- [External access through Unity REST APIs](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- [Enable external access](https://docs.databricks.com/aws/en/external-access/admin)
- [Credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Unity Catalog privilege reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)
- [Temporary table credentials API](https://docs.databricks.com/api/workspace/temporarytablecredentials/generatetemporarytablecredentials)
- [Temporary path credentials API](https://docs.databricks.com/api/workspace/temporarypathcredentials/generatetemporarypathcredentials)
- [Get table API](https://docs.databricks.com/api/workspace/tables/get)
- [Catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits)
- [Databricks 2026 年 5 月 Release Notes](https://docs.databricks.com/aws/en/release-notes/product/2026/may)
- [Delta Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads)
- [Compatibility Mode](https://docs.databricks.com/aws/en/external-access/compatibility-mode)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
- [Delta Kernel Unity Catalog integration](https://docs.delta.io/kernel/rust/unity_catalog/overview.html)
- [Delta Kernel Unity Catalog reads](https://docs.delta.io/kernel/rust/unity_catalog/reading)
- [Delta Kernel catalog-managed reads](https://docs.delta.io/kernel/rust/catalog_managed/reading.html)
- [Delta Kernel Rust FFI](https://docs.delta.io/kernel/rust/ffi/overview.html)
- [Delta Kernel Java `UCCatalogManagedClient`](https://github.com/delta-io/delta/blob/master/kernel/unitycatalog/src/main/java/io/delta/kernel/unitycatalog/UCCatalogManagedClient.java)
- [Delta transaction log protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Delta Standalone deprecation](https://docs.delta.io/delta-standalone/)

### Snowflake 与 Spark

- [Snowflake bidirectional access to Unity Catalog](https://docs.snowflake.com/en/user-guide/tutorials/tables-iceberg-set-up-bidirectional-access-to-unity-catalog)
- [Snowflake REST catalog integration for Unity Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity)
- [Snowflake externally managed Iceberg writes](https://docs.snowflake.com/en/user-guide/tables-iceberg-externally-managed-writes)
- [Snowflake catalog-linked database limitations](https://docs.snowflake.com/en/user-guide/tables-iceberg-catalog-linked-database)
- [Snowflake Delta Sharing catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing)
- [Snowflake object storage / Delta Direct catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage)
- [Apache Spark CatalogPlugin](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/CatalogPlugin.html)
- [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Delta Sharing](https://docs.delta.io/delta-sharing/)

### Starburst 与 ClickHouse

- [Starburst Enterprise Delta Lake with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-delta-lake-unity.html)
- [Starburst Enterprise Delta Lake connector](https://docs.starburst.io/481-e/connector/delta-lake.html)
- [Starburst Enterprise 481-e Iceberg with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-iceberg-unity.html)
- [Starburst Enterprise 480-e.6 LTS Iceberg with Unity Catalog](https://docs.starburst.io/480-e/connector/starburst-iceberg-unity.html)
- [Starburst Enterprise 481-e metastore properties](https://docs.starburst.io/481-e/object-storage/metastores.html)
- [Trino 483 metastore support](https://trino.io/docs/483/object-storage/metastores.html)
- [Trino 483 Delta Lake connector](https://trino.io/docs/483/connector/delta-lake.html)
- [ClickHouse DataLakeCatalog](https://clickhouse.com/docs/reference/engines/database-engines/datalake)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse data warehouse support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix)
- [ClickHouse writing data guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/getting-started/writing-data)
- [ClickHouse DeltaLake engine](https://clickhouse.com/docs/reference/engines/table-engines/integrations/deltalake)
- [ClickHouse Cloud catalog integration](https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud)
- [ClickHouse integration with Rust Delta Kernel](https://clickhouse.com/blog/integrating-rust-delta-kernel)

## 12. 最终建议

立即把工作拆成两条并行主线，但保持一个共同的 Databricks compatibility lab：

- **Iceberg 主线**：不重造 catalog，修复 credential lifecycle/fallback 与测试缺口，按官方限制认证 managed Iceberg R/W 和 Delta Iceberg reads；
- **Delta 主线**：以 `Unity REST + Delta Kernel + Doris native Parquet + catalog commits` 为架构边界，先交付 managed/external read，再逐步开放 write。

对用户的产品表述应是：“对于具备 external-engine capability、且存储支持外部 FileIO/vending 的表，Doris 通过 Unity Catalog 按表名直接访问 Databricks managed tables，并使用短期、范围受限的存储凭证。”同时明确 default storage 例外。不要表述成“Doris 通过 external location 访问 Databricks 的 external tables”，也不要暗示可以绕过 Unity Catalog 直接读取 managed table URI。
