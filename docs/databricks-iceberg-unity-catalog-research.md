# Apache Doris 对 Databricks Iceberg 的支持完善调研

> 状态：调研结论与实现路线建议，不是最终接口承诺
>
> 调研更新：2026-08-13
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Databricks Unity Catalog 中的 Iceberg 外部访问及 Doris 现有 Iceberg 能力；native Delta Lake Catalog 见[独立调研](databricks-native-delta-lake-catalog-research.md)。

## 1. 结论摘要

1. **已有 Databricks catalog 中符合外部访问条件的 managed Iceberg 表，可以由 Doris 按原表名直接访问，不要求为 Doris 再创建一份 catalog、external table 或复制数据。** “直接”是 Doris 通过 Unity Catalog 的 Iceberg REST API 发现表并取得临时凭证，然后直读对象存储；不是绕过 UC 读取 `__unitystorage` URI。

2. **Doris 教程中的 External Location 不等于 External Table。** 教程把 External Location 选作新 catalog 的 managed storage root，未指定 `LOCATION` 创建的 Iceberg 表仍是 managed table。这样做的关键目的是把 managed storage 放到支持外部 FileIO 和 credential vending 的 customer-managed storage，而不是要求 Doris 只能访问 external table。

3. **是否需要新建 External Location，取决于已有表的存储条件，不取决于它是不是 managed table。** 已有 catalog/schema 若继承了可 vending 的 customer-managed storage，通常不需要新建；如果表位于 Databricks default storage，当前不支持外部 Iceberg/Delta FileIO 和 credential vending，则不能靠给 Doris 配一个长期存储密钥绕过限制。

4. **Doris 已有正确的 Iceberg REST 主干，但尚不能把“Databricks 三云 managed Iceberg”整体标为 production-ready。** 当前具备 REST、OAuth、access delegation 和 native Iceberg scan/write 基础；缺口包括真实环境门禁、凭证过期/刷新、vending 失败时 fail-closed、Databricks capability 校验，以及 AWS/Azure/GCP 的完整认证。

5. **欧洲 Azure 现场不是“未新建 External Location”导致失败的优先解释。** 现场已成功列库/列表，`loadTable` 又返回 `abfss://.../__unitystorage/...` metadata 和 table-scoped ADLS SAS，证明 UC 控制面、表解析和 credential vending 已经走通。当前 Doris 会过滤 Databricks 返回的 `adls.sas-token.*`，也没有形成 Azure Iceberg FileIO/SAS 到执行层的完整闭环，这是高置信候选根因；由于缺少原始 `SELECT` 错误和 FE/BE 日志，尚不能写成最终根因。

## 2. 先把五个概念分开

| 概念 | 是什么 | 不是什么 |
| --- | --- | --- |
| Unity Catalog | Databricks 的表目录、权限、治理和临时凭证控制面 | 表格式或对象存储 |
| Managed table | UC 管理表的位置和生命周期 | “数据一定存放在 Databricks 计算节点” |
| External table | UC 登记用户管理的数据路径 | 使用了 External Location 的所有表 |
| External Location | UC 中“云存储路径 + Storage Credential + 权限”的治理对象 | External Table 的同义词 |
| Iceberg REST Catalog | 外部 Iceberg client 按表名获取 metadata、capability 和临时凭证的开放协议 | 原始对象存储路径访问 |

在 customer-managed storage 场景中，managed table 的数据仍可位于客户自己的 S3、ADLS 或 GCS，只是位置和生命周期由 UC 管理。Databricks 新的 default storage 是 fully managed storage，外部 FileIO 能力不同，不能把两种 managed storage 混为一谈。

## 3. External Location 问题

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

### 4.1 表类型支持矩阵

| 表类型 | Iceberg REST 外部能力 | Doris 应如何处理 |
| --- | --- | --- |
| Managed Iceberg，存储支持外部 FileIO/vending | 读写 | 作为主要认证目标，按服务端 capability 限制操作 |
| Foreign Iceberg | 只读 | 保守标为只读；官方页面对 vending 的表述仍有差异，需 live test |
| Delta table with Iceberg reads | 只读 | 读取异步生成的 Iceberg metadata；不等价于 native Delta，也不可据此写 Delta |
| Default-storage managed table | 不支持外部 FileIO/vending | 在规划期给出明确诊断；使用 JDBC/ODBC、受限 OpenSharing 或迁移副本 |
| 带 row filter/column mask 的表 | 普通文件凭证不足以执行策略 | 在支持 cross-engine ABAC server-side planning 前明确拒绝，不能绕过策略 |

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

| 产品 | 公开支持边界 | 对 Doris 的启示 |
| --- | --- | --- |
| Snowflake | 通过 Unity Iceberg REST 和 catalog-linked database 访问 managed Iceberg；vended credentials 模式无需 Snowflake external volume，双向访问已 GA | 标准 Iceberg REST 足以承载商业级集成；无需把 managed table 变成 external table |
| Apache Spark + Iceberg | `SparkCatalog` 直连 UC Iceberg REST，并加载对应云 bundle | 是协议和三云 FileIO 的开源基准，但不是裸 Spark core 自带 UC 能力 |
| Starburst Enterprise | 新版 STS 支持 UC Iceberg external R/W、managed R 和 server-side planning；较新 LTS 文档仍要求 read-only | 能力必须带版本；ABAC 需要服务端 planning，不能只靠 vending |
| ClickHouse | UC 指南明确主要支持 external-location 表，managed Iceberg 不支持；能列 metadata 不代表能读取 managed data | 必须把 catalog 控制面、storage vending 和 FileIO 分层验收 |
| Doris 当前 | 标准 Iceberg REST 与 native scan/write 已存在，但三云和 credential lifecycle 未认证完整 | 增强现有实现，不建立平行的 Databricks 专用 Iceberg 栈 |

竞品共同采用“开放 catalog API + 短期凭证 + 原生格式引擎”。成熟方案没有把“猜 managed path + 长期 AK/SK”作为主路径。

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
- [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Starburst Enterprise Iceberg with Unity Catalog](https://docs.starburst.io/481-e/connector/starburst-iceberg-unity.html)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
