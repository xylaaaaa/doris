# Apache Doris 对 Databricks Iceberg 的支持完善调研

> 状态：调研结论，不是实现方案或最终接口承诺
>
> 调研更新：2026-08-14
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Databricks Unity Catalog 中的 Iceberg 外部访问及 Doris 现有 Iceberg 能力；native Delta Lake Catalog 见[独立调研](databricks-native-delta-lake-catalog-research.md)。
>
> Azure 实际联调环境的资源清单和操作步骤见[Databricks Azure Managed Iceberg 与 Doris 联调环境搭建](databricks-azure-iceberg-e2e-setup.md)。

## 1. 直接结论

1. **External Location 不是 External Table。** Doris 教程用它为 catalog 准备支持外部 FileIO 和 credential vending 的 customer-managed storage；未指定 `LOCATION` 创建的表仍是 managed table。
2. **符合条件的已有 managed Iceberg 可以原地访问。** Doris 按表名通过 Unity Catalog Iceberg REST 获取 metadata 和短期凭证，再读取原文件；不需要复制数据、重建表或绕过 UC 猜路径。
3. **不是所有 managed table 都能直读。** 位于可 vending 的 customer-managed storage 时可以；位于 Databricks default storage 时，当前外部 Iceberg/Delta client 不支持 FileIO 和 credential vending。
4. **当前 Azure 客户问题高度疑似 Doris 没有消费 UC 返回的 ADLS SAS。** 客户的 `loadTable` 已返回 `adls.sas-token.<host>`，但 Doris 当前凭证过滤规则不接受 `adls.` 前缀，该凭证会在转换为执行层存储配置前被过滤；仍需客户日志和网络结果排除其他原因。
5. **竞品使用相同主链路。** Snowflake、Spark、Starburst 和 ClickHouse 都以 Unity Catalog 为控制面，再由各自的格式引擎读文件；差别主要在支持范围、版本认证和治理能力。

## 2. 先把五个概念分开

| 概念 | 是什么 | 不是什么 |
| --- | --- | --- |
| Unity Catalog | Databricks 的表目录、权限、治理和临时凭证控制面 | 表格式或对象存储 |
| Managed table | UC 管理表的位置和生命周期 | “数据一定存放在 Databricks 计算节点” |
| External table | UC 登记用户管理的数据路径 | 使用了 External Location 的所有表 |
| External Location | UC 中“云存储路径 + Storage Credential + 权限”的治理对象 | External Table 的同义词 |
| Iceberg REST Catalog | 外部 Iceberg client 按表名获取 metadata、capability 和临时凭证的开放协议 | 原始对象存储路径访问 |

在 customer-managed storage 场景中，managed table 的数据仍可位于客户自己的 S3、ADLS 或 GCS，只是位置和生命周期由 UC 管理。Databricks 新的 default storage 是 fully managed storage，外部 FileIO 能力不同，不能把两种 managed storage 混为一谈。

## 3. 为什么需要 External Location，已有表能否直接访问

Doris [官网教程](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/) 创建 External Location，是为了给 catalog 准备一块支持外部 FileIO 和 credential vending 的 customer-managed storage：

```text
External Location
  └── catalog/schema managed storage
      └── __unitystorage/.../managed Iceberg table
```

表创建时没有指定 `LOCATION`，所以它仍是 managed table。External Location 保存底层路径和长期存储身份；Doris 获得的是 UC 按表权限签发的短期凭证，不是长期 AK/SK。

已有表是否需要新建 External Location，要看实际存储：

| 已有表场景 | 能否原地直读 | 是否为 Doris 新建 External Location |
| --- | --- | --- |
| Managed Iceberg 已位于可 vending 的 customer-managed storage | 可以，按原表名走 Iceberg REST | 不需要，复用现有 managed storage |
| Managed Iceberg 位于 default storage | 当前不能通过外部 FileIO 直读 | 新建不会移动原表；只能迁移数据或改用 JDBC/ODBC、受限 OpenSharing |
| 存储或 capability 不明确 | 不能只看 `MANAGED` 判断 | 用真实 `loadTable` 检查是否返回可用临时凭证 |

直接访问链路是：

```text
Doris ──表名──> Unity Catalog Iceberg REST
  │                  └── metadata + capability + 短期凭证
  └────────────────────> S3 / ADLS / GCS 文件
```

这意味着不复制数据、不重建 external table，也不把查询交给 Databricks SQL Warehouse。读取 managed table 通常还需要开启 external data access，并授予 `EXTERNAL USE SCHEMA`、`USE CATALOG`、`USE SCHEMA` 和 `SELECT`。

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

| UC 中的对象 | 它实际是什么 | 公开支持边界 |
| --- | --- | --- |
| Managed Iceberg | UC 原生管理位置、metadata 和生命周期的 Iceberg 表 | Iceberg REST 支持读写；具体操作以服务端 capability 为准 |
| Foreign Iceberg | UC 从外部 Iceberg catalog 登记的表，生命周期不由 UC 管理 | 只读；credential vending 的官方表述存在差异，需要真实环境确认 |
| Delta table with Iceberg reads | 本质仍是 Delta 表，但 Databricks 为外部 Iceberg client 异步生成兼容 metadata | 只读；不等于 native Delta 支持，也不能通过 Iceberg 路径写 Delta |

### 4.2 第二步：检查这张表能否由 Doris 直接读写

知道对象形式后，还必须分别检查存储、临时凭证和治理策略。下面这些是访问条件或阻断因素，不是新的表类型。

| 检查项 | 观察到的情况 | 对外部客户端的影响 |
| --- | --- | --- |
| 底层存储 | customer-managed storage 支持外部 FileIO/vending | UC 可以签发临时凭证；客户端还必须支持对应的 AWS、Azure 或 GCP 凭证 |
| 底层存储 | Databricks default storage | 当前不能通过 Iceberg REST + FileIO 直读；可选 JDBC/ODBC、受限 OpenSharing 或迁移副本 |
| 服务端 capability | 不支持当前 read/write/DDL 操作 | 该操作不可用；不能把通用 Iceberg 能力直接套用到 Databricks |
| 临时凭证 | UC 未返回凭证，或客户端不认识返回的云凭证 | metadata 可能可见，但底层文件无法读取 |
| 行列策略 | 表带有 UC row filter/column mask | 普通文件扫描无法执行策略，需要 cross-engine ABAC/server-side planning |

因此 Doris 的判断顺序应该是：

```text
识别 managed / foreign / Delta with Iceberg reads
        ↓
检查底层 storage 是否支持外部 FileIO/vending
        ↓
检查服务端 capability 和 UC 治理策略
        ↓
检查客户端能否消费返回的云凭证
        ↓
决定允许读、允许写，还是给出明确拒绝
```

Databricks managed Iceberg 也不是通用 Iceberg 能力的无条件超集。截至本调研日期，官方限制涉及文件格式、delete 表达、branches/tags、partition transform、部分类型和 UC 控制的 table properties。因此不能因为客户端支持通用 Iceberg，就推导出它在 Databricks 上支持全部语法。

## 5. 竞品支持对比

各产品的主链路相同：连接已有 Unity Catalog，由 UC 返回表信息和短期凭证，再用自己的 Iceberg/FileIO 引擎读文件。主要差别如下：

| 产品 | Databricks 接入方式 | 公开支持边界 | 调研结论 |
| --- | --- | --- | --- |
| [Snowflake](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity) | Unity Iceberg REST；OAuth；vended credentials 或 external volume | catalog-linked database 支持受支持的读写操作；AWS/Azure/GCP；相关能力已 GA | 本次比较中商业化最完整，自动同步、凭证和写入形成完整产品链路 |
| Spark + Iceberg | SparkCatalog + Iceberg runtime + 对应云 bundle | Managed Iceberg R/W、Foreign Iceberg R、Delta with Iceberg reads R，以 UC capability 为准 | Databricks 官方参考客户端，适合作为协议和三云 FileIO 基线 |
| [Starburst Enterprise 481-e](https://docs.starburst.io/latest/connector/starburst-iceberg-unity.html) | Iceberg connector + UC REST + OAuth/vending | external R/W、managed R；AWS/Azure/GCP；支持 Databricks server-side planning | 商业增量主要是 external write、行列策略和版本认证；481-e STS 能力不能直接套用到旧 LTS |
| [Trino OSS](https://trino.io/docs/current/object-storage/metastores.html) | 通用 Iceberg REST + OAuth/vending | Databricks UC 公开配置仍要求只读；支持三云可刷新凭证 | 是 Starburst 的开源基线，不具备 SEP 的全部 Databricks 商业能力 |
| [ClickHouse OSS/Cloud](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog) | DataLakeCatalog + Unity Catalog | UC 指南主要覆盖 external-storage tables；支持矩阵标为 Beta，指南标为 experimental | managed table 和写入边界不清，应按具体版本和 PoC 判断；Cloud 增量主要在托管、缓存和并行执行 |

由此可以得到三个结论：

1. 没有成熟产品要求先复制 managed table；它们都优先连接已有 catalog。
2. catalog 控制面和文件数据面是两层能力；能列出表不等于能消费三云临时凭证。
3. “完成度”指声明范围内是否端到端可用；“产品化程度”还包括配置、诊断、安全、运维和版本化支持矩阵。

## 6. Doris 当前支持状态

有了第 5 节的竞品基线后，本节只总结 Doris 已经具备的能力、尚未验证完整的范围和客户现场证据，不展开实现设计。

### 6.1 已有基础

当前 Doris 已有：

- 标准 Iceberg REST Catalog；
- OAuth/PAT 认证和 vended-credentials 请求；
- native Iceberg metadata、manifest、Parquet 读取和通用写入基础。

这说明 Doris 的总体协议路线与竞品一致，不需要把现有能力描述成另一种 Databricks 专用 catalog。

### 6.2 尚未完成系统认证的范围

| 范围 | 当前调研结论 |
| --- | --- |
| 云存储 | AWS 已有基础；Azure table-scoped SAS 尚未形成已验证闭环；GCP 未完成系统认证 |
| 短期凭证 | 长查询、排队和重试跨越凭证有效期的行为尚未完成认证 |
| 表与操作 | managed、foreign、Delta Iceberg reads 以及 Databricks 特有 DDL/DML 限制尚未形成正式支持矩阵 |
| 治理 | UC row filter/column mask 需要 server-side planning，普通文件扫描不能等价执行 |
| 测试 | 缺少覆盖三云、表类型、读写和凭证过期场景的完整真实环境结果 |

### 6.2.1 Doris Iceberg 能力与证据矩阵

以下矩阵把“代码存在”和“真实环境已经验证”分开。`代码/单测` 不等于客户环境已经可用。

| 场景 | Doris 当前判断 | 证据状态 | 下一步 |
| --- | --- | --- | --- |
| Azure managed Iceberg + customer-managed ADLS | 目标支持路径：UC REST 返回 `adls.sas-token.*`，Doris 转换为 ABFS SAS 并读取 `abfss://` | 代码、162 个 FE 单测、FE/BE 构建已通过；真实环境待验证 | 用联调环境完成 `loadTable` 和 `SELECT` |
| Azure external Iceberg + External Location | 协议上应复用 Azure FileIO 和 vended SAS；具体表能力由 UC 返回 capability 决定 | 未做本轮真实验证 | 主场景通过后补一张 external table |
| Azure managed Iceberg + default storage | 外部 FileIO 和 credential vending 是 Databricks 官方限制 | 官方文档已确认 | 作为预期失败场景记录，不作为 Doris 缺陷 |
| AWS managed/external Iceberg + vended credentials | Doris 已有 AWS 基础路径 | 代码已有；本轮未做回归环境验证 | 后续用现有 AWS 环境回归 |
| GCP managed/external Iceberg + vended credentials | 当前没有系统认证结论 | 未验证 | 单独准备 GCP 环境或明确暂不支持 |
| Foreign Iceberg | UC 对 credential vending 和刷新有额外限制，不能按 managed Iceberg 推断 | 官方边界已记录；Doris E2E 未验证 | 按 UC capability 单独判断 |
| Row filter/column mask | 普通文件扫描不能自动等价执行 UC 策略 | 官方边界已记录 | 暂不宣称支持，另做 server-side planning 调研 |
| 长查询跨越 SAS expiration | 当前 PR 消费当前 table load 返回的 SAS，未增加查询中途刷新 | 代码范围已确认 | 作为后续独立能力，不纳入首轮通过条件 |

能力状态使用以下证据等级：

- **官方限制**：Databricks 官方明确不支持或有前置条件；
- **代码/单测**：Doris 代码和本地测试已覆盖；
- **真实环境**：已在 Azure Databricks + ADLS 中完成端到端验证；
- **未验证**：不能据此对客户承诺支持。

这里需要修正一个容易误读的说法：**Doris 不是完全不支持 Azure。** Doris 已有 Azure account key/OAuth 等静态存储配置；当前未验证完整的是 Databricks Iceberg REST 返回的 table-scoped `adls.sas-token.*` 数据访问链路。

### 6.3 欧洲 Azure 现场的证据链

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

**代码观察：** vended credential 在转换为执行层存储配置前，会经过 `CredentialUtils.filterCloudStorageProperties()`。当前白名单包含 `azure.`，但不包含 Databricks/Iceberg 使用的 `adls.`，因此 `adls.sas-token.<host>` 会被过滤。FE 依赖中也只有 `iceberg-aws`，没有 `iceberg-azure`。这两点共同说明，当前 Doris 没有形成 Databricks ADLS SAS 的完整消费链路。

**结论：** “Doris 没有正确消费 UC 返回的 ADLS SAS”是当前代码和接口证据共同支持的高置信候选根因，但还不是现场最终诊断。仍需 Doris 版本、完整 `SELECT` 错误、FE/BE 日志，以及 Doris 到目标 ADLS endpoint 的网络、防火墙、token host 和有效期结果，排除网络或 token 使用条件问题。

### 6.4 中国区输入的证据等级

“2026 年 6 月某中国区 workspace 未部署 Iceberg REST、无 ETA”只能记录为特定现场和时间点的反馈，不能泛化为当前全部中国区能力。Azure 中国官方文档在 2026-07-24 已给出 Iceberg REST endpoint 和中国云专用域名。产品测试应记录 cloud、region、workspace、HTTP 状态和支持工单，按 workspace 探测能力，而不是静态判断整个区域支持或不支持。

## 7. 主要官方资料

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
