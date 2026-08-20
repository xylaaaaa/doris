# Apache Doris Native Delta Lake Catalog 方向性调研

> 以 Databricks Unity Catalog 作为第一个 catalog adapter
>
> 状态：方向性调研，不是详细设计或实施计划
>
> 调研更新：2026-08-14
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Doris 原生 Delta Lake Catalog 和 Databricks Delta 外部访问；Databricks Iceberg、External Location 问题见[独立调研](databricks-iceberg-unity-catalog-research.md)。

本文以厂商正式文档作为产品能力依据。Preview/Beta 状态具有时效性，进入实现阶段前需要重新核对。

## 1. 调研目标与核心结论

这项调研主要回答三个问题：Doris 现有 Iceberg REST 能否用于 native Delta、是否需要 Unity Catalog、Doris 应选择什么总体方向。

结论如下：

1. **Doris 当前没有 native Delta Lake Catalog。** 现有 Delta 方案依赖实验性的 Trino connector plugin，可以作为过渡方案，但不等于 Doris 原生能力。
2. **Iceberg REST 不能用来实现 native Delta。** 它可以把开启 Iceberg Reads 的 Delta 表按照 Iceberg metadata 只读访问，但不会让 Doris 成为 Delta client。
3. **以 native Delta 方式按表名访问 Unity Catalog 中的 Delta，尤其是 managed Delta，需要 Unity adapter。** 这里的“引入 Unity”是增加客户端适配层，不是让 Doris 部署一套 Unity Catalog 服务。
4. **Unity adapter 不是所有 Delta 场景的前提。** 用户显式提供 URI 和存储权限的 path Delta 可以不经过 Unity；但 path 模式不能作为 managed table 的 fallback。
5. **Native Delta 需要独立的 Delta 格式层。** 建议优先评估 Delta Kernel，避免长期自行追踪 Delta protocol；当前调研不决定 Java/Rust、FE/BE 放置或最终接口。
6. **方向上先完成 native read，再评估 write。** Databricks managed Delta 的外部 create/write 仍有 Public Preview 和客户端能力限制。

## 2. 三种访问路径必须分开

| 场景 | 实际读取的格式/元数据 | 是否需要 Unity adapter | 是否属于 native Delta |
| --- | --- | --- | --- |
| Delta-as-Iceberg | Databricks 为 Delta 表生成的 Iceberg metadata，经 Iceberg REST 读取 | 使用现有 Iceberg REST 即可 | 否，只读兼容路径 |
| Path Delta | 用户给出 Delta 表 URI 和对象存储权限，直接解析 `_delta_log` | 否 | 是，但不继承完整 UC 治理 |
| Unity Catalog Delta | 按 `catalog.schema.table` 发现表，取得临时凭证和 catalog 状态，再解析 Delta | 是 | 是，是 Databricks managed/external Delta 的目标路径 |

### 2.1 为什么 Iceberg REST 不够

Databricks 的 Iceberg REST endpoint 返回 Iceberg metadata 和 Iceberg FileIO 所需信息。对于 Delta 表，必须先开启 Iceberg Reads，官方能力是 managed/external Delta 均可读、不可通过该路径写 Delta。

```text
Delta table
    │ Databricks 生成 Iceberg metadata
    ▼
Unity Catalog Iceberg REST
    ▼
Doris 作为 Iceberg client 读取
```

这条路径不要求 Doris 解析 `_delta_log`，因此适合只读兼容，但不能作为 native `deltalake` catalog 的基础。即使 Iceberg REST 返回的短期凭证在存储层可能覆盖部分文件，也不能把它当成访问 Delta log、table capability 或 catalog commits 的协议承诺。

### 2.2 Native Unity Delta 为什么需要单独的 adapter

Databricks 为外部 Delta client 提供的是 Unity REST 路线。Unity adapter 负责按表名取得 table ID、storage location、访问能力和临时凭证；对于 catalog-managed Delta，还需要取得 catalog 持有的最新状态。

这里的 Unity REST 指表发现、凭证和 catalog 状态相关 API，不是上一节的 Iceberg REST endpoint。

Databricks 只会向声明了 `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT` 的表发放外部引擎读取凭证。Doris 列举或加载表时应使用该能力声明做前置判断，而不是看到表类型是 Delta 就直接访问对象存储。

Delta Kernel 或其他 Delta 格式实现负责解析 transaction log、checkpoint 和 table features。两者不是替代关系：Unity 是 catalog 控制面，Delta 格式层负责构造 snapshot。

特别需要区分：

- **Managed Delta**：Unity Catalog 管理表位置和生命周期。
- **Catalog-managed Delta**：启用了 `catalogManaged` table feature，最新提交状态由 catalog commits 协调。

Catalog-managed 表不能退化成普通 path 扫描。只读取对象存储中的 `_delta_log`，可能遗漏 Unity Catalog 中尚未发布的 log tail，得到旧 snapshot。

### 2.3 Databricks 当前公开边界

| 访问模式 | 外部读 | 外部写/建表 | 结论 |
| --- | --- | --- | --- |
| Managed Delta，经 Unity REST | 是 | Public Preview，要求 catalog commits | native Delta 目标范围，但 write 不宜作为第一阶段承诺 |
| External Delta，经 Unity REST | 是 | 是 | 可作为 native read/write 的标准场景 |
| Delta with Iceberg Reads，经 Iceberg REST | 是 | Delta write 否 | 只读兼容模式，不是 native Delta |
| External/unmanaged path Delta | 取决于客户端 Delta 能力 | 取决于客户端 Delta 能力 | 不需要 Unity，但不等价于 managed table 支持 |
| Default-storage managed Delta | 外部 FileIO 不支持 | 不支持 | 当前明确例外，不能通过猜路径或长期云密钥绕过 |

## 3. Doris 当前能力与缺口

Doris 当前可以把 Databricks Unity Catalog 当作一个 Iceberg REST 服务使用：

```text
type = iceberg
iceberg.catalog.type = rest
iceberg.rest.uri = .../api/2.1/unity-catalog/iceberg-rest
```

代码中没有独立的 Unity Catalog 类型；Databricks 是通用 `IcebergRestExternalCatalog` 所连接的一种服务端。因此，“Doris 支持通过 Unity Catalog 访问 Iceberg”不能推导为“Doris 已经有 Unity Delta client”。

Doris 当前 Delta Lake Catalog 则依赖实验性的 Trino connector plugin：

```text
type = trino-connector
trino.connector.name = delta_lake
```

方向性差距可以归纳为：

| 当前已有 | Native Delta 仍缺少 |
| --- | --- |
| 通用 Iceberg REST Catalog | 通用 native `deltalake` catalog |
| Databricks Iceberg REST 接入 | Databricks Unity Delta adapter |
| 实验性 Trino Delta plugin | Doris 自己可控的 Delta 格式与规划能力 |
| Doris 原生 Parquet reader 和执行引擎 | 将 Delta snapshot/file plan 接入 Doris 数据面 |

## 4. Doris 建议的最小方向

| 组成 | 首期方向 | 解决的问题 |
| --- | --- | --- |
| `deltalake` catalog 入口 | 新增一个统一的 Delta 入口 | 让 Doris 通过表名或路径找到 Delta 表 |
| Catalog adapter | 先实现 Unity adapter，path 作为另一种 adapter | 隔离 Unity、path、未来 HMS/Glue 的发现和权限差异 |
| Delta 格式层 | 优先评估 Delta Kernel | 解析 transaction log、checkpoint、snapshot 和 table features |
| Doris 数据面 | 复用现有 Parquet reader 和执行引擎 | 不重新实现文件扫描和 SQL 执行 |

这个方向不意味着现在就决定具体实现：

- 不需要在 Doris 中部署 Unity Catalog server；
- 不要求通用 Delta Catalog 永久绑定 Databricks；
- 不在本调研阶段决定 Java Kernel、Rust Kernel 或自行实现的最终选型；
- 不在本调研阶段定义 SQL、SPI、FE/BE 模块和 commit 调用顺序。

## 5. 竞品调研

| 产品 | Catalog 路径 | Delta 实现 | Databricks managed/write 边界 | 对 Doris 的启示 |
| --- | --- | --- | --- | --- |
| Apache Spark | Unity Catalog connector + Unity REST | 常规路径使用 Delta Spark，不以 Delta Kernel 为主 | 官方支持 managed/external read；managed create/write 通过 catalog commits，仍为 Preview | Databricks 官方正确性基线 |
| Starburst Enterprise | 商业 Unity Catalog adapter | 公开基础是 Trino 自研 Delta log 实现 | external 能力较完整；普通 managed 只读；catalog-managed DML 仍有 experimental/Preview 限制 | 证明 Delta 格式层和 Unity adapter 必须同时存在，是最接近目标的商业参考 |
| ClickHouse OSS/Cloud | `DataLakeCatalog` | Rust Delta Kernel + ClickHouse 原生 Parquet reader | 正式 Unity 指南主要承诺 external read；managed/catalog commits 未形成稳定产品承诺 | 证明 Kernel 与原生执行引擎分层可行 |
| Snowflake | Delta Sharing、Delta Direct、Iceberg REST 等多条路线 | Delta Direct 能解析 Delta log，内部库未公开 | 公开的 Databricks Delta 路线以只读为主，没有 native Unity Delta catalog commits | 证明 path reader 或 Iceberg bridge 不等于 native Unity Delta |

竞品给出的共同结论不是“必须使用同一个库”，而是需要把两件事分开：

1. 正确理解 Delta transaction log 和 snapshot；
2. 正确接入 Unity Catalog 的表发现、凭证和 catalog-managed 状态。

Spark 和 Starburst 使用已有的完整 Delta 实现；ClickHouse 从自研迁移到 Delta Kernel；Snowflake 的闭源实现无法判断。

## 6. 建议推进方向与能力边界

### 6.1 建议推进顺序

| 阶段 | 只明确目标，不在本文展开实现 |
| --- | --- |
| 方向验证 | 验证 Unity adapter + Delta 格式层 + Doris 原生 reader 的链路；比较 Kernel 复用方案 |
| 第一阶段 | native read：覆盖 Unity external/managed Delta，并正确读取 catalog-managed snapshot |
| 后续阶段 | 先评估 external Delta write；managed create/write 待 Preview、协议和互操作成熟度满足要求后再立项 |

### 6.2 进入实现前必须验证的问题

- AWS、Azure、GCP 的 temporary credentials 和刷新是否都能稳定接入 Doris；
- Delta Kernel 对目标 table features 和 catalog-managed read 的覆盖程度；
- catalog-managed 表能否稳定取得完整 log tail，确保与 Databricks/Spark snapshot 一致；
- row filter、column mask 等治理策略需要的 cross-engine server-side planning 是否在目标客户端范围内；
- Databricks managed write 的 Preview、客户端和 feature 限制是否已经满足产品化条件；
- default storage 继续作为明确不支持的负向场景。

这些问题应通过 PoC 和后续详细设计关闭，不在本调研文档中预设实现答案。

## 7. 主要官方资料

### Apache Doris

- [Doris Delta Lake Catalog（Trino Connector compatibility）](https://doris.apache.org/docs/4.x/lakehouse/catalogs/delta-lake-catalog/)
- [Doris Unity Catalog / Iceberg REST 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/)

### Databricks 与 Delta Lake

- [Unity REST access for Delta clients](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Iceberg REST access and table-type matrix](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Catalog commits](https://docs.databricks.com/aws/en/tables/features/catalog-commits)
- [Default storage limitations](https://docs.databricks.com/aws/en/storage/default-storage)
- [Cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
- [Delta Kernel Unity Catalog integration](https://docs.delta.io/kernel/rust/unity_catalog/overview.html)
- [Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)

### 竞品

- [Starburst Enterprise Delta Lake with Unity Catalog](https://docs.starburst.io/latest/connector/starburst-delta-lake-unity.html)
- [Trino Delta Lake connector](https://trino.io/docs/current/connector/delta-lake.html)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse integration with Rust Delta Kernel](https://clickhouse.com/blog/integrating-rust-delta-kernel)
- [Snowflake Delta Sharing catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-delta-sharing)
- [Snowflake Object Store / Delta Direct](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-object-storage)
- [Snowflake Unity Catalog through Iceberg REST](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity)
