# Databricks Native Delta 开发文档

更新时间：2026-08-22
状态：实验性实现，供开发和验证使用

本文说明 Doris 当前 Native Delta connector 的代码结构、运行流程、配置方式、测试入口和继续开发时必须遵守的边界。它是开发文档，不代表 Doris 已经完成生产级 Databricks Delta 支持。

## 1. 开发目标

Native Delta 要解决的是让 Doris 直接理解 Delta transaction log，并把 Delta 表作为 Doris 外部表访问。目标访问路径有两种：

```text
Path Delta
  用户提供表路径和存储配置
        |
        v
  Delta Kernel 读取 _delta_log

Unity Catalog Delta
  catalog.schema.table
        |
        v
  Unity REST 发现表、获取能力和临时凭证
        |
        v
  Delta Kernel 读取 snapshot / catalog-managed log tail
```

以下路径不属于 Native Delta：

- `type = iceberg` 连接 Databricks Iceberg REST。它读取的是 Delta 生成的 Iceberg metadata，只读兼容，不解析 Delta log。
- `type = trino-connector`、`trino.connector.name = delta_lake`。它是 Trino connector 兼容路径，不是 Doris 自己的 Delta connector。
- Unity 表发现失败后猜测对象存储路径。managed 或 catalog-managed 表不能绕过 Unity Catalog 访问。

## 2. 当前架构

### 2.1 模块分层

| 层 | 代码位置 | 责任 |
| --- | --- | --- |
| Connector API | `fe/fe-connector/fe-connector-api` | 定义 connector capability、表句柄、snapshot、写入句柄和写入操作接口。 |
| 插件入口 | `fe/fe-connector/fe-connector-delta/src/main/java/org/apache/doris/connector/delta/DeltaConnectorProvider.java` | 通过 `ServiceLoader` 注册 `type = delta`，校验 CREATE CATALOG 属性并创建 connector。 |
| Connector 门面 | `DeltaConnector.java` | 组装 Hadoop/对象存储配置、选择 adapter、暴露 scan provider、metadata 和 capability。 |
| Catalog adapter | `DeltaCatalogAdapter.java`、`DeltaPathCatalogAdapter.java`、`UnityDeltaCatalogAdapter.java` | 将路径或 Unity 控制面转换为统一的 `DeltaTableHandle` 和 snapshot 操作。 |
| Unity client | `UnityDeltaClient.java` | 调用 Unity Catalog Delta API、Tables API、临时凭证 API 和 catalog-managed commit API。 |
| Delta 格式层 | `DeltaKernelSnapshotLoader.java`、`DeltaKernelSnapshot.java`、`DeltaKernelWriter.java` | 使用 Delta Kernel 解析 protocol、schema、checkpoint、active files、deletion vectors，并提交 Delta actions。 |
| Doris 数据面 | `DeltaScanPlanProvider.java`、`DeltaScanRange.java` | 将 Delta active files 转为 Doris Parquet scan range，复用现有 Parquet reader。 |
| FE catalog 路由 | `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java` | 将外部表的 create、insert、overwrite、delete、update、merge、truncate、drop 路由到 connector SPI。 |
| 测试 | `fe-connector-delta/src/test`、`regression-test/suites/external_table_p0/delta` | 覆盖 Kernel fixture、Unity HTTP fixture 和隔离 FE/BE SQL 回归。 |

### 2.2 关键对象关系

```text
DeltaConnectorProvider
        |
        v
DeltaConnector
   +----+-------------------+
   |                        |
   v                        v
DeltaPathCatalogAdapter   UnityDeltaCatalogAdapter
   |                        |
   +----------+-------------+
              v
       DeltaTableHandle
              |
              v
       DeltaKernelSnapshot
              |
      +-------+--------+
      |                |
      v                v
DeltaScanPlanProvider  DeltaKernelWriter
      |                |
      v                v
 Doris Parquet scan   Delta commit / UC catalog commit
```

`DeltaTableHandle` 记录表路径、数据库和表名、snapshot version、表类型、是否 `catalogManaged` 以及 Unity 是否声明 `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT`。snapshot 可以绑定到 query-local handle；序列化后不会携带内存中的 Kernel snapshot，避免跨查询复用失效状态。

## 3. 依赖和版本

依赖在 `fe/pom.xml` 和 `fe/fe-connector/fe-connector-delta/pom.xml` 中维护：

| 依赖 | 当前版本 | 用途 |
| --- | --- | --- |
| `delta-kernel-api` | 4.3.1 | Delta snapshot、protocol、schema 和 transaction API。 |
| `delta-kernel-defaults` | 4.3.1 | 默认文件系统和 Delta Kernel engine。 |
| `delta-kernel-unitycatalog` | 4.3.1 | catalog-managed Delta 的 Unity integration。 |
| `unitycatalog-client` | 0.5.0 | Unity Catalog Java client、Tables API、Delta API。 |
| `unitycatalog-hadoop` | 0.5.0 | 将 Unity 返回的存储凭证映射为 Hadoop 配置。 |
| GCS connector | Doris FE 依赖版本 | GCS OAuth 和 resumable upload 所需的文件系统实现。 |

升级 Delta Kernel 或 Unity client 时，必须重新验证 protocol、catalog-managed commit、临时凭证和三云文件系统；不能只通过 Maven 编译判断兼容。

## 4. Catalog 配置

### 4.1 Path Delta

Path catalog 是单表入口，适合用户明确提供 Delta 路径和存储访问配置的场景：

```sql
CREATE CATALOG delta_path PROPERTIES (
    'type' = 'delta',
    'delta.catalog.type' = 'path',
    'delta.database' = 'default',
    'delta.table' = 'events',
    'delta.table.path' = 's3://bucket/events',
    'delta.write.enabled' = 'true',
    'test_connection' = 'false'
);
```

必需属性：`delta.database`、`delta.table`、绝对 URI 形式的 `delta.table.path`。对象存储配置沿用 Doris 的 storage properties，并同时传给 Delta Kernel 和 Doris BE scan。

Path catalog 的 `DROP TABLE` 始终拒绝，避免把删除外部表误变成删除对象存储目录。删除 catalog 不删除数据。

### 4.2 Unity Catalog Delta

Unity catalog 按 `catalog.schema.table` 访问 Unity Catalog：

```sql
CREATE CATALOG delta_unity PROPERTIES (
    'type' = 'delta',
    'delta.catalog.type' = 'unity',
    'unity.uri' = 'https://<workspace-host>',
    'unity.auth.type' = 'pat',
    'unity.token' = '<token>',
    'unity.catalog' = 'main',
    'delta.write.enabled' = 'true',
    'delta.create.enabled' = 'true',
    'delta.drop.enabled' = 'true',
    'test_connection' = 'false'
);
```

OAuth 认证使用 `unity.auth.type = oauth`，并提供 `unity.oauth.uri`、`unity.oauth.client-id` 和 `unity.oauth.client-secret`。`unity.uri` 必须是 workspace 根地址，不带 API path；非 loopback 地址必须使用 HTTPS。token、client secret 和 vended credential 不得写入日志或测试输出，生产环境应使用 Doris 的安全配置管理方式。

可选超时属性：

| 属性 | 默认值 | 作用 |
| --- | ---: | --- |
| `unity.connect-timeout-ms` | 10000 | Unity HTTP 建连超时。 |
| `unity.read-timeout-ms` | 30000 | Unity HTTP 读取超时。 |
| `unity.credential-min-lifetime-ms` | 60000 | FE 接受临时凭证所需的最小剩余有效期。 |

`delta.write.enabled` 只打开 connector 的写入候选能力；真正能否写入还要经过表级 capability、table feature、凭证有效期和 snapshot 并发检查。`delta.create.enabled` 和 `delta.drop.enabled` 分别控制 Unity managed 建表和 Unity 删除 catalog 注册项。

## 5. 读取流程

### 5.1 Provider 和 adapter 选择

1. `DeltaConnectorProvider.validateProperties` 校验类型、URI、认证方式、超时和写入开关。
2. `DeltaConnector` 根据 `delta.catalog.type` 选择 path 或 Unity adapter。未配置时，存在 `delta.table.path` 默认推断为 path，否则默认 Unity。
3. adapter 解析数据库和表名，构造 `DeltaTableHandle`，并在需要时加载当前 snapshot。

### 5.2 Path 读取

`DeltaPathCatalogAdapter` 直接使用 `DeltaKernelSnapshotLoader` 从表路径加载 `_delta_log`。Kernel 负责：

- 读取 protocol、metadata、JSON commit 和 checkpoint；
- 验证 reader/writer protocol 和已支持的 table features；
- 生成 schema、分区列、active data files 和 deletion vectors；
- 按 version 或 timestamp 加载历史 snapshot。

Path 读取不经过 Unity，也不获得 Unity 的权限治理语义。

### 5.3 Unity 读取

`UnityDeltaClient` 先调用配置协商接口，再调用 Tables API 和 Delta table API：

1. 列 schema 和表时按 API 分页；Databricks Tables API 使用不超过 50 的 `max_results`。
2. 只暴露 `data_source_format = DELTA` 且声明 `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT` 的表。
3. 发现 `row_filter`、顶层 `column_masks` 或 `columns[].mask` 时 fail-closed；当前没有实现 cross-engine ABAC 执行。
4. 加载表 metadata、location、table UUID、最新 version 和 catalog-managed commit/log tail。
5. 按读取操作申请临时存储凭证，将 AWS、Azure SAS 或 GCS OAuth 配置传给 Kernel 和 BE。

catalog-managed 表必须使用 Unity 返回的 catalog-held snapshot/log tail。不能只根据 location 扫描 `_delta_log`，否则可能读到未被 catalog 发布的状态。

### 5.4 Snapshot 和 scan plan

`DeltaScanPlanProvider` 使用 handle 绑定的 snapshot 生成 `DeltaScanRange`：

1. 根据 partition predicate 进行分区裁剪。
2. 将 active Parquet files 转成 Doris scan ranges。
3. 将删除向量、文件大小、分区值和 storage properties 写入 scan range。
4. 由现有 Doris Parquet reader 在 BE 执行数据读取。

`FOR VERSION AS OF` 和 `FOR TIME AS OF` 通过 `ConnectorTableSnapshot` 生成新 handle。当前要求历史 snapshot 与当前表拥有兼容的 schema 和分区列；发生 schema/partition evolution 时明确拒绝，而不是用错误 schema 执行查询。

## 6. 写入流程

### 6.1 通用门禁

写入路径必须同时满足：

```text
catalog delta.write.enabled = true
        |
        v
connector capability 支持目标操作
        |
        v
Unity 表声明 external-engine write capability（如适用）
        |
        v
Delta protocol / table features / schema / credential lifetime 检查通过
        |
        v
以 pinned snapshot 开始事务并检查并发版本
```

只读 Unity 表不会因为 catalog 全局设置了 `delta.write.enabled` 就进入 writer 路径。普通 Unity managed Delta 的写入当前拒绝；catalog-managed 写入还要求 `delta.enableInCommitTimestamps = true`，且 writer feature 处于当前实现允许的集合。

### 6.2 文件写入和 Delta commit

1. FE 通过 `getWriteConfig` 选择 Parquet/Snappy、写入位置、分区列和 BE storage properties。
2. BE 生成数据文件并返回 `ConnectorFileCommitInfo`。
3. `DeltaKernelWriter` 将文件转换为 add actions；overwrite 先为 pinned snapshot 的 active files 生成 remove actions。
4. Delta Kernel 提交 transaction。并发版本改变时提交失败并向上报告，不覆盖其他 writer 的提交。
5. catalog-managed 表使用 Unity catalog committer；external/path 表使用对应的 Delta transaction commit。

### 6.3 当前 DML 语义

当前 DELETE、UPDATE、MERGE 是 copy-on-write 切片：读取一个 pinned snapshot，重写幸存或修改后的行，再以原子 remove+add transaction 替换 active files。

| 操作 | 当前支持 | 当前限制 |
| --- | --- | --- |
| `INSERT` | path、Unity external、catalog-managed 的实验性 append | 受 schema、protocol、凭证和 capability 限制。 |
| `INSERT OVERWRITE` | 原子 remove+add | 要求并发 snapshot 未变化。 |
| `DELETE` | 整表和 `WHERE` | 不支持 `PARTITION`、`USING`、CTE、子查询、`ORDER BY/LIMIT`。 |
| `UPDATE` | 确定性表达式、别名、`IS NULL` | 不支持 `FROM`、CTE、子查询、非确定性谓词、`ORDER BY/LIMIT`。 |
| `MERGE` | matched DELETE/UPDATE、not-matched INSERT | source 当前要求 Doris UNIQUE KEY 表，`ON` 覆盖全部 source key；不支持 CTE、子查询 source 和非确定性条件。 |
| `TRUNCATE TABLE` | 整表 remove-only overwrite | 不支持分区级 truncate；新增回归输出仍待生成。 |
| `DROP TABLE` | Unity 删除 catalog 注册项 | 必须同时打开 write/drop；path 永不删除对象存储目录。 |

带 Unity row filter 或 column mask 的表，DELETE/UPDATE/MERGE 继续拒绝，不能把普通文件扫描当作策略执行。

## 7. Protocol 和类型边界

Kernel 层只接受已经审核过的 protocol 和 table features。新增 feature 时必须同时更新：

1. `DeltaKernelSnapshotLoader` 的 reader/writer feature 校验；
2. schema/type mapping 和 Parquet reader 的映射；
3. scan range 对 deletion vector、column mapping 等物理信息的处理；
4. writer 的 protocol、commit action 和并发测试；
5. Path、Unity external、Unity catalog-managed 三类 fixture。

当前应保持 fail-closed 的项目包括 generated/default/constraint columns、type widening、variant 和未审核的 reader/writer feature，以及 schema evolution 的历史 time travel。不要通过忽略 metadata 字段来“兼容”新 feature。

## 8. 测试和开发验证

### 8.1 FE connector 单测

Delta connector 的单测位于 `fe/fe-connector/fe-connector-delta/src/test`，覆盖 provider 校验、snapshot、分区裁剪、类型映射、Kernel writer 和 Unity HTTP fixture。使用仓库预置脚本运行指定测试时，可以把 Delta 模块加入 `EXTRA_FE_MODULES`：

```bash
EXTRA_FE_MODULES=delta=fe-connector/fe-connector-delta \
  ./run-fe-ut.sh --run org.apache.doris.connector.delta.DeltaConnectorVerticalSliceTest
```

也应运行 `UnityDeltaCatalogAdapterTest`、`DeltaKernelSnapshotLoaderTest`、`DeltaKernelWriterTest` 和 `DeltaPartitionPrunerTest`。完整 FE 构建使用仓库规定的 `./build.sh --fe`；不要用独立手工 Maven 命令替代标准构建结论。

### 8.2 隔离 FE/BE 回归

回归脚本位于 `regression-test/suites/external_table_p0/delta`：

```bash
./run-regression-test.sh --run \
  -d regression-test/suites/external_table_p0/delta \
  -s test_native_delta_path

./run-regression-test.sh --run \
  -d regression-test/suites/external_table_p0/delta \
  -s test_native_delta_unity
```

回归结果必须由测试框架生成，不能手写 `.out`。新增或修改 case 时，查询结果使用 `order_qt` 或显式 `ORDER BY`，错误场景使用 `test { sql, exception }` 模式。

### 8.3 真实 Databricks E2E

本地 Delta fixture 只能证明协议和代码路径，不能证明 Databricks 兼容性。产品化前至少需要在启用 Unity Catalog 的真实 workspace 验证：

- external、managed、catalog-managed Delta 的表发现和权限；
- `HAS_DIRECT_EXTERNAL_ENGINE_READ_SUPPORT` 与 write capability 的实际返回；
- AWS、Azure ADLS Gen2、GCS 的临时凭证格式、有效期和网络访问；
- Databricks/Spark 与 Doris 双向写入后的 snapshot、schema、分区和 null 语义；
- catalog commit 并发冲突、重试和失败恢复；
- 长查询中的凭证续期，以及刷新失败时的可诊断错误。

## 9. 继续开发顺序

按风险而不是按 SQL 数量推进：

1. 先完成真实 workspace 的 read-only E2E 和三云 credential 记录，确认 Unity API 响应与本地 fixture 一致。
2. 补齐 BE 长查询凭证刷新协议，覆盖刷新并发、过期和不落日志。
3. 扩展 protocol/table feature 和 schema evolution，并为每个 feature 添加正向和负向 fixture。
4. 完成 external Delta 的互操作写入和并发 commit 验证，再评估 catalog-managed/managed write 的产品边界。
5. 补齐 DML 语法、分区操作、维护操作和监控指标，最后再制定 GA/版本兼容矩阵。

以下内容在完成上述验证前不得标为已支持：完整 Databricks managed Delta、所有 Unity policy 表、自动 credential refresh、完整 Delta SQL DML、所有 Delta table features，以及 path 和 Unity 之间完全相同的治理语义。

## 10. 关联文档和代码

- [Native Delta 方向性调研](databricks-native-delta-lake-catalog-research.md)
- [当前进度与待办](databricks-native-delta-lake-catalog-status.md)
- [Delta connector 模块](../fe/fe-connector/fe-connector-delta/pom.xml)
- [Path 回归](../regression-test/suites/external_table_p0/delta/test_native_delta_path.groovy)
- [Unity 回归](../regression-test/suites/external_table_p0/delta/test_native_delta_unity.groovy)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
- [Databricks Unity REST 外部 Delta client](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Databricks credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
