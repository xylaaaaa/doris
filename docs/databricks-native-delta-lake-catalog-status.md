# Databricks Native Delta 当前进度与待办

更新时间：2026-08-22

## 1. 当前结论

Doris 已经有一个可运行的实验性 native Delta vertical slice，核心路径是：

```text
Doris Delta connector
  ├── Path adapter       -> 直接读取用户指定的 Delta 路径
  └── Unity adapter      -> 按 catalog.schema.table 访问 Unity Catalog
       └── Delta Kernel   -> 解析 Delta log、checkpoint 和 snapshot
```

当前结论不是“已经完整支持 Databricks Delta”，而是：

1. path Delta 的读写和主要 copy-on-write DML 已有本地可运行实现。
2. Unity Catalog 的表发现、临时凭证、catalog-managed snapshot 和部分写入路径已有本地协议 fixture 验证。
3. 真实 Databricks workspace、三云存储、权限、ABAC 和长查询凭证续期还没有完成 E2E 验收。
4. 因此当前代码应视为实验性能力，不能直接宣称生产级 Databricks Delta 支持。

## 2. 已完成能力

状态标记：

- **代码/单测**：代码已实现，并有对应单测。
- **本地 fixture**：使用本地 Delta 文件或 Unity Catalog HTTP fixture 验证。
- **隔离集群**：在本地 FE/BE 隔离集群执行过 SQL 回归。
- **真实 Databricks**：使用真实 workspace 验证。

| 能力 | 当前状态 | 证据与边界 |
| --- | --- | --- |
| Delta catalog 入口 | 代码/单测 | 新增 `type = delta`，支持 `path` 和 `unity` 两种 adapter。 |
| Delta 格式解析 | 代码/单测 | 使用 Delta Kernel 4.3.1 解析 protocol、log、checkpoint、snapshot。 |
| Path Delta 读取 | 本地 fixture、隔离集群 | 复用 Doris Parquet reader；支持分区裁剪、删除向量和 column mapping 的受限路径。 |
| Unity external/managed 读取 | 本地 fixture | 按表名调用 Unity Delta REST；不绕过 Unity Catalog 猜测对象存储路径。 |
| catalog-managed snapshot | 本地 fixture | 通过 Unity catalog-held commit/log tail 读取，不退化为普通 path 扫描。 |
| 版本/时间点查询 | 本地 fixture、隔离集群 | 支持 `FOR VERSION AS OF` 和 `FOR TIME AS OF`；跨 schema/分区演进当前 fail-closed。 |
| Unity 表发现 | 本地 fixture | 支持 Databricks 官方 `securable_kind_manifest.capabilities`，Tables API 分页使用 `max_results=50`。 |
| 临时凭证 | 本地 fixture | AWS、Azure SAS、GCS OAuth 的初始凭证映射已验证；凭证过期时间会参与 FE 侧检查。 |
| Path Delta 创建 | 本地 fixture、隔离集群 | 支持空表 version 0 创建、直接列名分区和后续 append。 |
| Unity managed 创建 | 本地 fixture | 已验证 staging table、version 0 初始提交和 catalog finalize；真实 Databricks 创建仍未验证。 |
| INSERT | 本地 fixture、隔离集群 | 支持 path 和 Unity external/catalog-managed 的实验性 append。 |
| INSERT OVERWRITE | 本地 fixture、隔离集群 | 通过 Delta remove+add 原子替换 active files，并检查并发版本。 |
| DELETE | 本地 fixture、隔离集群 | copy-on-write；当前支持整表范围和 `WHERE`，不支持分区语法、CTE、子查询、`ORDER BY/LIMIT`。 |
| UPDATE | 本地 fixture、隔离集群 | copy-on-write；支持确定性表达式、别名、`IS NULL`，不支持 `FROM`、CTE、子查询和 `ORDER BY/LIMIT`。 |
| MERGE | 本地 fixture、隔离集群 | 支持 matched DELETE/UPDATE 和 not-matched INSERT；source 当前要求 Doris UNIQUE KEY 表，`ON` 覆盖全部 source key。 |
| TRUNCATE TABLE | 代码/单测 | 当前切片复用空 overwrite transaction 清理全部 active files，只支持整表；完整 FE 构建和最终回归输出待验证。 |
| Unity DROP TABLE | 本地 fixture、代码/单测 | 通过 Unity Delta API 删除 catalog 注册项；path catalog 不删除对象存储目录。 |
| Unity write capability | 本地 fixture、代码/单测 | 只有 `HAS_DIRECT_EXTERNAL_ENGINE_WRITE_SUPPORT` 的 Unity 表才进入 `READ_WRITE` credential/writer 路径。 |
| ABAC 安全边界 | 本地 fixture | 发现 `row_filter`、顶层 `column_masks` 或 `columns[].mask` 时 fail-closed；尚未实现 ABAC 正向执行。 |

## 3. 当前测试证据

| 测试 | 结果 | 说明 |
| --- | --- | --- |
| Delta connector 单测 | 63/63 | 覆盖 snapshot、feature、凭证、创建、写入、DML 和 Unity fixture。 |
| FE catalog TRUNCATE 定向测试 | 5/5 | 覆盖 capability、远端 handle、分区拒绝、缺失远端表和 replay 缓存清理。 |
| FE catalog 其他定向测试 | 已通过 | 覆盖 create/drop、MERGE、overwrite snapshot 和 plugin capability。 |
| Native Delta path 回归 | 已有历史结果 | append、overwrite、DELETE、UPDATE、MERGE、credential 边界均有通过记录。 |
| TRUNCATE 新增回归 | 待验证 | 需要在包含当前 FE 改动的构建上执行回归脚本并由测试框架生成 `.out`；当前没有手写结果文件。 |
| 标准 `./build.sh --fe` | 当前切片尚未通过 | 多次失败原因为宿主机全局 OOM，内核杀掉约 13.7 GB 的 Maven/Javac 进程，不是 Java 编译错误。 |
| 隔离 FE/BE 集群 | 已通过历史验证 | FE/BE 健康，验证查询结果为 45；当前 FE 为释放编译内存已停止，BE 和 metadata 未删除。 |

## 4. 待开发功能

这些项目是代码能力缺口，不应通过当前本地 fixture 的绿色结果推断为已支持。

### 4.1 长查询自动凭证续期

当前行为是：FE 检查 vended credential 是否覆盖 `query_timeout`/`insert_timeout` 加安全余量；覆盖不了就 fail-closed。BE 扫描过程中还没有从 Unity Catalog 重新申请并热更新凭证的完整链路。

需要补齐：

- FE/BE 的凭证刷新协议和生命周期管理；
- AWS 临时密钥、Azure SAS、GCS OAuth 的统一刷新行为；
- 正在读取的文件请求如何切换到新凭证；
- 刷新失败、并发刷新和 token 不写日志的处理。

### 4.2 Unity cross-engine ABAC 正向执行

当前对带 row filter/column mask 的表直接隐藏。后续如果要支持这类表，需要把 Unity 策略转换成 Doris 可执行的过滤/掩码表达式，并验证列权限、表达式语义和错误行为。不能只放开 discovery 而继续使用普通 Parquet 扫描。

### 4.3 Delta protocol 和 table features 扩展

当前只接受明确审核过的 feature。以下类型仍有不同程度限制或拒绝：

- generated/default/constraint columns；
- type widening、variant 和其他物理行变换 feature；
- schema evolution 与历史 schema 不一致的 time travel；
- 更高 reader/writer protocol；
- 新版本 checkpoint、deletion vector 或未来 Delta feature 的兼容性。

### 4.4 DML 语法和语义扩展

当前 DELETE/UPDATE/MERGE 是受限 copy-on-write 切片，不是完整 Delta SQL DML。后续可评估：

- 分区级 DELETE/TRUNCATE；
- 更完整的 MERGE source、CTE、子查询和表达式；
- 更丰富的 UPDATE/DELETE 语法；
- 更完整的 affected-row、schema evolution 和并发冲突语义。

### 4.5 Unity catalog 生命周期和维护操作

当前明确支持/不支持的边界：

- 支持实验性的 Unity DROP TABLE；
- path DROP 不会删除对象存储目录；
- table rename、属性修改、OPTIMIZE、VACUUM、ANALYZE 等外部 Delta client 能力尚未产品化；
- 不应把 Unity Catalog OSS/实验 API 的能力直接当成 Databricks 稳定承诺。

### 4.6 正式产品化

当前仍缺少稳定的配置兼容策略、版本兼容矩阵、升级/回滚说明、监控指标和真实云环境验收，因此不能把实验性 `delta` connector 直接标为完整生产支持。

## 5. 待验证功能

### 5.1 Databricks workspace 验证

至少需要一个启用 Unity Catalog 的真实 workspace，以及一个具有外部访问权限的测试 principal。需要验证：

- `EXTERNAL USE SCHEMA`、`SELECT`、`MODIFY`、`CREATE` 等权限；
- external Delta、managed Delta、catalog-managed Delta；
- 只读 capability 和可写 capability 的差异；
- Unity API 返回的官方 capability manifest、table metadata 和 credential response；
- catalog commit 与并发 writer 冲突；
- CREATE、append、overwrite、DELETE、UPDATE、MERGE、TRUNCATE、DROP 的实际结果。

### 5.2 三云存储验证

分别验证 AWS S3、Azure ADLS Gen2、GCS：

- storage firewall、Private Link/VNet、Doris FE/BE 出口网络；
- temporary credential 的实际格式和有效期；
- Parquet、Delta log、checkpoint、deletion vector 的读取；
- credential 到期前刷新和到期后失败行为。

### 5.3 互操作验证

用 Databricks/Spark 写入或修改表，再用 Doris 读取；再用 Doris 写入后由 Databricks/Spark 读取。重点比较：

- snapshot version 和 active file 集合；
- null、分区值、时间戳、decimal、复杂类型；
- concurrent commit 冲突和重试；
- catalog-managed 表是否始终读取到 Unity Catalog 发布的最新 snapshot。

## 6. 明确不应宣称的能力

在真实 Databricks E2E 通过前，不应对外宣称以下内容：

- “完整支持 Databricks managed Delta”；
- “支持所有 Unity Catalog Delta 表”；
- “支持带 row filter/column mask 的表”；
- “支持长查询自动 credential refresh”；
- “支持完整 Delta SQL DML 和所有 table features”；
- “path Delta 与 Unity Catalog Delta 具有相同治理语义”。

## 7. 关联文档

- [Native Delta 方向性调研](databricks-native-delta-lake-catalog-research.md)
- [Databricks Azure Iceberg 联调环境清单](databricks-azure-iceberg-e2e-setup.md)
- [Databricks Tables API](https://docs.databricks.com/api/workspace/tables/list)
- [Databricks Unity REST 外部 Delta client](https://docs.databricks.com/aws/en/external-access/unity-rest)
- [Databricks credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Delta Kernel](https://docs.delta.io/delta-kernel/)
