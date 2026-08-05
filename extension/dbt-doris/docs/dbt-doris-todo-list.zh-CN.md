# dbt-doris TODO

## 已确定的方案

- 只以 **dbt Core 1.12.x** 为开发和测试基线，Python 使用 3.10+。
- 当前只开发 Python dbt Adapter，不同时开发 Fusion Adapter。
- 覆盖 dbt Core 1.12 官方五种 Model Materialization。
- Incremental 对齐 dbt Core 1.12 的五种内置策略：

| 策略 | Doris 实现 | 阶段 |
| --- | --- | --- |
| `append` | `INSERT INTO` 追加 | P0 |
| `merge` | Unique Key 表 + `INSERT INTO` Upsert | P0 |
| `delete+insert` | 按 `unique_key` 删除后重新插入 | P0 |
| `insert_overwrite` | Doris 原生 `INSERT OVERWRITE` | P0 |
| `microbatch` | 按 `event_time` 拆分时间批次 | P1 |

旧实现曾把 Unique Key `INSERT INTO` 错命名为 `insert_overwrite`。当前实现已经
把该能力归到 `merge`，而 `insert_overwrite` 生成 Doris 原生
`INSERT OVERWRITE`。

这里的 `merge` 指结果语义，不要求 SQL 文本必须是 `MERGE INTO`。首版复用
Doris Unique Key Upsert；Doris 4.1+ 需要条件更新、条件删除或局部列更新时，
再按版本启用原生 `MERGE INTO`。Doris 2.1.x、3.x 和 4.0.x 继续使用
Unique Key `INSERT INTO`，不能生成集群不支持的 `MERGE INTO`。

### Doris 写入术语与 dbt 映射

- `INSERT` 把 Source 行写入目标。Duplicate Key 表会追加；Unique Key 表遇到
  相同 Key 时按表模型规则合并，因此同一条 `INSERT` 可以表现为 Upsert。
- `UPDATE` 只修改已经匹配的目标行，不负责插入新 Key。dbt-doris 当前的
  `merge` 不生成 UPDATE，而是利用 MOW Unique Key 的 `INSERT` 完成全行 Upsert。
- `UPSERT` 是“存在则更新、不存在则插入”的结果语义，不是 Doris 2.1/3.x 的
  独立 SQL 关键字。当前兼容路径是 Unique Key `INSERT INTO`。
- `delete+insert` 也不是 UPDATE：真实路径先按 Key DELETE，再把冻结的同一批
  Source INSERT；MOW Unique Key 上最终结果与全行 Upsert 相同时直接路由到
  `merge`。
- Doris 4.1+ `MERGE INTO` 是一条新的原生 DML，可以表达条件 UPDATE、DELETE 和
  INSERT；普通全行 Upsert 不需要为了使用它而放弃兼容 2.1+ 的路径。

### Incremental 临时关系原则

Incremental 不统一把 Model 结果写入物理 `__dbt_tmp`。Adapter 应根据策略是否
只需要一条最终 DML 选择 Source 形式：

| Source 形式 | 是否物理写入 | 使用场景 |
| --- | --- | --- |
| 内联子查询或 CTE | 否 | Strategy Macro 的直接 SQL 能力；原生 `MERGE INTO` 等不需要命名 Source 的路径 |
| 普通逻辑 View | 否 | 当前单语句生命周期：保持标准 `temp_relation` 契约并取得准确列类型 |
| 物理 Staging Table | 是 | 多语句复用同批结果，或目标 Schema Change 前必须冻结自引用 Model 时 |
| Doris `TEMPORARY TABLE` | 是 | 仅改善会话隔离和失败清理，不解决二次写入；4.x 仍为实验能力 |

Strategy Macro 支持下面的内联单语句形式，供原生 `MERGE INTO` 等不需要命名
Source 的路径复用；当前普通 Incremental 生命周期为了标准五 Key 契约，实际把
同一份 Model SQL 放在不存数据的逻辑 View 中：

```sql
INSERT INTO target (id, value)
SELECT id, value
FROM (
    -- compiled model SQL
) DBT_INTERNAL_SOURCE;
```

目标列必须显式列出并按列名投影，不能依赖 Model SQL 的列位置。需要
`contract.enforced` 时，继续复用现有列类型 CAST 投影。

物理 Staging 只作为正确性工具保留，不能仅因为 dbt Core 通用实现提供了
`temp_relation` 就默认使用。逻辑 View 和 CTE 不会冻结数据，不能替代
`delete+insert` 的物理 Staging。即使使用显式事务，Doris 的
`READ COMMITTED` 也是每条语句分别获取快照，不能保证 DELETE 和 INSERT
重新计算 Model SQL 时得到相同结果。

dbt Core 的通用 Incremental Materialization 默认先建立 `temp_relation`，因为
它必须给不同数据库 Adapter 提供稳定的命名 Source、Schema 比较和多语句复用
契约；这不是所有数据库、所有策略都必须二次物化数据。成熟 Adapter 通常也会按
数据库能力选择临时表、临时 View 或内联 SQL。dbt-doris 的标准五 Key Strategy
Macro Contract 仍接受 `temp_relation`；当前单语句生命周期传入逻辑 View，
既不二次写数据，也允许项目覆盖标准 Strategy Macro。

## P0：升级到 dbt Core 1.12

- [x] 将依赖和测试环境统一到 dbt Core 1.12.x。
- [ ] 更新已变化的 Adapter API 和宏接口。
- [ ] 验证源码安装、wheel 构建、wheel 安装和 `pip check`。
- [ ] CI 运行 Unit Test 和真实 Doris Functional Test。
- [ ] Functional Test 至少覆盖 `dbt debug/seed/run/test/snapshot`。

## P0：覆盖官方 Model Materialization

| Materialization | 当前状态 | 下一步 |
| --- | --- | --- |
| `view` | 已实现 | 验证 dbt 1.12 生命周期、Docs、Grants 和对象类型切换 |
| `table` | 已实现 | 完善安全替换、Contracts 和 Doris Table 配置 |
| `incremental` | P0 已实现 | `microbatch` 和 4.1 原生 `MERGE INTO` 留在 P1 |
| `ephemeral` | dbt Core 提供 | 验证 CTE 编译和 `ref()`，不新增 Doris DDL |
| `materialized_view` | 未实现 | 实现 Doris Async Materialized View，见后文 |

- [ ] 测试 `view`、`table`、`incremental` 和 `materialized_view` 之间的
  Relation 类型切换。
- [x] 将现有自定义 `partition` Materialization 的能力并入
  `incremental_strategy='insert_overwrite'`，保留兼容迁移说明。

Snapshot、Seed 和 Data Test 是独立 dbt Resource，不属于 Model
Materialization；其 Doris 兼容工作放在 P1。

## P0：完善 Incremental 基础策略

- [x] 接入 dbt 1.12 标准 Incremental Strategy Dispatch 和对应策略宏。
- [x] 未支持的策略或配置在执行 SQL 前明确报错。
- [x] 完善策略与 Doris 表模型的映射：
  - `append` 使用 Duplicate Key；
  - `merge` 使用 Merge-on-Write Unique Key；
  - `delete+insert` 和 `insert_overwrite` 校验目标表模型是否兼容。
- [x] Strategy Macro 同时支持内联 Model SQL 和命名 Source Relation，不再
  强制所有策略接收物理 `temp_relation`。
- [x] Functional Test 检查生成 SQL 和 Doris Catalog：单语句策略不得创建
  物理 `__dbt_tmp`，多语句策略必须正确创建、复用和清理 Staging。
- [x] 首次建表、普通增量和 Full Refresh 共用 Duplicate/Unique Key DDL，
  并保留 Key、Partition、Distribution 和 Properties。

### `append`

- [x] 改为一条 `INSERT INTO ... SELECT`，Source 使用逻辑 View，不创建物理
  `__dbt_tmp`。
- [x] 显式生成目标列列表，并从 Model 子查询按列名投影。
- [x] 测试首次创建、重复运行和 Full Refresh。

### `merge`

- [x] 将当前错误命名的 `insert_overwrite` 路径改为 `merge`。
- [x] 要求配置 `unique_key`，支持单列和复合 Key。
- [x] 首次运行创建 Merge-on-Write Unique Key 目标表。
- [x] 后续运行使用一条 `INSERT INTO ... SELECT`，利用 Unique Key
  完成全行 Upsert，不创建物理 `__dbt_tmp`。
- [x] 校验现有目标表的 Key 类型和 Key 列；不兼容时提示
  `--full-refresh`。
- [x] 校验 Model 输出包含全部 Unique Key；全行 Upsert 必须按目标列写入，
  避免未提供列被默认值或 `NULL` 覆盖。
- [x] 同批次出现重复 `unique_key` 时明确报错；当前即使使用 Sequence Column
  也要求上游先确定性去重。
- [x] 测试更新已有行、插入新行、保留本批未出现的旧行。

### `delete+insert`

- [x] 要求配置 `unique_key`，支持单列和复合 Key。
- [x] 使用物理 Staging Table 冻结本批结果，先删除目标表中的匹配行，再从
  同一个 Staging 插入本批结果；普通 View、CTE 或重复 Model SQL 不可作为
  等价实现。
- [x] 明确事务边界和失败恢复；不能假设 `BEGIN/COMMIT` 会让两条语句读取
  同一个源快照。
- [x] 如果目标已限定为无 Sequence Column 的 MOW Unique Key、只要求按 Key
  全行替换且没有特殊 Delete Predicate，则路由到 `merge` 的单条 Upsert；
  Sequence Column 因 DELETE Tombstone 的顺序语义前置拒绝，并提示改用
  `merge`。
- [x] 测试已有 Key 替换、新 Key 插入、未匹配旧行保留和失败恢复。

真实两语句路径要求 Doris 3.0+ 和支持显式事务的部署模式；当前固定 Staging
Relation 不支持同一 Model 并发执行，异常遗留会由下次运行启动时清理。

### `insert_overwrite`

- [x] 不再要求 `unique_key`。
- [x] 首先实现整表覆盖：

```sql
INSERT OVERWRITE TABLE target
SELECT ...;
```

- [x] 再实现指定分区覆盖：

```sql
INSERT OVERWRITE TABLE target PARTITION (p1, p2)
SELECT ...;
```

- [x] 支持 Doris 2.1.3+ 的自动分区识别：

```sql
INSERT OVERWRITE TABLE target PARTITION(*)
SELECT ...;
```

- [x] Model SQL 通过逻辑 View 接入 `INSERT OVERWRITE`，不创建 dbt 物理
  `__dbt_tmp`。Doris 内部“写临时分区后原子替换”属于一次数据写入和元数据
  发布，不等同于 dbt 先 CTAS、再重新写目标表。
- [x] 测试覆盖范围内旧数据被删除、非覆盖分区保持不变。
- [ ] 测试空批次语义：`PARTITION(*)` 无法自动识别并清空完全没有输出数据的
  分区，需要清空时必须显式配置分区。
- [ ] 明确失败清理和重试行为。

### 兼容和迁移

- [x] 旧项目若想按 Key Upsert，将
  `incremental_strategy='insert_overwrite'` 改为 `merge`。
- [x] 旧项目若想覆盖整表或分区，继续使用 `insert_overwrite`。
- [x] 在 Release Note 中明确这是行为修正：新的 `insert_overwrite` 会删除
  覆盖范围内未出现在本批的数据。

## P0：实现 Materialized View

实现独立的 `materialized='materialized_view'`，对应 Doris Async Materialized
View，不作为 Incremental Strategy。

- [ ] 根据 Model SQL 生成 `CREATE MATERIALIZED VIEW ... AS ...`。
- [ ] 支持 `BUILD IMMEDIATE/DEFERRED`。
- [ ] 支持 `REFRESH AUTO/COMPLETE` 和
  `ON MANUAL/SCHEDULE/COMMIT`。
- [ ] 支持刷新周期、`PARTITION BY`、Distribution、Buckets 和 Properties。
- [ ] 正确识别、删除和重建 Materialized View Relation。
- [ ] 重复执行 `dbt run` 时保持幂等；配置变化时明确更新或重建。
- [ ] 支持手动 Refresh，并返回 Doris Refresh Task 状态。
- [ ] Functional Test 覆盖创建、查询、刷新、配置变化和删除。

## P1：完善 Incremental 高级能力

- [ ] `microbatch`：支持 `event_time`、`begin`、`batch_size`、`lookback`
  和并行批次；每批复用 `append`、Unique Key Upsert 或
  `insert_overwrite` 的单语句路径，只有底层选择 `delete+insert` 时才使用
  物理 Staging。
- [x] `on_schema_change`：支持 `ignore`、`fail`、
  `append_new_columns`、`sync_all_columns`。
- [x] `on_schema_change='ignore'` 使用无物理 Staging 的逻辑 View；`fail`、
  `append_new_columns` 和 `sync_all_columns` 使用物理冻结批次，保证读取
  `{{ this }}` 的 Model SQL 不会在目标 DROP/ADD 后被重新解释。
- [x] 完善 Metadata 类型映射，保留 VARCHAR 长度、DECIMAL 精度和 Scale，
  避免取消 CTAS 后丢失类型扩展能力。
- [ ] Merge 配置：支持 `merge_update_columns`、
  `merge_exclude_columns` 和 `incremental_predicates`；无法支持的组合明确报错。

### Doris 4.1+ 原生 `MERGE INTO`

原生 `MERGE INTO` 是 Doris 4.1.0 新增的数据库 SQL 能力，不是 dbt 版本，
也不是 `DELETE + INSERT` 宏的别名。它允许在一个 Statement 中把 Model SQL
作为 Source，并根据匹配结果执行 UPDATE、INSERT 或 DELETE：

```sql
MERGE INTO target t
USING (
    -- compiled model SQL
) s
ON t.id = s.id
WHEN MATCHED THEN
    UPDATE SET value = s.value
WHEN NOT MATCHED THEN
    INSERT (id, value) VALUES (s.id, s.value);
```

- [ ] 运行时读取 Doris FE 版本；仅 4.1+ 注册或启用原生
  `MERGE INTO` 路径，2.1.x、3.x 和 4.0.x 在执行 SQL 前给出明确的版本错误或
  使用受支持的 Unique Key Upsert 路径。
- [ ] 目标表必须是 Merge-on-Write Unique Key；校验 dbt `unique_key` 与
  物理 Key 一致，禁止更新 Key 列。
- [ ] 使用内联子查询或 CTE 作为 `USING` Source，整条操作只执行一次 Model
  SQL，不创建物理 `__dbt_tmp`。
- [ ] 普通全行 Upsert 仍默认使用兼容 2.1+ 的 Unique Key
  `INSERT INTO`；只有条件 UPDATE、条件 DELETE、局部列更新等高级语义才选择
  原生 `MERGE INTO`。
- [ ] 校验 Source 每个 Key 最多匹配一个目标行；重复匹配必须报错或通过用户
  明确配置的确定性规则去重。
- [ ] 测试 matched update、matched delete、not-matched insert、复合 Key、
  NULL-safe Key 比较、重复 Source Key、版本门禁和失败原子性。

参考：

- [Doris Unique Key 全行 Upsert](https://doris.apache.org/docs/4.x/data-operate/update/update-of-unique-model/)
- [Doris INSERT OVERWRITE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/DML/INSERT-OVERWRITE/)
- [Doris 4.1 MERGE INTO](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/DML/MERGE-INTO/)
- [Doris 事务与 READ COMMITTED](https://doris.apache.org/docs/4.x/data-operate/transaction/)
- [Doris Temporary Table（实验）](https://doris.apache.org/docs/4.x/table-design/temporary-table/)

## P1：补齐 dbt 通用能力

| 能力 | 大概功能 | 用户入口 |
| --- | --- | --- |
| Snapshot | 用 Check/Timestamp Strategy 保存数据历史版本，保证失败时旧历史仍可用 | `dbt snapshot` |
| Contracts | 建表前校验 Model 输出的列名和类型是否符合 YAML 声明 | `contract.enforced: true` |
| Persist Docs | 把 Model 和 Column Description 写入 Doris Comment | `persist_docs` |
| Source Freshness | 按源表最近加载时间产生 Pass、Warn 或 Error | `dbt source freshness` |
| Store Failures | 把 Data Test 失败的具体数据行保存到审计表 | `dbt test --store-failures` |
| Grants | 按 Model Config 授权并回收 Relation 的过期权限 | `grants:` |

Snapshot 的 S1-S6 已完成，使用方式、失败语义和验证范围见
[`foundation/snapshot.zh-CN.md`](foundation/snapshot.zh-CN.md)。其中 Grants SQL 仍按本表的
独立 Grants 能力跟踪。

- [ ] 接入适用的 `dbt-tests-adapter` 官方测试，作为上述能力的兼容性验收。

## P2：完善 Doris Table 原生能力

- [ ] 在 P0 已有的 Duplicate/Unique Key 支持上，抽取供 Table、Incremental
  和 Full Refresh 共用的配置与 DDL 层。
- [ ] 新增 Aggregate Key、聚合函数配置及测试；只开放能够保证正确结果的
  Incremental 策略，不默认套用 `append` 或 `merge`。
- [ ] RANGE/LIST/Auto/Dynamic Partition。
- [ ] HASH/RANDOM Distribution 和 `BUCKETS AUTO`。
- [ ] Inverted、Bloom Filter、Bitmap 等索引。

## P3：生产能力

- [ ] SSL、Timeout、Retry 和多 FE Failover。
- [ ] Query ID、Invocation ID、影响行数和执行耗时。
- [ ] Doris 服务端 Query Cancel。
- [ ] External Catalog 元数据支持和性能优化。
- [ ] 自动构建、测试和发布 wheel。
