# dbt-doris：Async Materialized View 与 Incremental 对比及选型

## 文档信息

| 项目 | 内容 |
| --- | --- |
| 文档目的 | 解释 dbt Incremental 与 Doris Async Materialized View 的区别、优缺点和选型边界 |
| 调研范围 | dbt Core、Apache Doris、StarRocks、BigQuery、Databricks、Snowflake、ClickHouse |
| 本地实现基线 | `extension/dbt-doris`，检查日期 2026-07-28 |
| 说明 | “目标能力”不代表 dbt-doris 当前已经支持，当前状态以第 9 节为准 |

## 0. 一句话结论

**Incremental 是 dbt 定期运行 Model SQL，把本批结果写入一张普通表；Async
Materialized View 是 dbt 部署视图定义和刷新配置，后续由 Doris 判断何时刷新、
刷新哪些分区，并可用结果做查询加速。**

二者都会避免每次重算全部历史数据，但不是同一种能力：

- Incremental 的增量边界和写入规则由 Model 作者与 dbt-doris 控制；
- Async MV 的失效判断、刷新任务和物化结果由 Doris 控制；
- Incremental 更适合构建可控的业务明细表和汇总表；
- Async MV 更适合对稳定查询模式做预计算和透明查询改写；
- 两者不是互斥关系。常见做法是先用 Incremental 维护标准业务表，再在其上建立
  Async MV 加速报表查询。

## 1. 先把三个容易混淆的“增量”分开

| 名称 | 它是什么 | 谁决定本次处理什么数据 |
| --- | --- | --- |
| dbt Incremental Model | dbt 的一种 Materialization，最终对象是普通 Doris Table | Model 中的 `is_incremental()` 条件和 `incremental_strategy` |
| Doris Async MV 的增量刷新 | 分区 Async MV 使用 `PARTITION BY` 和 `REFRESH AUTO` | Doris 根据底表分区版本判断哪些 MV 分区失效 |
| Doris Sync MV | 写入底表时同步维护的索引型物化结构 | Doris 在底表写入过程中同步维护 |

因此，Doris 文档中的 **Incremental Materialized View** 不是
`materialized='incremental'`。它指的是能够按分区增量刷新的 **Async
Materialized View**。

## 2. 用同一个订单场景看执行差异

假设有一张按天分区的订单明细表 `fact_orders`：

| 字段 | 含义 |
| --- | --- |
| `order_id` | 订单 ID |
| `order_time` | 下单时间，也是分区依据 |
| `category_id` | 商品类别 |
| `pay_amount` | 实付金额 |
| `update_time` | 订单最后更新时间 |

目标是得到“每天、每个类别的销售额”。

### 2.1 用 dbt Incremental 实现

下面表达的是当前 dbt-doris 可使用的 Unique Key Upsert 路径。注意：当前策略名
仍叫 `insert_overwrite`，但实际行为是按 Unique Key 更新或插入，并不是真正的
分区覆盖。

```sql
-- models/daily_category_sales.sql

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        unique_key=['order_day', 'category_id']
    )
}}

select
    date(order_time) as order_day,
    category_id,
    sum(pay_amount) as sales_amount,
    max(update_time) as last_update_time
from {{ ref('fact_orders') }}

{% if is_incremental() %}
where update_time >= (
    select date_sub(max(last_update_time), interval 2 day)
    from {{ this }}
)
{% endif %}

group by date(order_time), category_id
```

执行过程是：

1. 外部调度器触发 `dbt run`；
2. `is_incremental()` 为真，Model 只查询最近变化的数据；
3. dbt-doris 把本批查询结果写入临时表；
4. dbt-doris 按目标策略把临时表结果写入普通 Doris Table；
5. 下一次 `dbt run` 之前，这张表不会自行更新。

这里“回看 2 天”是 Model 作者制定的迟到数据策略。窗口太小会漏数，窗口太大又会
增加扫描和重算成本。

### 2.2 用 Doris Async Materialized View 实现

下面是 Doris 原生 DDL，不是伪代码。运行前要求 `fact_orders` 已存在、其
`order_time` 可用于合法的分区映射，并且 Doris 版本支持对应语法。

```sql
CREATE MATERIALIZED VIEW daily_category_sales_mv
BUILD IMMEDIATE
REFRESH AUTO
ON SCHEDULE EVERY 1 HOUR
PARTITION BY (DATE_TRUNC(order_time, 'DAY'))
DISTRIBUTED BY RANDOM BUCKETS 2
AS
SELECT
    DATE_TRUNC(order_time, 'DAY') AS order_day,
    category_id,
    SUM(pay_amount) AS sales_amount
FROM fact_orders
GROUP BY
    DATE_TRUNC(order_time, 'DAY'),
    category_id;
```

执行过程是：

1. dbt 运行一次，把 Model 编译成 `CREATE MATERIALIZED VIEW`，相当于部署定义；
2. Doris 创建物化结果和刷新任务；
3. 某一天的订单分区发生变化后，Doris 把对应的 MV 分区标记为失效；
4. 定时任务到点后，Doris 对失效的 MV 分区执行 `INSERT OVERWRITE` 重算；
5. 后续无需每小时执行 dbt；只有 Model SQL 或 Config 改变时才需要再次运行 dbt。

Async MV 的“增量”不是简单地把新订单金额加到旧结果上。它通常是：

```text
底表某个分区变化
  -> 对应 MV 分区失效
  -> 重新查询该底表分区
  -> 完整覆盖对应 MV 分区
```

因此，同一天的旧订单被修改或删除时，只要 Doris 能感知底表分区变化，重算后的
当日汇总仍然可以得到正确结果，不要求查询满足“新数据完全不依赖旧数据”。

### 2.3 同一批迟到订单进入系统时

假设 7 月 28 日收到一条属于 7 月 26 日的迟到订单：

| 阶段 | Incremental | Async MV |
| --- | --- | --- |
| 如何发现 | Model 的时间条件必须覆盖到 7 月 26 日 | Doris 检测到 7 月 26 日底表分区版本变化 |
| 重算范围 | 取决于 Model 的回看窗口和策略 | 通常重算映射到 7 月 26 日的 MV 分区 |
| 谁触发 | dbt 或外部调度器 | Doris 的 Schedule、Commit 或手动刷新任务 |
| 谁承担正确性 | Model 作者和 Adapter | Doris 的分区映射与刷新机制 |
| 何时可见 | 本次 `dbt run` 写完后 | 对应刷新任务完成后 |

如果 Incremental 只回看 1 天，这条数据可能永远不会进入结果；Async MV 不依赖
Model 作者手写回看窗口，但前提是底表分区变化能被 Doris 正确识别。

## 3. 核心差异

| 对比项 | dbt Incremental | Doris Async Materialized View |
| --- | --- | --- |
| 最终对象 | 普通 Doris Table | Doris MTMV/Async MV |
| 主要目的 | 构建和维护数据模型 | 预计算、查询加速，也可用于轻量建模 |
| 数据刷新负责人 | dbt-doris | Doris |
| 刷新触发 | `dbt run` 或外部调度器 | `ON MANUAL`、`ON SCHEDULE`、`ON COMMIT` |
| 增量边界 | Model 中的 `is_incremental()`、`event_time`、条件 SQL | Doris 的底表分区版本和 MV 分区映射 |
| 写入方式 | Append、Upsert、Overwrite、Delete+Insert 等策略 | Full Refresh 或对失效分区执行 `INSERT OVERWRITE` |
| 更新粒度 | 行、Key、条件范围或分区，取决于策略 | 整个 MV 或映射后的 MV 分区 |
| 删除处理 | Model 和策略必须显式覆盖删除语义 | 底表变化使相关分区失效，刷新时重算 |
| 一致性 | 取决于 dbt 调度频率 | 最终一致，存在刷新延迟 |
| 直接查询 | 可以，目标就是普通表 | 可以直接查询 Async MV |
| 透明查询改写 | 没有 | Doris 可把底表查询改写到 MV |
| SQL 自由度 | 主要受 Doris 普通查询和写入策略限制 | 受 Async MV 创建、分区映射和查询改写规则限制 |
| Schema 变化 | Adapter 通过 `on_schema_change` 或 Full Refresh 处理 | 由对象 DDL、配置变更和 Doris 能否 `ALTER` 决定 |
| 失败恢复 | dbt Job、临时表和策略负责 | Doris Refresh Task 负责；部分分区可能已完成 |
| 运维观察入口 | dbt 日志、调度器、目标表 | dbt 部署日志 + Doris MV/Task 状态 |

最本质的区别不是“一个全量、一个增量”，而是 **控制权在哪里**：

```text
Incremental:
dbt 决定运行时机 -> Model 决定本批数据 -> Adapter 决定如何写表

Async MV:
dbt 部署定义 -> Doris 观察底表变化 -> Doris 决定刷新范围并维护结果
```

## 4. Incremental 的优缺点

### 4.1 优点

1. **业务规则可控**

   Model 可以明确处理业务 Key、迟到数据、软删除、回看窗口、全量修复和特殊条件，
   不必受 MV 分区推导能力限制。

2. **适合行级更新**

   当“订单 ID 相同就更新原记录”或“本批 Key 对应旧数据要删除”时，Upsert、
   Merge 或 Delete+Insert 的语义更直接。

3. **结果是一张普通表**

   下游工具、权限、导出和其他数据产品可以把它当普通 Doris Table 使用，不依赖
   查询改写是否命中。

4. **复杂转换更自由**

   只要 Doris 能执行 Model SQL，并且写入策略能保证正确性，就可以使用复杂
   Join、窗口计算和自定义逻辑。

5. **调度和数据发布边界清楚**

   一次 dbt Job 可以串联多个 Model、Test 和下游依赖，成功后再统一发布数据。

### 4.2 缺点

1. **增量正确性由开发者承担**

   `is_incremental()` 过滤条件写错、回看窗口太短或 `unique_key` 不正确，都可能
   静默产生漏数或重复数据。

2. **必须持续运行 dbt**

   Model 不会自行更新，需要 dbt Cloud、Airflow、Cron 等持续触发。

3. **策略实现复杂**

   Adapter 需要正确实现 Append、Upsert、Overwrite、Schema Change、失败回滚、
   Full Refresh 和临时对象清理。

4. **目标表扫描可能昂贵**

   Upsert 或 Merge 可能同时扫描本批数据和已有目标表；分区设计不合理时成本较高。

5. **逻辑改变时可能需要 Full Refresh**

   如果历史计算公式改变，只更新新数据无法修正旧结果，通常要重建整表或显式回刷
   历史分区。

## 5. Async Materialized View 的优缺点

### 5.1 优点

1. **刷新由 Doris 管理**

   dbt 主要负责部署 SQL 和 Config，不需要为了数据刷新频繁运行同一个 Model。

2. **能够自动识别变化分区**

   合法的分区映射下，Doris 按底表分区版本标记失效，只覆盖需要重算的 MV 分区，
   不需要用户手写 `max(update_time)`。

3. **支持透明查询改写**

   用户仍然查询底表或原始 SQL，优化器可以自动使用物化结果。一个 MV 可以加速
   多个能够匹配的查询，而不只是一个显式依赖它的下游 Model。

4. **适合重复的复杂计算**

   多表 Join、大聚合和湖仓外表查询可以预计算并存到 Doris，减少重复扫描和远端
   IO。

5. **刷新与查询能力属于数据库**

   刷新调度、分区状态、资源组和查询改写能够在 Doris 内统一管理。

### 5.2 缺点

1. **只能做到最终一致**

   底表已经更新但刷新任务尚未完成时，直接查询 MV 可能看到旧数据。业务必须接受
   这个延迟。

2. **增量刷新有结构限制**

   MV 分区必须能从某个底表分区推导。不能建立有效映射时，可能只能 Full Refresh，
   或在创建阶段直接被拒绝。

3. **不适合任意行级写入规则**

   Async MV 没有让用户自由选择“按订单 ID Upsert”或“只删除某些业务 Key”的
   Incremental Strategy；其核心写入单位是物化结果或分区。

4. **SQL 和查询改写能力有边界**

   MV 能创建不等于任意查询都能透明命中；查询形态、函数、Join 关系和一致性设置
   都会影响改写。

5. **运维链路更长**

   需要同时观察底表变化、MV 分区状态、刷新 Task、资源消耗和改写命中情况。

6. **外表变化感知受 Catalog 能力影响**

   不同外表格式对自动发现数据变化和分区刷新支持不同，不能默认与 Doris 内表
   完全相同。

## 6. 应该选哪个

| 场景 | 建议 | 原因 |
| --- | --- | --- |
| 维护订单、客户等标准业务明细表 | Incremental | 需要业务 Key、更新和删除语义 |
| 构建下游必须直接依赖的 DWD/DWS 表 | Incremental | 普通表的发布和依赖边界更明确 |
| 每小时更新一张复杂的业务宽表 | 优先 Incremental | Model 可精确控制数据修正和发布 |
| 加速固定报表聚合或重复 Join | Async MV | Doris 可预计算并透明改写多个查询 |
| 按天分区的事实表，只改少量近期分区 | Async MV 很合适 | Doris 可自动识别并覆盖变化分区 |
| 需要按主键修正任意历史行 | Incremental | Upsert/Delete+Insert 更直接 |
| SQL 含跨表复杂计算，但允许分钟级延迟 | Async MV | 可用异步刷新换取查询性能 |
| 必须强一致、底表写完立即可见 | Async MV 不合适 | 应评估 Sync MV 或直接查询底表 |
| 无分区底表且数据量很大 | 谨慎使用 Async MV | 可能退化为 Full Refresh |
| 需要一次 Job 完成构建、测试和数据发布 | Incremental | dbt 调度边界更清楚 |

### 6.1 推荐的组合方式

实际项目中更常见的是分层组合：

```text
ODS 订单源表
  |
  | dbt Incremental：去重、修正、统一业务口径
  v
fct_orders 标准事实表
  |
  | Doris Async MV：按天和类别预聚合
  v
报表查询加速
```

这里 Incremental 解决“数据模型怎样正确维护”，Async MV 解决“重复查询怎样更快”。
职责清楚，也避免把所有业务正确性寄托在查询加速对象上。

## 7. Sync MV 在这个选择中的位置

Sync MV 与 Async MV 都由 Doris 维护，但目标不同：

| 对比项 | Sync MV | Async MV |
| --- | --- | --- |
| 一致性 | 与底表写入强一致 | 异步刷新，最终一致 |
| 刷新方式 | 底表写入时同步维护 | Manual、Schedule 或 Commit 触发任务 |
| SQL 范围 | 主要是单表过滤、排序、表达式和聚合 | 支持更复杂的 Join 和聚合 |
| 是否可直接查询 | 不可作为普通表直接查询，依靠改写 | 可以直接查询，也可透明改写 |
| 写入影响 | 每次底表写入都要同步维护，MV 多时会拖慢导入 | 刷新与底表写入解耦 |
| 典型用途 | 实时单表聚合、前缀索引、预过滤 | 跨表预计算、周期报表、湖仓加速 |

简单判断：

- 要求底表写完后结果立即一致，并且是单表加速：看 Sync MV；
- 能接受一定延迟，需要复杂 Join、分区刷新或直接查询：看 Async MV；
- 需要业务 Key、迟到数据和发布流程由 dbt 明确控制：看 Incremental。

本需求中的 `materialized='materialized_view'` 应先明确支持 **Async MV**。Sync MV
的对象归属、创建语法和生命周期不同，不应悄悄复用同一套默认行为。

## 8. 其他 Adapter 怎么实现

### 8.1 总览

| Adapter / 产品 | 普通增量表 | 数据库维护的物化对象 | 对 dbt-doris 的启示 |
| --- | --- | --- | --- |
| dbt-starrocks | `incremental`，支持 Append、Insert Overwrite、Dynamic Overwrite 等 | 独立的 `materialized_view` | 与 Doris 最接近，应保持两类 Materialization 独立 |
| dbt-bigquery | `incremental`，支持 Merge、Insert Overwrite、Microbatch | 独立的 `materialized_view`，配置自动刷新 | dbt 负责部署和配置变化，平台负责刷新 |
| dbt-databricks | `incremental`，支持多种 Delta/Hudi 写入策略 | `materialized_view` 和 `streaming_table` | 数据库托管对象不应被塞进 Incremental Strategy |
| dbt-snowflake | `incremental`，支持 Merge、Append、Delete+Insert 等 | 使用 `dynamic_table`，而非原生 MV Materialization | 当平台对象语义特殊时，应使用清晰的专用名称和 Config |
| dbt-clickhouse | 多种 Incremental Strategy | `materialized_view` 是 Insert Trigger 语义 | 同名对象在不同数据库中语义不同，Adapter 必须明确说明 |

### 8.2 StarRocks：最接近 Doris 的参考

dbt-starrocks 同时提供：

- `materialized='incremental'`；
- `materialized='materialized_view'`；
- Incremental 的 Append、Insert Overwrite、Dynamic Overwrite 和 Microbatch；
- MV 的 Partition、Distribution、Bucket、Properties 和 Refresh 配置。

它没有把异步 MV 做成 `incremental_strategy='materialized_view'`。这说明即使两者都
能“只处理变化数据”，生命周期仍然不同：

- Incremental 每次运行都执行数据写入；
- Materialized View 的 dbt 运行主要负责创建和配置对象；
- 物化视图后续刷新由数据库任务完成。

对 dbt-doris 最直接的启示是：实现两个独立入口，共享底层的标识符、Properties
和 Relation 工具，但不要共享同一套数据写入生命周期。

### 8.3 BigQuery：把自动刷新暴露为 Materialized View Config

dbt-bigquery 的 Incremental 支持：

- `merge`；
- `insert_overwrite`；
- `microbatch`。

它还独立支持 `materialized_view`，并把 `enable_refresh`、
`refresh_interval_minutes` 和 `max_staleness` 等平台能力暴露为 Config。
dbt 监控 Config 变化，能 `ALTER` 的配置就原地修改，SQL 定义等不能修改的变化则
需要重建对象。

启示：

1. Refresh Config 是 Materialized View 的一等配置，不是 Hook 字符串；
2. dbt 应区分“本次部署是否成功”和“后台数据是否已经刷新”；
3. `on_configuration_change` 应优先修改可变配置，不能修改时才 Drop/Create。

### 8.4 Databricks：Incremental Table、MV 和 Streaming Table 三条路径

dbt-databricks 的 Incremental 可以按存储格式选择 Append、Insert Overwrite、
Merge、Replace Where、Delete+Insert 和 Microbatch。另一方面，它把平台托管的
Materialized View 和 Streaming Table 做成独立 Materialization，并支持 Schedule、
Partition、Cluster、Table Properties 等配置。

启示：

- 写表策略解决“这一批怎样合并进普通表”；
- MV/Streaming Table 解决“平台怎样持续维护派生对象”；
- 即使能力有重叠，也应按对象类型和控制权拆开，而不是追求一个万能 Incremental。

### 8.5 Snowflake：语义不同就使用 `dynamic_table`

dbt-snowflake 的普通 Incremental 支持 Merge、Append、Delete+Insert、
Insert Overwrite 和 Microbatch。对于由 Snowflake 自动维护的派生结果，Adapter
使用：

```sql
{{ config(
    materialized='dynamic_table',
    snowflake_warehouse='COMPUTE_WH',
    target_lag='1 minute'
) }}
```

`target_lag` 表达的是平台负责维持的目标延迟，而不是 dbt 每分钟重跑一次 Model。

启示：dbt Materialization 名称要反映数据库对象的真实语义。Doris 已经有明确的
Async MV 对象，因此使用 `materialized_view` 合理；但 Config 仍应直接表达 Doris
的 Build、Refresh Method 和 Trigger，而不是照搬 Snowflake 的 `target_lag`。

### 8.6 ClickHouse：同名 Materialized View，刷新模型不同

dbt-clickhouse 的 Incremental 支持 Append、Delete+Insert、Insert Overwrite、
Microbatch 等策略。它的 `materialized_view` 则对应 ClickHouse 的 Insert Trigger：
源表插入新行时，视图查询只处理新插入的数据，并把结果写入目标表。

这与 Doris Async MV 的“创建刷新任务、检测分区失效、重算物化分区”不同。

启示：dbt 的 Materialization 负责把统一概念映射到数据库真实能力，但不能因为
都叫 `materialized_view` 就宣称各平台拥有相同的一致性、刷新和删除处理语义。

## 9. dbt-doris 当前实现到哪里

### 9.1 Incremental 当前已实现

本地代码
[`incremental.sql`](../dbt/include/doris/macros/materializations/incremental/incremental.sql)
当前具备：

- `append`；
- 名为 `insert_overwrite`、实际依赖 Doris Unique Key 的 Upsert；
- `is_incremental()` 条件编译；
- `--full-refresh`；
- 临时表、Hook 和基本校验。

尚未完整实现：

- 真正的整表或分区 `insert_overwrite`；
- 清晰命名的 `unique_key_upsert`；
- `delete+insert`；
- 完整 `on_schema_change`；
- 标准 Incremental Strategy Dispatch 接口；
- Microbatch 和 Dynamic Overwrite 等高级能力。

### 9.2 Async Materialized View 当前未实现

当前 dbt-doris 没有 `materialized_view` Materialization，不能把 dbt Model 原生
编译成 Doris Async MV DDL。对应需求见：

[`dbt-doris-issue-65967-async-materialized-view-requirements.zh-CN.md`](dbt-doris-issue-65967-async-materialized-view-requirements.zh-CN.md)。

需要补充的不是一条孤立的 `CREATE MATERIALIZED VIEW`，还包括：

- Config 到 Doris DDL 的映射；
- 从 Doris 元数据正确识别 Async MV；
- 首次创建、重复运行和 `--full-refresh`；
- SQL/Config 变化时 Alter 或重建；
- Drop、Rename、Relation Cache 与返回结果；
- 刷新任务触发、状态观察和错误处理边界。

## 10. 对 dbt-doris 的设计建议

### 10.1 保持两个独立 Materialization

建议明确提供：

```text
materialized='incremental'
  -> 创建普通 Doris Table
  -> 每次 dbt run 计算本批数据并写入

materialized='materialized_view'
  -> 创建 Doris Async MV
  -> dbt run 主要部署定义和配置
  -> 后续数据刷新由 Doris 负责
```

不建议增加：

```text
materialized='incremental'
incremental_strategy='async_materialized_view'
```

因为 Async MV 不是一种“把本批数据写入普通表”的策略。这样设计会让
`is_incremental()`、`--full-refresh`、Hook、Schema Change 和运行结果的语义全部
变得含混。

### 10.2 分别补齐各自的基础能力

Incremental 优先补：

- Append；
- Unique Key Upsert；
- 真正的 Insert Overwrite；
- Delete+Insert；
- `on_schema_change`；
- Full Refresh 和失败安全。

Async MV 优先补：

- Manual 与 Schedule；
- Complete 与 Auto；
- Partition、Distribution、Bucket 和 Properties；
- Config Change；
- 正确的 Relation 识别和 Drop/Create 生命周期；
- 刷新状态的最小可观测能力。

Dynamic Overwrite、Microbatch、外表变化感知增强和高级查询改写配置可以在基础
生命周期稳定后继续建设。

### 10.3 不让 dbt 重复实现 Doris 的刷新算法

dbt-doris 应负责：

```text
识别 Config
  -> 校验组合
  -> 生成正确 Doris DDL
  -> 管理对象生命周期
```

Doris 应继续负责：

```text
检测底表变化
  -> 标记失效分区
  -> 调度刷新
  -> 执行分区或全量重算
  -> 决定查询改写
```

Adapter 不应另外维护一套“上次刷新到哪个分区”的状态来替代 Doris，这会造成两套
状态源和更复杂的恢复问题。

## 11. 最终选型口诀

```text
要维护一张业务表，选 Incremental；
要让 Doris 自动维护预计算结果，选 Async MV；
要底表写入后立即强一致，而且只是单表加速，评估 Sync MV；
要既保证业务模型正确又加速查询，Incremental + Async MV 分层组合。
```

## 12. 官方资料

### dbt

- [dbt Materializations](https://docs.getdbt.com/docs/build/materializations)
- [dbt Incremental Models](https://docs.getdbt.com/docs/build/incremental-models)
- [dbt Incremental Strategies](https://docs.getdbt.com/docs/build/incremental-strategy)

### Apache Doris

- [Incremental Materialized View](https://doris.apache.org/docs/4.x/key-features/incremental-materialized-view/)
- [Async Materialized View Overview](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/async-materialized-view/overview/)
- [Manage and Query Async Materialized Views](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/async-materialized-view/functions-and-demands/)
- [Sync Materialized View](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/sync-materialized-view/)

### 其他 Adapter

- [dbt-starrocks](https://github.com/StarRocks/dbt-starrocks)
- [dbt-bigquery Configurations](https://docs.getdbt.com/reference/resource-configs/bigquery-configs)
- [dbt-databricks Configurations](https://docs.getdbt.com/reference/resource-configs/databricks-configs)
- [dbt-snowflake Configurations](https://docs.getdbt.com/reference/resource-configs/snowflake-configs)
- [dbt-clickhouse Materializations](https://clickhouse.com/docs/integrations/connectors/data-ingestion/etl-tools/dbt/materializations)
