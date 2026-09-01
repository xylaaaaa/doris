# data-eng-bench 的 5 个 dbt-for-apache-doris Demo

> 数据源：[Snowflake-Labs/data-eng-bench](https://github.com/Snowflake-Labs/data-eng-bench)，固定使用 commit `53353547b9869d35d61b40fd6ee9397a7ac8ca80`。
> 本文只讨论 5 个可用于 dbt-for-apache-doris 发布的 Demo。Agent 是可选入口，不是本轮交付重点。

## 1. 交付范围

我们从上游任务中选出 5 个场景，整理成独立的 Doris Demo：

1. 每日订单汇总
2. 客户地域分析
3. 广告数据标准化和合并
4. 迟到订单增量模型
5. Customer Snapshot 和当前客户维表

每个 Demo 都需要包含：

- 可独立运行的 Doris 环境和源数据；
- `dbt_project.yml`、`profiles.yml` 和 model；
- 明确的 `dbt deps/seed/run/test/snapshot` 命令；
- Doris 表设计和 dbt materialization 配置；
- verifier，以及 `manifest.json`、`run_results.json` 等运行证据。

完整的业务说明、模型列表、流程图和命令见：
[5 个 dbt-for-apache-doris 发布 Demo](data-eng-bench-dbt-doris-demos.zh-CN.md)。

## 2. Demo 清单

| Demo | 上游任务 | 主要 model | 重点验证的 Doris/dbt 能力 |
| --- | --- | --- | --- |
| 每日订单汇总 | [dbt-daily-order-summary](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-daily-order-summary) | `daily_order_summary` | Source、Table、DATE/DECIMAL、Duplicate Key、分区、分桶、Async MV |
| 客户地域分析 | [dbt-customer-geographic](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac80/tasks/dbt-customer-geographic) | `stg_customer_addresses`、`stg_orders`、`fct_state_customers` | 跨 Database Source、`ref()`、View/Table、Join、schema 映射 |
| 广告数据标准化和合并 | [dbt-consolidate](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac80/tasks/dbt-consolidate) | 3 个 staging view、`int__ads_unified` | Seed、CSV 类型映射、`dbt_utils`、QUALIFY、UNION ALL、Data Test |
| 迟到订单增量模型 | [dbt-incremental-late-arriving-sales](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac80/tasks/dbt-incremental-late-arriving-sales) | 7 个模型，其中 `incremental_daily_sales` 为 Incremental | Unique Key、`merge`、迟到数据、二次运行、`on_schema_change` |
| Customer Snapshot | [dbt-fix-customer-snapshot-and-build-dimension](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac80/tasks/dbt-fix-customer-snapshot-and-build-dimension) | `stg_customers`、`customer_snapshot`、`dim_customer_current` | Snapshot、SCD Type 2、`check_cols`、硬删除、历史版本 |

## 3. 统一执行方式

每个 Demo 都在干净的 Doris 环境中独立运行：

```text
启动 Doris FE/BE
  -> 创建 Demo 专用 database/schema
  -> 装载源表或 seed
  -> 写入 Doris profile
  -> dbt debug
  -> dbt deps / seed / run / test / snapshot
  -> 检查 manifest 和 run_results
  -> 查询 Doris 对象、DDL 和结果数据
  -> 按 Demo 要求重复运行或修改源数据后再次运行
```

推荐的 Demo 目录结构：

```text
demo/
├── docker-compose.yml
├── Dockerfile
├── load_data.sh
├── dbt_project.yml
├── profiles.yml.example
├── models/
├── snapshots/              # 只有 Snapshot Demo 使用
├── seeds/                  # 只有广告合并 Demo 使用
├── tests/
└── verify/
```

源数据准备方式按场景区分：

| 场景 | 数据准备 |
| --- | --- |
| 每日订单、客户地域、增量、Snapshot | Doris 源表，通过 Stream Load 或固定 fixture 装载 |
| 广告数据合并 | 3 个小 CSV，通过 `dbt seed` 装载 |
| 需要源表更新的场景 | 使用 Doris Unique Key 表，并在两次运行之间执行 UPDATE/DELETE |

## 4. 五个 Demo 的执行重点

### 4.1 每日订单汇总

```text
ORDERS.ORDERS
  -> daily_order_summary (table)
  -> monthly_order_summary_mv (Doris Async MV)
```

执行顺序：

```bash
dbt run --select daily_order_summary
dbt run --select monthly_order_summary_mv
```

先验证 Table model 的业务结果、列类型、Key、分区、分桶和幂等性，再验证 Async MV 的创建、
初始构建、手动刷新和重复执行。

### 4.2 客户地域分析

```text
CUSTOMER.CUSTOMER_ADDRESSES -> stg_customer_addresses (view)
ORDERS.ORDERS               -> stg_orders (view)
两个 staging view            -> fct_state_customers (table)
```

执行 `dbt run` 后，检查 source/ref lineage、跨 Database 访问、LEFT JOIN 结果、每州一行以及
第二次运行的一致性。

### 4.3 广告数据标准化和合并

```text
googleads.csv -> googleads seed -> stg__ads_googleads (view)
metaads.csv   -> metaads seed   -> stg__ads_metaads (view)
tiktokads.csv -> tiktokads seed -> stg__ads_tiktokads (view)
三个 staging view -> int__ads_unified (table) -> dbt_utils test
```

执行 `dbt deps`、`dbt seed`、`dbt run` 和 `dbt test`，检查三个渠道字段是否统一、
重复记录是否去除，以及 `source + ad_date` 是否唯一。

### 4.4 迟到订单增量模型

```text
ORDERS.ORDERS -> order_version_history (table)
              -> incremental_daily_sales (incremental, unique_key=order_id)
              -> channel_latency_analysis
              -> revenue_reconciliation_waterfall
              -> late_arrival_metrics
              -> order_data_quality
              -> daily_sales_summary
```

至少运行三轮：

1. 第一次全量创建目标表；
2. 修改既有订单并插入迟到订单，验证 merge、回看窗口和主键唯一性；
3. 给 model 增加可空列，验证 `on_schema_change='append_new_columns'`。

### 4.5 Customer Snapshot

```text
CUSTOMER.CUSTOMERS -> stg_customers (view)
                    -> customer_snapshot (snapshot)
                    -> dim_customer_current (table)
```

执行：

```bash
dbt run --select stg_customers
dbt snapshot --select customer_snapshot
dbt run --select dim_customer_current
```

修改一个 `check_cols` 字段并删除一个客户后再次执行，检查旧版本是否关闭、新版本是否成为当前
记录，以及硬删除客户是否不再出现在当前维表。

## 5. Verifier 和发布门禁

Verifier 不能只检查 SQL 是否执行成功，还必须检查 dbt 产物和 Doris 结果：

| 检查层 | 内容 |
| --- | --- |
| dbt 产物 | `manifest.json`、`run_results.json`、model 状态、source/ref 依赖 |
| Doris 对象 | database/schema、View/Table 类型、列类型、Key、分区、分桶、properties |
| 数据结果 | 行数、金额、唯一性、NULL、日期和业务公式 |
| 生命周期 | 二次运行、Incremental merge、Snapshot 版本、UPDATE/DELETE、schema change |
| 清理隔离 | 每个 Demo 使用独立 database 或容器，不依赖上一个 Demo 的表 |

发布前每个 Demo 都必须从空 Doris 环境独立跑通，并保存：

```text
Doris / dbt / adapter 版本
执行命令
compiled SQL
manifest.json
run_results.json
verifier 输出
最终 reward 或通过结果
```

## 6. Doris 能力边界

本轮 5 个 Demo 覆盖：

- profile、连接、Source、Ref 和依赖图；
- View、Table、Incremental、Snapshot；
- Seed、Package、Data Test；
- Duplicate Key、Unique Key、分区、分桶和 properties；
- DATE、DECIMAL、窗口函数、QUALIFY、UNION ALL；
- merge、schema change、SCD Type 2 和异步物化视图。

以下能力不属于本轮 Demo 的已支持范围：

- Microbatch；
- Insert Overwrite；
- Contract；
- Freshness；
- Hook；
- dbt Docs/Catalog artifact；
- Grants；
- Doris Catalog 的创建和连接器配置。

External Catalog 必须预先在 Doris 中配置，Demo 只负责通过三层 Relation 读取和转换数据。

## 7. Agent 的位置

Agent 可以作为每个 Demo 的额外入口：

```text
准备 Doris 环境和任务说明
  -> Agent 创建或修改 dbt project
  -> Agent 执行 dbt debug/build/test
  -> verifier 检查 manifest、Doris 对象和业务结果
```

但 Demo 的基础交付不依赖 Agent。先让固定 reference solution 在 Doris 上稳定通过，再增加 Agent
试跑，分别记录 Agent 日志、verifier 结果、reward 和失败原因。

## 8. 后续工作

1. 为 5 个 Demo 固定 Doris、dbt Core 和 dbt-for-apache-doris 版本；
2. 固定 fixture、数据库命名、权限和资源配置；
3. 完成每个 Demo 的 clean-room reference run；
4. 把可复现失败转成 dbt-doris 单元测试或集成测试；
5. 将 5 个 Demo 接入 CI 和发布文档；
6. 在 reference run 稳定后，再为代表 Demo 增加 Agent smoke。

## 9. 资料

- [5 个 dbt-for-apache-doris 发布 Demo](data-eng-bench-dbt-doris-demos.zh-CN.md)
- [data-eng-bench](https://github.com/Snowflake-Labs/data-eng-bench)
- [Apache Doris dbt adapter 文档](https://doris.apache.org/docs/4.x/connection-integration/data-integration/dbt-doris-adapter/)
- [Apache Doris dbt adapter 源码](https://github.com/apache/doris/tree/master/extension/dbt-doris)
- [Doris Stream Load](https://doris.apache.org/docs/4.x/key-features/stream-load/)
- [Doris Async Materialized View Demo](materialized-view.zh-CN.md)
