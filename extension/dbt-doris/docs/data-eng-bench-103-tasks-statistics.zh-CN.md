# data-eng-bench 103 个 dbt 任务的 Doris 迁移清单与发布 Demo

> 数据源：[Snowflake-Labs/data-eng-bench@53353547](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80)。
> 本文记录上游任务怎样执行 dbt、会创建哪些模型，以及移植到 Doris 时要验证什么。

本文不比较任务数量、难度分布或关键词多少。对 dbt-for-apache-doris 发布真正有用的是三件事：

1. 先把 5 个代表场景做成用户可以独立运行的 Doris Demo；
2. 用 103 个任务清单定位后续要验证的 dbt 行为、SQL 方言和 Doris 对象；
3. 参考答案和 verifier 先跑通，Agent 只作为后续可选入口。

## 1. 5 个发布 Demo

### 1.1 为什么选这 5 个 Demo

下面五个官方任务可以组成一组 dbt-doris 产品 Demo。原始参考答案负责定义业务结果；
Doris 产品扩展负责展示 dbt-for-apache-doris 1.1.0 的物理表、增量和异步物化视图能力。

| Demo | 原任务覆盖 | Doris 产品扩展 | 发布价值 |
| --- | --- | --- | --- |
| [`dbt-daily-order-summary`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-daily-order-summary) | 独立 project、source、table | Duplicate Key、Range Partition、Hash Distribution、Buckets、Properties、异步物化视图 | 展示 Doris 物理设计和 MV 生命周期 |
| [`dbt-customer-geographic`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-geographic) | 两个 staging view、一个 mart table | 跨 Database Source、schema 映射、`ref()` 血缘 | 展示标准分层建模 |
| [`dbt-consolidate`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-consolidate) | seed、三个 staging view、union table、dbt_utils test | Seed 类型映射、Package 兼容、Data Test | 展示数据准备和质量检查 |
| [`dbt-incremental-late-arriving-sales`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-incremental-late-arriving-sales) | incremental、unique_key、迟到数据 | `merge`、Unique Key MOW、`on_schema_change`、分桶 | 展示增量加工和 schema evolution |
| [`dbt-fix-customer-snapshot-and-build-dimension`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-customer-snapshot-and-build-dimension) | snapshot、下游 dimension | SCD Type 2、历史版本、故障后重跑 | 展示状态型数据建模 |

这五个 Demo 在全部通过下文的发布验收后，可以覆盖 dbt-for-apache-doris 的发布主线：连接、
Source/Ref、View/Table、Seed/Test、物理表配置、Incremental、Snapshot 和异步物化视图。
它们不能证明 Adapter 的全部兼容性。Microbatch、Insert Overwrite、Contract、Freshness、
Hook、dbt Docs/Catalog artifact 和 Grants 需要单独测试或后续 Demo。dbt-for-apache-doris
当前不支持 Doris External Catalog 的三段式命名空间，因此 External Catalog 不纳入本轮 Demo。

| 要发布的能力 | 主要证据 Demo | 必须看到的结果 |
| --- | --- | --- |
| Profile、Source、Ref 和 DAG | 每日订单、客户地域 | dbt artifact 中依赖正确，Doris relation 位于预期 database/schema |
| View、Table 和 Doris 物理设计 | 客户地域、每日订单 | 对象类型、Duplicate Key、Range Partition、Hash Distribution 和 Buckets 正确 |
| Async Materialized View | 每日订单 | MV 创建、初始构建、手动刷新和重复运行成功 |
| Seed、Package 和 Data Test | 广告数据合并 | 三个 seed、dbt_utils macro 和 data test 全部成功 |
| Incremental Merge 和 Schema Change | 迟到订单 | 新增、更新和新增列三次运行结果正确，主键不重复 |
| Snapshot 和 SCD Type 2 | 客户 Snapshot | 字段变化、硬删除、历史关闭和当前维表正确 |

因此，这组 Demo 足以作为 **dbt-for-apache-doris 核心能力** 的发布材料；它不是 Apache Doris
查询性能、并发、存储模型和全部数据集成能力的完整展示。

阅读下面的流程只需要先理解几个词：

| 词 | 在这些 Demo 中的含义 |
| --- | --- |
| Source | Doris 中已经存在的原始表；dbt 读取它，但不负责定义其业务数据 |
| Model | 一个 dbt SQL 文件；SQL 的查询结果会被 materialization 写成数据库对象 |
| Staging | 靠近源表的清洗层，通常做过滤、改名和类型统一，这里多使用 View |
| Mart | 给报表或业务直接使用的结果层，这里多使用 Table |
| Materialization | dbt 把 model 落到 Doris 的方式，例如 View、Table、Incremental 或 Materialized View |
| Snapshot | dbt 跨多次运行保存记录历史版本的机制，不等同于普通 Table model |

### 1.2 Demo 1：每日订单汇总

这个 Demo 把一张订单明细表转换成每日经营指标。它最适合用户第一次接触
dbt-for-apache-doris，因为输入、输出和验证结果都很直观。

可直接运行的项目位于
[`examples/data-eng-bench-daily-order-summary`](../examples/data-eng-bench-daily-order-summary/README.md)。

**业务问题**

运营人员不想逐条查看订单，而是希望每天看到有效订单数和收入。取消、退货和失败的订单
不能计入结果。

**输入与输出**

| 对象 | 类型 | 主要内容 |
| --- | --- | --- |
| <code>ORDERS.ORDERS</code> | 已存在的 Doris 源表 | <code>ORDERED_AT</code>、<code>GRAND_TOTAL</code>、<code>STATUS</code> |
| <code>daily_analytics.daily_order_summary</code> | dbt Table model | <code>order_date</code>、<code>order_count</code>、<code>total_revenue</code> |
| <code>monthly_order_summary_mv</code> | Doris 产品版新增的 Async MV model | 基于每日结果继续按月汇总；不属于上游原始答案 |

一个最小数据示意：

| ORDERED_AT | STATUS | GRAND_TOTAL | 是否计入 |
| --- | --- | ---: | --- |
| 2026-08-01 09:00 | COMPLETED | 100.00 | 是 |
| 2026-08-01 10:00 | CANCELLED | 30.00 | 否 |
| 2026-08-02 11:00 | SHIPPED | 80.00 | 是 |

<code>daily_order_summary</code> 应得到：

| order_date | order_count | total_revenue |
| --- | ---: | ---: |
| 2026-08-01 | 1 | 100.00 |
| 2026-08-02 | 1 | 80.00 |

**model 分别做什么**

| model | materialization | 职责 |
| --- | --- | --- |
| <code>daily_order_summary</code> | <code>table</code> | 把时间戳转成日期，过滤无效状态，按日执行 <code>count</code> 和 <code>sum</code> |
| <code>monthly_order_summary_mv</code> | <code>materialized_view</code> | 汇总每日表，由 Doris 管理初始构建和后续刷新 |

~~~mermaid
flowchart LR
    A[ORDERS.ORDERS<br/>订单明细] -->|过滤状态并按日聚合| B[daily_order_summary<br/>Doris Table]
    B -->|按月再次聚合| C[monthly_order_summary_mv<br/>Doris Async MV]
~~~

**怎样执行**

1. 在 <code>sources.yml</code> 中把 dbt source 指向 Doris 的 <code>ORDERS.ORDERS</code>。
2. 第一次执行 Table model，dbt 编译 SQL，并让 adapter 在 Doris 中创建目标表。
3. Doris 产品版再执行 MV model。首次运行创建异步物化视图并等待初始构建完成。
4. 再次选择 MV 时，<code>refresh_trigger='manual'</code> 配合
   <code>refresh_on_run=true</code> 会提交刷新。
5. verifier 独立从源表计算订单数和收入，与 model 结果比较。

~~~bash
dbt run --select daily_order_summary
dbt run --select monthly_order_summary_mv
~~~

为了展示 Doris 物理设计，Table model 增加下面这些 dbt-for-apache-doris 配置：

~~~text
materialized=table
duplicate_key=[order_date]
partition_by=[order_date]
partition_type=RANGE
partition_by_init=[演示数据范围, MAXVALUE]
distributed_by=[order_date]
buckets=4
properties={replication_num: 1}
refresh_on_run=true
~~~

**这个 Demo 需要证明什么**

- dbt profile 能连接 Doris，<code>source()</code> 能解析大写 database/table；
- Table model 的 DATE、BIGINT 和 DECIMAL 类型正确；
- <code>SHOW CREATE TABLE</code> 中的 Key、分区、分桶和 properties 与 model config 一致；
- 原任务要求的状态过滤、每日唯一性、订单数、收入和幂等性全部正确；
- <code>mv_infos()</code> 能看到 MV，初始构建和 <code>refresh_on_run=true</code>
  触发的手动刷新均成功。

### 1.3 Demo 2：客户地域分析

这个 Demo 展示最常见的 dbt 分层建模：两个原始 source 先变成轻量 staging view，
再通过 <code>ref()</code> 合并成一个面向分析的 mart table。

**业务问题**

业务人员希望按州查看客户数、订单数、收入、客单价，以及每个客户平均贡献多少收入和订单。

**输入与输出**

| 对象 | 类型 | 作用 |
| --- | --- | --- |
| <code>CUSTOMER.CUSTOMER_ADDRESSES</code> | Doris 源表 | 客户地址、州、是否默认收货地址 |
| <code>ORDERS.ORDERS</code> | Doris 源表 | 订单、客户、状态和金额 |
| <code>stg_customer_addresses</code> | View | 只保留默认收货地址，并排除州为空的记录 |
| <code>stg_orders</code> | View | 只保留 COMPLETED、DELIVERED、SHIPPED 订单，并把空金额转成 0 |
| <code>fct_state_customers</code> | Table | 形成每州一行的客户和收入指标 |

最终 mart 的主要列是：

~~~text
state_province
customer_count
order_count
total_revenue
avg_order_value
revenue_per_customer
orders_per_customer
~~~

**model 依赖**

~~~mermaid
flowchart LR
    A[CUSTOMER.CUSTOMER_ADDRESSES] -->|默认地址且州非空| B[stg_customer_addresses<br/>View]
    C[ORDERS.ORDERS] -->|保留有效订单| D[stg_orders<br/>View]
    B -->|按 customer_id 左连接| E[fct_state_customers<br/>Table]
    D --> E
~~~

<code>LEFT JOIN</code> 很重要：某个州即使有客户但暂时没有有效订单，也应该保留，
对应的订单数和收入为 0。

**怎样执行**

~~~bash
dbt run
~~~

dbt 会先解析 DAG。两个 staging view 没有互相依赖，可以先创建；mart 通过
<code>ref('stg_customer_addresses')</code> 和 <code>ref('stg_orders')</code> 声明父节点，
因此只会在两个 view 成功后创建。

**这个 Demo 需要证明什么**

- 一个 dbt project 可以同时读取 Doris 的 <code>CUSTOMER</code> 和 <code>ORDERS</code> database；
- <code>source()</code>、<code>ref()</code>、View 和 Table materialization 能组成正确 DAG；
- Doris 中的标识符大小写和目标 schema <code>geographic_analytics</code> 正确；
- mart 每个州只有一行，指标非负且计算公式正确；
- 第二次 <code>dbt run</code> 结果不变。

### 1.4 Demo 3：广告数据标准化和合并

这个 Demo 不依赖预先存在的业务表，而是从三个小型 CSV 开始。它同时展示
<code>dbt deps</code>、Seed、Package、View、Table 和 Data Test，适合作为完整 dbt 工作流示例。

**业务问题**

Google Ads、Meta Ads 和 TikTok Ads 导出的字段并不完全一样。分析人员需要先把它们整理成
统一字段，再合并成一张可以直接比较渠道表现的表。

**三个 staging model 的区别**

| 输入 | staging model | 标准化逻辑 |
| --- | --- | --- |
| <code>googleads.csv</code> | <code>stg__ads_googleads</code> | 直接把 <code>views</code> 映射为统一的 <code>views</code> |
| <code>metaads.csv</code> | <code>stg__ads_metaads</code> | 用 <code>views_1 + views_2</code> 生成统一的 <code>views</code> |
| <code>tiktokads.csv</code> | <code>stg__ads_tiktokads</code> | 把 <code>views_1</code> 映射为统一的 <code>views</code> |

三个 view 都使用 <code>QUALIFY row_number()</code> 去除相同日期和指标的重复记录。
统一后的列为 <code>ad_date</code>、<code>clicks</code>、<code>impressions</code>、
<code>views</code> 和 <code>conversions</code>。

**model 依赖**

~~~mermaid
flowchart LR
    A[googleads.csv] --> S1[googleads Seed Table] --> V1[stg__ads_googleads<br/>View]
    B[metaads.csv] --> S2[metaads Seed Table] --> V2[stg__ads_metaads<br/>View]
    C[tiktokads.csv] --> S3[tiktokads Seed Table] --> V3[stg__ads_tiktokads<br/>View]
    V1 --> U[int__ads_unified<br/>Table]
    V2 --> U
    V3 --> U
    U --> T[dbt_utils Data Test]
~~~

<code>int__ads_unified</code> 使用 <code>UNION ALL</code> 合并三个 view，并增加
<code>source</code> 列，值分别为 google、meta 和 tiktok。最终表包含：

~~~text
source, ad_date, clicks, impressions, views, conversions
~~~

**怎样执行**

~~~bash
dbt deps
dbt seed
dbt run --select stg__ads_googleads stg__ads_metaads stg__ads_tiktokads int__ads_unified
dbt test
~~~

1. <code>dbt deps</code> 下载 <code>dbt_utils</code>。
2. <code>dbt seed</code> 把三个 CSV 建成 Doris 小表。
3. <code>dbt run</code> 创建三个 staging view 和一个统一 Table。
4. <code>dbt test</code> 执行 <code>dbt_utils.unique_combination_of_columns</code>，
   确认 <code>source + ad_date</code> 唯一。

**这个 Demo 需要证明什么**

- Seed 的日期和数值类型可以稳定映射到 Doris；
- <code>QUALIFY</code>、窗口函数和 <code>UNION ALL</code> 在 Doris 上结果正确；
- dbt Package 的安装、macro dispatch 和 Data Test 能正常执行；
- <code>run_results.json</code> 中 model 和 test 均为 success；
- 重复执行 seed/run 后，行数和结果不发生漂移。

这里的 Seed 只用于小数据 Demo；大型业务数据仍应使用 Stream Load 等 Doris 数据导入方式。

### 1.5 Demo 4：迟到订单增量模型

这个 Demo 解释“订单发生时间”和“数据进入系统时间”不一致时，怎样只重算必要范围。
它是五个 Demo 中最能验证 Incremental materialization 的场景。

**业务问题**

订单可能在 8 月 1 日发生，但直到 8 月 3 日才进入数仓。如果增量任务只读取“今天新增的数据”，
8 月 1 日的销售额就会永久偏低。模型需要根据历史延迟分布自动扩大回看窗口，同时处理同一订单
的后续版本。

这里有两个关键时间：

- <code>ordered_at</code>：订单实际发生时间；
- <code>created_at</code>：这条记录进入系统的时间。

**七个 model 分别做什么**

| model | materialization | 职责 |
| --- | --- | --- |
| <code>order_version_history</code> | Table | 用窗口函数按 <code>order_id + created_at</code> 排出版本，生成 <code>valid_from</code>、<code>valid_to</code> 和当前版本标记 |
| <code>incremental_daily_sales</code> | Incremental | 每个订单保留当前版本；根据 P95 延迟动态决定回看范围 |
| <code>channel_latency_analysis</code> | Table | 按渠道计算平均延迟、P50、P95 和延迟区间占比 |
| <code>revenue_reconciliation_waterfall</code> | Table | 把收入分成按时、迟到 1-2 天、3-7 天和 8 天以上的调整 |
| <code>late_arrival_metrics</code> | Table | 计算每日完整度、延迟指标和置信区间 |
| <code>order_data_quality</code> | Table | 标记未来时间、负金额、空客户和极端延迟，生成质量分 |
| <code>daily_sales_summary</code> | Table | 排除低质量订单，输出每日收入、客户数、完整度和重述风险 |

<code>order_version_history</code> 虽然保存 SCD2 风格字段，但它是普通 Table model，
不是 <code>dbt snapshot</code>。真正的 Snapshot 生命周期由 Demo 5 覆盖。

~~~mermaid
flowchart LR
    S[ORDERS.ORDERS] --> H[order_version_history<br/>Table]
    S --> I[incremental_daily_sales<br/>Incremental]
    H --> I
    I --> C[channel_latency_analysis]
    I --> R[revenue_reconciliation_waterfall]
    I --> L[late_arrival_metrics]
    I --> Q[order_data_quality]
    I --> D[daily_sales_summary]
    R --> D
    L --> D
    Q --> D
~~~

**第一次运行发生什么**

~~~bash
dbt run --select order_version_history incremental_daily_sales channel_latency_analysis revenue_reconciliation_waterfall late_arrival_metrics order_data_quality daily_sales_summary
~~~

目标表尚不存在，<code>is_incremental()</code> 为 false，因此
<code>incremental_daily_sales</code> 读取全部有效订单。上游答案只要求
<code>materialized='incremental'</code> 和 <code>unique_key='order_id'</code>。

**后续运行发生什么**

目标表已经存在时，<code>is_incremental()</code> 为 true。模型从历史订单延迟计算 P95，
并从目标表的最大 <code>created_at</code> 向前回看“P95 + 2 天”，从而重新捕获迟到订单。
dbt-for-apache-doris 产品版显式使用：

~~~jinja
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    distributed_by=['order_id'],
    buckets=4,
    properties={'replication_num': '1'}
) }}
~~~

<code>merge</code> 应把同一 <code>order_id</code> 的新版本写入 Doris Unique Key 表，而不是新增重复行。

一个行为示意：

| 运行 | 源数据变化 | 目标结果 |
| --- | --- | --- |
| 第一次 | 订单 A，金额 100 | A=100 |
| 第二次 | A 出现更新版本，金额 120；新增迟到订单 B=50 | A=120、B=50，A 仍只有一行 |
| 第三次 | 源表和 model 新增可空列 | 目标表新增该列，历史行可为空 |

**这个 Demo 需要证明什么**

- 首次全量和后续 Incremental 两条编译路径都能执行；
- adaptive lookback 能捕获迟到数据；
- <code>merge</code> 更新相同 <code>unique_key</code>，不会重复插入；
- <code>on_schema_change='append_new_columns'</code> 行为正确；
- 七个 model 的结构和数据公式正确；
- 无数据变化时再次运行，行数和收入保持不变。

原 verifier 分八个阶段检查结构、版本连续性、渠道延迟、收入 waterfall、完整度、
质量分、每日汇总和幂等性。Doris 产品 verifier 还要增加真实的新增、更新和 schema change。

### 1.6 Demo 5：Snapshot 和当前客户维表

这个 Demo 展示客户属性发生变化后，Doris 中既能保留旧值，也能方便地查询当前值。
它验证的不是普通 SQL 重建，而是 dbt Snapshot 的跨次运行状态。

**业务问题**

客户修改邮箱或电话后，直接覆盖原行会丢失历史。Snapshot 要把旧记录关闭，再创建一条新的
当前记录。下游维表只读取当前版本，同时计算客户一共有多少个历史版本。

**三个对象的职责**

| 对象 | 类型 | 职责 |
| --- | --- | --- |
| <code>stg_customers</code> | View | 从 <code>CUSTOMER.CUSTOMERS</code> 提供标准化客户数据 |
| <code>customer_snapshot</code> | dbt Snapshot | 使用 <code>customer_id</code> 作为唯一键，监控邮箱、类型、电话、姓名和公司名 |
| <code>dim_customer_current</code> | Table | 只保留 <code>dbt_valid_to is null</code> 的当前记录，并增加分析字段 |

Snapshot 配置使用 <code>strategy='check'</code>、<code>check_cols</code> 和
<code>invalidate_hard_deletes=True</code>。dbt 会维护
<code>dbt_scd_id</code>、<code>dbt_updated_at</code>、
<code>dbt_valid_from</code> 和 <code>dbt_valid_to</code>。

**一次客户变更后的结果**

| customer_id | email | dbt_valid_from | dbt_valid_to | 含义 |
| --- | --- | --- | --- | --- |
| C001 | old@example.com | T1 | T2 | 已关闭的旧版本 |
| C001 | new@example.com | T2 | NULL | 当前版本 |

如果客户从源表删除，<code>invalidate_hard_deletes=True</code> 应关闭其当前版本，而不是继续
把它当作有效客户。

**model 依赖和执行顺序**

~~~mermaid
flowchart LR
    A[CUSTOMER.CUSTOMERS] --> B[stg_customers<br/>View]
    B --> C[customer_snapshot<br/>SCD Type 2]
    C --> D[dim_customer_current<br/>Table]
~~~

~~~bash
dbt run --select stg_customers
dbt snapshot --select customer_snapshot
dbt run --select dim_customer_current
~~~

1. <code>dbt run</code> 先创建客户 staging view。
2. <code>dbt snapshot</code> 比较当前 source 与已有 snapshot 表，插入新版本并关闭旧版本。
3. 最后创建 <code>dim_customer_current</code>，输出每个客户一行，并增加
   <code>days_since_last_update</code>、<code>total_versions</code> 和
   <code>is_recently_changed</code>。

**怎样验证第二次运行**

1. 第一次执行 Snapshot，记录客户当前版本。
2. 修改一个受 <code>check_cols</code> 监控的字段，再删除另一个客户。
3. 第二次执行 Snapshot 和当前维表。
4. 检查变更客户有一条关闭的旧版本和一条新的当前版本；删除客户没有当前版本。
5. 检查 <code>dim_customer_current</code> 每个仍存在的客户正好一行，属性与源表一致。

这个 Demo 最终证明 Snapshot helper relation、SCD 元数据、更新/删除语义、下游
<code>ref()</code> 和重复运行能够在 Doris 上协同工作。

### 1.7 统一执行链

五个 Demo 都遵循同一套 Doris runner 流程：

```text
启动 Doris
  -> 装载源表或 seed 数据
  -> 写入 dbt profile
  -> dbt deps/seed/run/snapshot
  -> verifier 检查 model、Doris DDL 和数据
  -> 二次运行需要验证幂等或增量变化
```

发布时建议先交付这五个 Demo 的 reference solution 和 verifier，再把 Agent 作为同一任务的可选执行入口。

### 1.8 发布验收

这五个 Demo 只有在干净 Doris 环境中分别独立执行通过，才可以作为产品 Demo 发布：

| 检查项 | 发布条件 |
| --- | --- |
| 环境 | 固定 Doris、dbt Core 和 dbt-for-apache-doris 版本；每个 Demo 不依赖前一个 Demo 的表 |
| dbt 过程 | `dbt debug` 成功；所需 `deps/seed/run/test/snapshot` 均返回成功 |
| dbt 证据 | 保存 `manifest.json`、`run_results.json` 和 compiled SQL；目标 node 的状态为 success |
| Doris 对象 | 检查对象类型、schema、列类型、Key、分区、分桶、properties 和 MV 状态 |
| 数据结果 | 原任务 verifier 的结构、业务结果和幂等性断言全部通过 |
| 状态变化 | Incremental 和 Snapshot 在源数据变化后的第二次运行结果正确 |
| 可复现性 | 从空 Doris 实例按 README 命令运行一次即可复现，不依赖 SQL rewrite shim 或人工补表 |

对外可以表述为“5 个 Doris 原生 dbt 场景已通过”；不能据此表述为“103 个任务全部兼容”或
“dbt-for-apache-doris 支持全部 dbt Core 功能”。

## 2. 103 个任务的 Doris 迁移清单

这张表是一份迁移索引，不是兼容性结论：

- **project 形态**：`standalone` 表示任务创建独立 project；`shared/reference` 表示依赖上游已有 project 或 relation。该列用于判断 fixture 和 project 准备方式。
- **dbt 参考执行摘要**：来自 `solution/solve.sh`，长命令会省略末尾；完整命令和 SQL 以链接中的 solution 为准。
- **参考答案中的行为**：只记录 solution 明确出现的 `source/ref`、materialization、dbt 命令和 SQL 构造，不从文件名或 Python import 猜能力。
- **Doris 验证点**：表示这个任务可以检查什么，不表示当前 adapter 已经通过。

| # | task | project 形态 | 任务做什么（上游原始描述） | dbt 参考执行摘要 | 参考答案中的 dbt/SQL 行为 | Doris/dbt-for-apache-doris 验证点 |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | [cohort-retention-matrix](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/cohort-retention-matrix) | shared/reference | Build comprehensive cohort retention matrix with revenue tracking using dbt | dbt deps; dbt run --select cohort_retention_matrix | source、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 2 | [dbt-abc-classification](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-abc-classification) | shared/reference | Build ABC inventory classification using running totals, cumulative percentages, and Pareto analysis with window functions | dbt deps | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 3 | [dbt-basket-composition-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-basket-composition-analysis) | shared/reference | Build dbt models to analyze order basket composition, size patterns, and value distribution | dbt deps; dbt run --select stg_order_baskets int_customer_basket_patterns int_basket_transitions int_customer_cohorts basket_size_analysis transition_summary; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 4 | [dbt-calculate-running-balance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-calculate-running-balance) | standalone | Create a running balance model using window functions to track customer balances over time | dbt deps; dbt run --select +rpt_risk_assessment --full-refresh | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 5 | [dbt-campaign-performance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-campaign-performance) | shared/reference | Build a marketing campaign performance analytics model from scratch | dbt deps; dbt run -s stg_marketing__campaigns stg_marketing__email_events rpt_campaign_performance | ref、view、table、window、date/time | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 6 | [dbt-campaign-roi-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-campaign-roi-analysis) | shared/reference | Build multi-layer marketing campaign ROI analysis model from scratch | dbt deps; dbt run -s stg_marketing__campaigns int_marketing__attributed_conversions mart_marketing__campaign_roi | ref、window | dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义 |
| 7 | [dbt-carrier-delivery-performance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-carrier-delivery-performance) | shared/reference | Fix buggy carrier performance model and add delivery speed classification | dbt deps; dbt run -s rpt_carrier_performance; dbt run-operation create_lowercase_views | window、date/time | 窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 8 | [dbt-cart-abandonment-recovery](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-cart-abandonment-recovery) | shared/reference | Build multi-layer cart abandonment recovery scoring model | dbt deps; dbt run -s stg_ecommerce__abandoned_carts int_ecommerce__cart_recovery_metrics mart_ecommerce__recovery_scorecard | source、ref、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 9 | [dbt-channel-attribution-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-channel-attribution-analysis) | shared/reference | Build dbt models to analyze sales performance across channels and understand customer acquisition patterns | dbt deps; dbt run --select stg_orders__channels int_customer_channel_first_touch channel_performance | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 10 | [dbt-consolidate](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-consolidate) | shared/reference | Standardize and unify data using dbt | dbt deps --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR"; dbt seed --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROJECT_DIR"; dbt run --select stg__ads_googleads stg__ads_metaads stg__ads_tiktokads int__ads_unified --project-dir... | ref、view、table、seed、test/build、window、dbt_utils | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；dbt seed 与 Doris 小数据类型映射；dbt test/build 与 verifier；窗口函数及 Doris 排序/NULL 语义；dbt_utils 宏兼容 |
| 11 | [dbt-coupon-effectiveness](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-coupon-effectiveness) | shared/reference | Build coupon effectiveness analytics with multi-table joins, conditional aggregations, NULL handling, and promotion performance metrics | dbt deps | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 12 | [dbt-customer-acquisition-channel-performance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-acquisition-channel-performance) | shared/reference | Evaluate marketing channels by customer quality and value using first-touch attribution, computing CLTV, repeat rates, and channel tiers | dbt deps; dbt run --select rpt_customer_acquisition_channel_performance_fixed; dbt run --select int_sales__orders_enriched stg_orders__orders | table、window | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 13 | [dbt-customer-churn-cohorts](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-churn-cohorts) | shared/reference | Implement cohort analysis with retention curves, churn probability scoring, and customer lifetime value prediction | dbt deps; dbt run --select  stg_analytics__fact_sales  stg_analytics__dim_customer  stg_analytics__dim_date  stg_customer__customers  int_analytics__fact_sales  int_customer__customers  customer_cohorts  cohort_retention  customer_clv | ref、view、window、date/time | dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 14 | [dbt-customer-churn-early-warning](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-churn-early-warning) | shared/reference | Build a weekly customer churn early-warning mart using dbt and DuckDB | dbt deps; dbt run --select int_sales__orders_enriched; dbt run --select rpt_customer_churn_early_warning_fixed --full-refresh | table、date/time | dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 15 | [dbt-customer-cltv-forecasting](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-cltv-forecasting) | shared/reference | Build advanced customer lifetime value forecast with segmentation, RFM scoring, churn probability, and multiple forecasting methodologies | dbt deps; dbt run --select int_sales__orders_enriched; dbt run --select rpt_customer_cltv_forecast; dbt run-operation create_lowercase_views | table、date/time | dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 16 | [dbt-customer-cohort-retention](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-cohort-retention) | shared/reference | Build dbt models for customer cohort retention analysis using enterprise retail data | dbt deps; dbt run --select +cohort_retention +cohort_revenue +cohort_summary | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 17 | [dbt-customer-cross-sell-insights](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-cross-sell-insights) | shared/reference | Build advanced cross-sell insights marts with multiple models: base metrics with revenue weighting, time-based trends, and customer segmentation using dbt | dbt deps; dbt run --select int_sales__order_lines int_sales__orders_enriched; dbt run --select rpt_cross_sell_insights rpt_cross_sell_trends rpt_cross_sell_by_segment rpt_cross_sell_category_hierarchy --full-refresh | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 18 | [dbt-customer-geographic](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-geographic) | standalone | Build a dbt project that analyzes customer distribution and revenue by state with market performance metrics | dbt deps; dbt run | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 19 | [dbt-customer-lifecycle-journey](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-lifecycle-journey) | shared/reference | Build multi-layer customer lifecycle journey analysis model | dbt deps; dbt run -s stg_customer__lifecycle_events int_customer__journey_metrics mart_customer__lifecycle_scorecard | ref、window、date/time | dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 20 | [dbt-customer-lifetime-value](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-lifetime-value) | shared/reference | Extend the existing retail data warehouse with Customer Lifetime Value (CLV) models using complex CTEs, LAG window functions, and predictive calculations | dbt deps; dbt run --select int_clv_customer_orders int_clv_metrics clv_predictions clv_segments clv_segment_summary clv_cohort_analysis clv_monthly_trends | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 21 | [dbt-customer-ltv-fix](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-ltv-fix) | shared/reference | Investigate and fix customer lifetime value calculation by identifying invalid orders | dbt deps; dbt run --select customer_ltv | source、table | dbt source 与 Doris database/schema 解析；dbt table materialization 与 Doris 对象 DDL |
| 22 | [dbt-customer-order-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-order-analytics) | standalone | Build customer order analytics with tiering using dbt and DuckDB | dbt deps; dbt run --select +dim_customer_tiers --full-refresh | source、ref、view、table、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 23 | [dbt-customer-retention-risk](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-retention-risk) | shared/reference | Build customer retention risk model from scratch with RFM metrics and segment peer comparison | dbt deps; dbt run -s rpt_customer_retention_risk | window、date/time | 窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 24 | [dbt-customer-risk-scoring](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-risk-scoring) | shared/reference | Build a customer risk scoring model based on payment behavior, returns, and chargebacks | dbt deps; dbt run --select stg_orders_risk stg_customers_risk customer_risk_scores; dbt run | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 25 | [dbt-daily-order-summary](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-daily-order-summary) | standalone | Build a dbt model that creates daily order aggregates from the orders table | dbt run; dbt run-operation create_lowercase_views | source、view、table | dbt source 与 Doris database/schema 解析；dbt view/table materialization 与 Doris 对象 DDL |
| 26 | [dbt-dq-macro-enforcement](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-dq-macro-enforcement) | shared/reference | Apply data quality macros to sales order line detail model | dbt deps; dbt run --select fct_order_line_detail; dbt run-operation create_lowercase_views | ref、window、date/time | dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 27 | [dbt-email-campaign-tracker](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-email-campaign-tracker) | shared/reference | Build multi-layer email campaign performance tracker model | dbt deps; dbt run -s stg_marketing__email_metrics int_marketing__email_performance mart_marketing__email_scorecard | source、ref、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 28 | [dbt-exchange-rate-settlement-date](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-exchange-rate-settlement-date) | shared/reference | Fix fact_revenue to use settlement-date FX rates for USD conversion | dbt deps; dbt run --select fact_revenue; dbt run-operation create_lowercase_views | ref、table、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 29 | [dbt-fix-cac-payback-waterfall](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-cac-payback-waterfall) | shared/reference | Fix a CAC payback waterfall report model using dbt + DuckDB, producing analytics.rpt_cac_payback_waterfall_fixed | dbt deps; dbt run --select rpt_cac_payback_waterfall_fixed | table、window、date/time | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 30 | [dbt-fix-category-revenue](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-category-revenue) | shared/reference | Fix revenue inflation in category performance report caused by product-variant join fan-out | dbt deps; dbt run --select rpt_category_performance | ref、date/time | dbt ref 和 DAG 依赖；日期时间 SQL 方言 |
| 31 | [dbt-fix-customer-churn](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-customer-churn) | shared/reference | Fix window function missing PARTITION BY causing incorrect customer activity calculations | dbt deps; dbt run --select fct_customer_activity | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 32 | [dbt-fix-customer-ltv](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-customer-ltv) | shared/reference | Debug and fix incorrect customer lifetime value calculations in a dbt dimensional model | dbt deps; dbt run --select dim_customers | ref、table、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 33 | [dbt-fix-customer-snapshot-and-build-dimension](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-customer-snapshot-and-build-dimension) | standalone | Debug a dbt snapshot model with incorrect SCD Type 2 tracking, then build a downstream dim_customer_current model with analytical columns | dbt deps --profiles-dir .; dbt run --select stg_customers --profiles-dir .; dbt snapshot --select customer_snapshot --profiles-dir .; dbt run --select dim_customer_current --profiles-dir . | ref、snapshot、date/time | dbt ref 和 DAG 依赖；dbt snapshot 与 Doris SCD Type 2；日期时间 SQL 方言 |
| 34 | [dbt-fix-daily-cohorts](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-daily-cohorts) | shared/reference | Fix CTE filtering logic causing data loss in cohort retention analysis | dbt deps; dbt run --select rpt_daily_cohorts | ref、table、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 35 | [dbt-fix-daily-revenue](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-daily-revenue) | shared/reference | Fix missing dates and incorrect cumulative revenue in daily revenue report | dbt deps; dbt run --select rpt_order_daily_summary | ref、table、window、date/time、json/array | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言；Doris JSON/ARRAY 类型和函数 |
| 36 | [dbt-fix-division-by-zero](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-division-by-zero) | shared/reference | Fix division by zero error in customer metrics model and add engagement, churn, and peer comparison metrics | dbt deps; dbt run -s rpt_customer_metrics | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 37 | [dbt-fix-email-attribution](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-email-attribution) | shared/reference | Fix an email attribution dbt model that broke due to upstream tracking changes | dbt deps; dbt run --select stg_ga__sessions stg_ga__events int_sessions_events_joined; dbt run --select rpt_email_attribution_fixed --full-refresh | table、window | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 38 | [dbt-fix-inventory-balance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-inventory-balance) | shared/reference | Fix running total using ROWS instead of RANGE causing incorrect cumulative calculations | dbt deps; dbt run --select fct_inventory_balance; dbt test --select fct_inventory_balance | ref、table、test/build、window、date/time、dbt_utils | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；dbt test/build 与 verifier；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言；dbt_utils 宏兼容 |
| 39 | [dbt-fix-inventory-model](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-inventory-model) | shared/reference | Fix bugs in dbt product inventory metrics model | dbt deps; dbt run -s rpt_product_inventory_metrics | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 40 | [dbt-fix-marketing-attribution](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-marketing-attribution) | shared/reference | Fix the broken marketing attribution report that stopped working after GA4 migration | dbt deps; dbt run --select stg_ga__sessions stg_ga__events int_sessions_events_joined; dbt run --select rpt_attribution_fixed | table、date/time | dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 41 | [dbt-fix-multi-touch-attribution](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-multi-touch-attribution) | shared/reference | Fix a multi-touch marketing attribution mart using dbt + DuckDB/Snowflake | dbt deps; dbt run --select stg_ga__sessions stg_ga__events int_sessions_events_joined; dbt run --select rpt_multi_touch_attribution_fixed | table、window、date/time | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 42 | [dbt-fix-paid-search-attribution](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-paid-search-attribution) | standalone | Fix paid search attribution reporting models using dbt + DuckDB/Snowflake | dbt deps; dbt run --select stg_ga__sessions stg_ga__events int_sessions_events_joined; dbt run --select rpt_paid_search_attribution_fixed | table、date/time | dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 43 | [dbt-fix-product-metrics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-product-metrics) | shared/reference | Fix fact table grain issue causing inflated product metrics from improper join to returns | dbt deps; dbt run --select fct_product_metrics | ref、table、json/array | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；Doris JSON/ARRAY 类型和函数 |
| 44 | [dbt-fix-refund-reconciliation](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-refund-reconciliation) | shared/reference | Fix bugs in return-refund reconciliation model and add refund velocity classification | dbt deps; dbt run -s rpt_return_reconciliation | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 45 | [dbt-fix-repeat-purchase-cohort-revenue](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-repeat-purchase-cohort-revenue) | shared/reference | Fix a broken repeat purchase cohort revenue model using dbt + DuckDB/Snowflake | dbt deps; dbt run --select rpt_repeat_purchase_cohort_revenue_fixed; dbt run --select int_sales__orders_enriched | table、window、date/time | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 46 | [dbt-fix-timezone-sales](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fix-timezone-sales) | shared/reference | Fix timezone handling bug in sales aggregation causing incorrect daily sales totals | dbt deps; dbt run --select int_sales__orders_enriched | ref、view、date/time | dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 47 | [dbt-fraud-detection-model](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fraud-detection-model) | shared/reference | Build a fraud detection model using multiple risk criteria including high value, bulk orders, velocity, and address mismatch | dbt deps; dbt run --select stg_fd_orders stg_fd_order_lines stg_fd_addresses fraud_flagged_orders | source、ref、view、table、window | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 48 | [dbt-fulfillment-sla](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-fulfillment-sla) | shared/reference | Build fulfillment SLA analytics with percentile calculations, date arithmetic, carrier performance comparison, and multi-dimensional analysis | dbt deps; dbt run --select int_shipment_times fulfillment_sla carrier_performance warehouse_performance fulfillment_summary | ref、view、table、window、date/time | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 49 | [dbt-gl-reconciliation](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-gl-reconciliation) | shared/reference | Implement GL reconciliation with trial balance, out-of-balance detection, unusual balance identification, and period-over-period activity analysis | dbt run | source、ref、view、table、window | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 50 | [dbt-harbor-marketing-mix-model](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-harbor-marketing-mix-model) | shared/reference | Marketing Mix Modeling - Media effectiveness, saturation curves, and budget optimization | dbt deps --profiles-dir .; dbt run --profiles-dir . --select stg_mmm__daily_sales stg_mmm__marketing_spend stg_mmm__calendar stg_mmm__channel_mapping; dbt run --profiles-dir . --select int_mmm__baseline_sales int_mmm__adstock_transformed... | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 51 | [dbt-harbor-product-affinity](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-harbor-product-affinity) | shared/reference | Build a product affinity and market basket analysis pipeline using dbt to calculate association rules (support, confidence, lift) and identify top product bundles for merchandising | dbt deps; dbt run --select stg_basket__order_products int_affinity__product_pairs int_affinity__association_rules fct_product_affinity_matrix rpt_recommended_bundles | source、ref、table、window | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 52 | [dbt-harbor-warehouse-capacity](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-harbor-warehouse-capacity) | standalone | Build a warehouse capacity planning and peak load analysis pipeline using dbt to identify bottlenecks, calculate utilization metrics, and forecast staffing needs across multiple warehouses | dbt deps; dbt run --profiles-dir . --target dev; dbt run-operation create_lowercase_views --profiles-dir . --target dev | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 53 | [dbt-hr-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-hr-analytics) | standalone | Build an HR analytics and workforce planning pipeline using dbt + DuckDB | dbt deps; dbt run --select stg_hr__employees stg_hr__org_hierarchy stg_hr__departments stg_hr__compensation stg_hr__time_entries stg_hr__positions stg_orders stg_shipments int_hr__employee_hierarchy int_hr__manager_spans int_hr__employee_productivity... | source、ref、view、table、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 54 | [dbt-incremental-late-arriving-sales](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-incremental-late-arriving-sales) | shared/reference | Build incremental sales pipeline with SCD Type 2 versioning, revenue waterfall reconciliation, channel-specific latency analysis, and completeness estimation | dbt deps; dbt run --select order_version_history incremental_daily_sales channel_latency_analysis revenue_reconciliation_waterfall late_arrival_metrics order_data_quality daily_sales_summary | source、ref、table、incremental、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；dbt incremental 与 Doris Unique Key/增量重跑；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 55 | [dbt-inventory-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-inventory-analysis) | shared/reference | Build a dbt project that analyzes inventory levels by warehouse with derived metrics | dbt run; dbt run-operation create_lowercase_views | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 56 | [dbt-inventory-stockout-risk](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-inventory-stockout-risk) | standalone | Build a dbt mart that predicts near-term stockout risk by SKU and warehouse using the enterprise retail warehouse. | dbt deps; dbt run --select +fct_stockout_risk --full-refresh | ref、view、table、window、date/time | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 57 | [dbt-inventory-turnover-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-inventory-turnover-analysis) | shared/reference | Build an inventory turnover and days of supply analysis mart for supply chain optimization | dbt deps; dbt run --select stg_orders__order_lines stg_orders__orders stg_inventory__inventory_levels stg_product__product_variants stg_product__products; dbt run --select rpt_inventory_turnover_analysis | table、window、date/time | dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 58 | [dbt-loyalty-points-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-loyalty-points-analysis) | shared/reference | Build loyalty points analytics with dbt analyzing program engagement and metrics | dbt deps; dbt run --select stg_lp_transactions stg_lp_programs stg_lp_customers int_loyalty_metrics program_loyalty_summary | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 59 | [dbt-monthly-channel-revenue](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-monthly-channel-revenue) | shared/reference | Build dbt models analyzing monthly sales performance by order channel with MoM growth, market share, market benchmarks, and composite scoring | dbt deps; dbt run --select stg_orders__channel int_monthly_channel_metrics int_monthly_market_benchmarks monthly_channel_performance; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 60 | [dbt-multi-warehouse-stock-rebalance](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-multi-warehouse-stock-rebalance) | shared/reference | Build inventory rebalancing recommendation model across warehouses | dbt deps; dbt run -s rpt_warehouse_rebalancing | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 61 | [dbt-multicurrency-lifo](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-multicurrency-lifo) | shared/reference | Implement LIFO inventory costing with multi-currency support, exchange rate conversion, weighted-average fallback costing, inventory shortfall tracking, and turnover metrics using dbt | dbt deps; dbt run --select  stg_finance__currency_exchange_rates  stg_procurement__purchase_orders  stg_procurement__purchase_order_lines  stg_orders__orders  stg_orders__order_lines  int_finance__exchange_rates_daily  int_procurement__purchases_enriched ... | ref、view、table、window | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 62 | [dbt-order-fulfillment-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-order-fulfillment-analytics) | shared/reference | Build an order fulfillment and return analytics pipeline using dbt | dbt deps; dbt run --select  stg_orders stg_order_lines stg_shipments stg_shipment_lines stg_order_status_history stg_returns  int_order_lifecycle int_shipment_performance int_order_fulfillment_status int_order_revenue_summary  mart_fulfillment_metrics... | source、ref、view、table、test/build、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；dbt test/build 与 verifier；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 63 | [dbt-order-fulfillment-metrics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-order-fulfillment-metrics) | standalone | Build a dbt project that analyzes order fulfillment performance | dbt deps; dbt run --select +fct_fulfillment_by_order_type --full-refresh | ref、view、table、window、date/time | dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 64 | [dbt-order-interval-metrics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-order-interval-metrics) | shared/reference | Build dbt models to analyze time intervals between consecutive customer orders and classify ordering patterns | dbt deps; dbt run --select stg_orders__timeline int_customer_order_gaps customer_order_intervals | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 65 | [dbt-order-reconciliation](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-order-reconciliation) | shared/reference | Build a multi-layer dbt project with staging, intermediate, and mart models for order financial reconciliation including subtotal and tax variance analysis | dbt run --profiles-dir . | source、ref、view | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL |
| 66 | [dbt-payment-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-payment-analytics) | shared/reference | Build a payment method performance analytics model from scratch | dbt deps; dbt run -s rpt_payment_analytics | ref、table、window | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 67 | [dbt-payment-fraud-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-payment-fraud-analysis) | shared/reference | Implement payment method analysis and fraud detection with anomaly pattern recognition | dbt build | source、ref、view、table、test/build、date/time、json/array | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；dbt test/build 与 verifier；日期时间 SQL 方言；Doris JSON/ARRAY 类型和函数 |
| 68 | [dbt-price-elasticity](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-price-elasticity) | standalone | Calculate price elasticity of demand using historical price and sales data | dbt deps; dbt run --select stg_price_history stg_order_sales int_monthly_price_quantity int_price_changes product_elasticity --profiles-dir . --target dev | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 69 | [dbt-product-affinity](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-affinity) | shared/reference | Build product affinity analytics using self-joins, association rule mining (support, confidence, lift), and market basket analysis | dbt deps | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 70 | [dbt-product-category-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-category-analytics) | shared/reference | Build a dbt project that analyzes product sales performance by category | dbt deps; dbt run --select stg_order_lines stg_products stg_categories stg_orders int_product_sales fct_category_performance; dbt run | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 71 | [dbt-product-performance-metrics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-performance-metrics) | shared/reference | Build dbt models to analyze product-level sales performance, return rates, and revenue contribution | dbt deps; dbt run --select stg_order_lines__products int_product_sales_summary product_performance | source、ref、view、table、window | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 72 | [dbt-product-return-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-return-analysis) | shared/reference | Build a product return analysis model from scratch | dbt deps; dbt run -s rpt_product_returns | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 73 | [dbt-product-return-rate-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-return-rate-analysis) | shared/reference | Build a dbt project that produces monthly product return rate reporting table with return metrics, revenue impact, and return reason analysis | dbt deps; dbt run --select int_sales__orders_enriched int_sales__order_lines; dbt run --select +rpt_product_return_rates_monthly | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 74 | [dbt-product-sales-velocity](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-product-sales-velocity) | shared/reference | Build dbt models for velocity, trends, and scoring | dbt deps; dbt run --select stg_order_lines__sales int_product_daily_sales int_product_monthly_sales int_product_sales_metrics product_sales_velocity | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 75 | [dbt-receivables-aging-buckets](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-receivables-aging-buckets) | shared/reference | Create a payments-aware receivables aging model with dedupe and as-of logic | dbt deps; dbt run --select fact_receivables_aging; dbt run-operation create_lowercase_views | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 76 | [dbt-retail-task-01-channel-revenue](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-retail-task-01-channel-revenue) | shared/reference | Implement Channel Revenue & Margin Trend Model | dbt deps; dbt run --select ts_sales__channel_revenue_margin_monthly | ref、view、date/time | dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 77 | [dbt-retail-task-04-market-roi](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-retail-task-04-market-roi) | shared/reference | Implement marketing analytics | dbt deps; dbt run --select rpt_channel_roi_payback_monthly --profiles-dir .; dbt test --select rpt_channel_roi_payback_monthly --profiles-dir . | ref、view、test/build、date/time、dbt_utils | dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL；dbt test/build 与 verifier；日期时间 SQL 方言；dbt_utils 宏兼容 |
| 78 | [dbt-retail-task-07-cart-recovery](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-retail-task-07-cart-recovery) | shared/reference | Implement cart recovery analysis for retail db | dbt deps; dbt run -s +fct_cart_recovery_priority; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 79 | [dbt-retail-task-10-trust-risk](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-retail-task-10-trust-risk) | shared/reference | Implement review moderation and trust risk for retail analytics | dbt deps; dbt run --select int_reviews__enriched  fct_review_moderation_risk  rpt_review_moderation_kpis  --profiles-dir ./; dbt test --select int_reviews__enriched  fct_review_moderation_risk  rpt_review_moderation_kpis  --profiles-dir ./ | ref、view、test/build、window、date/time | dbt ref 和 DAG 依赖；dbt view materialization 与 Doris 对象 DDL；dbt test/build 与 verifier；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 80 | [dbt-rfm-customer-segmentation](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-rfm-customer-segmentation) | shared/reference | Implement RFM (Recency, Frequency, Monetary) customer segmentation using enterprise retail data | dbt run | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 81 | [dbt-rfm-customer-segmentation-2](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-rfm-customer-segmentation-2) | shared/reference | Build RFM customer segmentation using dbt and DuckDB | dbt deps; dbt run --select rfm_segments rpt_segment_summary rpt_customer_recommendations rfm_cohort_retention | source、ref、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 82 | [dbt-rfm-customer-tiering](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-rfm-customer-tiering) | shared/reference | Implement RFM customer segmentation with dynamic tiering, tier transitions, and channel revenue attribution | dbt deps; dbt run --select customer_rfm_scores customer_tier_transitions channel_revenue_attribution customer_cohort_analysis | ref、window、date/time、json/array | dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言；Doris JSON/ARRAY 类型和函数 |
| 83 | [dbt-sales-funnel-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-sales-funnel-analysis) | standalone | Build a sales funnel analysis pipeline with conversion rates and drop-off analysis | dbt deps; dbt run --profiles-dir . | source、ref、view、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL |
| 84 | [dbt-session-attribution](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-session-attribution) | shared/reference | Implement sessionization and time-decay multi-touch attribution in a single dbt model | dbt deps; dbt run --select attribution_report stg_digital__web_sessions stg_analytics__fact_sales; dbt run-operation create_lowercase_views | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 85 | [dbt-supplier-payment-optimization](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-supplier-payment-optimization) | shared/reference | Implement supplier payment optimization with aging analysis, early payment discounts, annualized ROI calculations, supplier risk scoring, multi-factor priority scoring, payment strategy recommendations, and cash flow projections with working capital impact in a single comprehensive dbt model | dbt deps; dbt run --select +supplier_payment_optimization; dbt run-operation create_lowercase_views | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 86 | [dbt-supplier-scorecard](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-supplier-scorecard) | standalone | Build a Supplier Performance Scorecard using dbt with on-time delivery, defect rate, cost variance, and composite scoring | dbt deps; dbt run --select stg_sp__suppliers stg_sp__purchase_orders stg_sp__purchase_order_receipts stg_sp__purchase_order_receipt_lines stg_sp__supplier_invoices int_po_delivery_performance int_po_quality_performance int_po_cost_performance... | source、ref、view、table、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 87 | [dbt-test-orders-filter](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-test-orders-filter) | shared/reference | Filter test, sample, and internal orders from production sales reporting | dbt deps; dbt run --select production_sales; dbt run-operation create_lowercase_views | source、ref、table | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL |
| 88 | [dbt-three-way-matching](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-three-way-matching) | shared/reference | Implement purchase order to receipt to invoice three-way matching with variance analysis and exception handling | dbt deps; dbt run --select +three_way_match; dbt run-operation create_lowercase_views | ref、date/time、json/array | dbt ref 和 DAG 依赖；日期时间 SQL 方言；Doris JSON/ARRAY 类型和函数 |
| 89 | [dbt-warehouse-fulfillment-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-warehouse-fulfillment-analytics) | shared/reference | Build a warehouse fulfillment performance analytics model from scratch | dbt deps; dbt run -s rpt_warehouse_fulfillment | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 90 | [dbt-web-session-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-web-session-analytics) | standalone | Analyze web session data to calculate quality scores and engagement metrics using window functions | dbt deps; dbt run | source、ref、view、table、window | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义 |
| 91 | [dbt-weekly-sales-growth](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-weekly-sales-growth) | shared/reference | Build dbt models to calculate weekly sales trends with week-over-week growth metrics | dbt deps; dbt run --select stg_orders__weekly int_weekly_sales weekly_sales_growth; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 92 | [deferred-revenue-recognition](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/deferred-revenue-recognition) | shared/reference | Build comprehensive revenue recognition schedule with prorated monthly calculations using dbt | dbt deps; dbt run --select deferred_revenue_schedule; dbt run-operation create_lowercase_views | source、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 93 | [fifo-inventory-cogs](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/fifo-inventory-cogs) | shared/reference | Implement FIFO inventory valuation with pure SQL dbt models using window functions and range-based joins | dbt deps; dbt run --select stg_fifo_transactions int_receipt_layers int_pick_consumption int_fifo_allocation fifo_cogs_monthly ending_inventory_valuation inventory_turnover_analysis | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 94 | [late-arriving-orders-reconciliation](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/late-arriving-orders-reconciliation) | shared/reference | Identify orders entered in a different GL period than when placed and generate revenue adjustment entries | solution 未直接执行 dbt；需结合 verifier/environment | date/time | 日期时间 SQL 方言 |
| 95 | [marketing-campaigns-harbor](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/marketing-campaigns-harbor) | shared/reference | Build a dimensional data model using dbt for marketing analytics, including RFM customer segmentation, campaign performance metrics, anomaly detection, and channel efficiency calculations. | dbt deps; dbt run --select $MODEL_LIST | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 96 | [payment-risk-scoring](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/payment-risk-scoring) | shared/reference | Build dbt models for payment transaction risk analysis with fraud risk scoring, customer risk profiling, and behavioral analytics to identify suspicious transactions and high-risk customers. | dbt deps; dbt run --select stg_pos__transactions models/intermediate/payments models/marts/payments; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 97 | [pos-operations](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/pos-operations) | shared/reference | Build a dimensional data model using dbt for point-of-sale operations analytics, including order fulfillment analysis, payment method performance, product velocity metrics, return analysis, and order pattern detection. | dbt deps; dbt run --select $ALL_MODELS | ref、window、date/time | dbt ref 和 DAG 依赖；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 98 | [promotional-lift-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/promotional-lift-analysis) | standalone | Create dbt models to analyze marketing promotion effectiveness by comparing redemption-driven sales against historical product performance | dbt run | table、date/time | dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 99 | [shipping-fulfillment-quality-scoring](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/shipping-fulfillment-quality-scoring) | shared/reference | Build dbt models for shipping and fulfillment quality analysis with delivery scoring, tracking metrics, and carrier performance scorecards to optimize logistics operations and identify carrier improvement opportunities. | dbt deps; dbt run --select int_shipment_tracking_metrics int_shipment_delivery_metrics int_shipment_package_metrics shipment_quality_scores carrier_performance_scorecard | ref、table、window、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 100 | [tier-migration-analysis](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/tier-migration-analysis) | shared/reference | Analyze customer tier migrations between time periods to understand loyalty program movement patterns | dbt deps; dbt run --select tier_migration_matrix cohort_tier_progression tier_velocity_metrics tier_migration_revenue_impact tier_retention_risk | source、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 101 | [time-decay-attribution-model](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/time-decay-attribution-model) | shared/reference | Implement a time decay marketing attribution model in dbt that assigns weighted credit to marketing touchpoints based on their temporal proximity to conversion events. | dbt run | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |
| 102 | [web-session-quality-scoring](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/web-session-quality-scoring) | shared/reference | Build dbt models for web session quality analysis with engagement scoring, conversion probability, and behavioral analytics to optimize digital marketing spend and identify UX improvement opportunities. | dbt deps; dbt run --select models/intermediate/digital models/marts/digital | ref、table、date/time | dbt ref 和 DAG 依赖；dbt table materialization 与 Doris 对象 DDL；日期时间 SQL 方言 |
| 103 | [workforce-analytics](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/workforce-analytics) | shared/reference | Build a dimensional data model using dbt and DuckDB for workforce analytics, including employee management, compensation analysis, payroll reporting, time tracking, employee lifecycle events, and organizational insights. | dbt deps; dbt run --select models/staging/hr models/intermediate/hr models/marts/hr; dbt run-operation create_lowercase_views | source、ref、view、table、window、date/time | dbt source 与 Doris database/schema 解析；dbt ref 和 DAG 依赖；dbt view/table materialization 与 Doris 对象 DDL；窗口函数及 Doris 排序/NULL 语义；日期时间 SQL 方言 |

## 3. 怎样使用这份清单

每迁移一个任务，按下面顺序读取和执行：

1. 打开任务链接，先读 `instruction.md`，确认业务结果和目标 relation。
2. 读 `solution/solve.sh`，列出 source、ref、model、materialization 和真实 dbt 命令。
3. 只准备该任务需要的源表和最小 dbt project，不依赖其他任务留下的 Doris 对象。
4. 为 Doris 配置 profile、Key Model、分区、分桶和 SQL 方言。
5. 运行参考答案，再运行原 verifier 和 Doris DDL 检查。
6. 保存 `manifest.json`、`run_results.json`、compiled SQL、verifier 输出和版本信息。

任务通过只说明该任务要求的行为通过，不能外推为整个 adapter 或 103 个任务全部兼容。
如果 solution 没有直接执行 dbt，必须继续检查 environment 和 verifier，不能只看表中的命令摘要。

## 4. 官方仓库怎样分类

官方 README 只给出总体汇总：Analytics 65、Development and bug-fixes 16、
Dimensional modeling and snapshots 9、Data engineering 13。它没有给 103 个任务逐项附上这四类标签。

每个 `task.toml` 另有更细的 `metadata.category`，但它与 README 的四类汇总不是同一套口径。
这些分类描述任务主题，不代表 Doris 迁移难度，所以不再放进迁移主表。逐任务原始 metadata
可通过表中的固定 commit 链接查看。
