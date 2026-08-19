# data-eng-bench 的 5 个 dbt-for-apache-doris 发布 Demo

> 数据源：[Snowflake-Labs/data-eng-bench@53353547](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80)。
> 本文只保留 5 个发布 Demo，说明它们的输入、dbt model、执行命令、Doris 对象和验收方式。

本文聚焦 dbt-for-apache-doris 发布真正需要的三件事：

1. 先把 5 个代表场景做成用户可以独立运行的 Doris Demo；
2. 把 Demo 中出现的 dbt 行为、SQL 方言和 Doris 对象变成可复现回归项；
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

其余四个可直接运行的项目位于
[`examples/data-eng-bench-doris-demos`](../examples/data-eng-bench-doris-demos/README.md)，
分别覆盖地域聚合、Seed 合并、Incremental 和 Snapshot。

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

对外可以表述为“5 个 Doris 原生 dbt 场景已通过”；不能据此表述为全部 dbt Core 功能都已兼容。

### 1.9 本地端到端结果

2026-08-19 在本地单 FE/单 BE Doris 集群上，用当前 checkout 的 `dbt-doris`、dbt Core
1.12.2 和 `DORIS_PORT=19030` 从仓库中的四个脚本目录重新执行，结果如下：

| Demo | 执行内容 | 结果 |
| --- | --- | --- |
| 客户地域 | 2 个 View、1 个 Table、2 次 `dbt build`、2 个 Data Test | 通过；CA=2 客户/2 订单/145.00，NY=1/1/50.00 |
| 广告合并 | 3 个 Seed、3 个 staging View、1 个 union Table、`dbt_utils` 唯一性测试、2 次 build | 通过；去重后 6 行 |
| 迟到订单 | 版本历史 Table、Incremental `merge`、5 个下游 Table、2 个内置 Data Test、源数据更新后二次 build | 通过；4 个唯一订单，订单 101 更新为 125.00，8 月 1 日收入 245.00 |
| 客户 Snapshot | staging View、Snapshot 首轮、当前维表、3 个 Data Test；修改客户 1 并删除客户 2 后再次 snapshot | 通过；3 条历史记录，客户 1 一条关闭旧版本和一条当前版本，客户 2 无当前版本 |

每个脚本都在开始时重建自己的专用 fixture database，`verify.sh` 再直接查询 Doris
结果表。广告 Seed 的空表头行由 staging 的 `ad_date is not null` 过滤；这条过滤也保留在
示例中，便于后续把 seed 行为单独纳入回归测试。
