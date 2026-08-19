# data-eng-bench 的 5 个 dbt-for-apache-doris 发布 Demo

> 数据源：[Snowflake-Labs/data-eng-bench@53353547](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80)。
> 本文只保留 5 个发布 Demo，说明它们的输入、dbt model、执行命令、Doris 对象和验收方式。

本文聚焦 dbt-for-apache-doris 发布真正需要的三件事：

1. 先把 5 个代表场景做成用户可以独立运行的 Doris Demo；
2. 把 Demo 中出现的 dbt 行为、SQL 方言和 Doris 对象变成可复现回归项；
3. 参考答案和 verifier 先跑通，Agent 只作为后续可选入口。

## 1. 5 个发布 Demo

### 1.1 为什么选这 5 个 Demo

下面五个官方任务可以组成一组 dbt-doris 产品 Demo。原始任务负责定义业务结果；
本地 Demo 项目负责展示 dbt-for-apache-doris 的物理表、增量、Snapshot 和异步物化视图能力。

| Demo | 原任务覆盖 | Doris 产品扩展 | 发布价值 |
| --- | --- | --- | --- |
| [`dbt-daily-order-summary`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-daily-order-summary) | 独立 project、source、table | Duplicate Key、Range Partition、Hash Distribution、Buckets、Properties、异步物化视图 | 展示 Doris 物理设计和 MV 生命周期 |
| [`dbt-customer-geographic`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-customer-geographic) | 两个 staging view、一个 mart table | 跨 Database Source、schema 映射、`ref()` 血缘 | 展示标准分层建模 |
| [`dbt-consolidate`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-consolidate) | seed、三个 staging view、union table、dbt_utils test | Seed 类型映射、Package 兼容、Data Test | 展示数据准备和质量检查 |
| [`dbt-incremental-late-arriving-sales`](https://github.com/Snowflake-Labs/data-eng-bench/tree/53353547b9869d35d61b40fd6ee9397a7ac8ca80/tasks/dbt-incremental-late-arriving-sales) | incremental、unique_key、迟到数据 | `merge`、Unique Key MOW、分桶 | 展示增量加工和重复运行 |
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
| Incremental Merge | 迟到订单 | 首次全量和二次增量均成功，新增和更新正确且主键不重复 |
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
| 上游任务的 <code>ORDERS.ORDERS</code> | 任务中的逻辑源表 | <code>ORDERED_AT</code>、<code>GRAND_TOTAL</code>、<code>STATUS</code> |
| <code>dbt_demo_daily_source.orders</code> | 本 Demo 的 Doris 源表 | 与上游逻辑源表相同的最小字段 |
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
    A[dbt_demo_daily_source.orders<br/>订单明细] -->|过滤状态并按日聚合| B[daily_order_summary<br/>Doris Table]
    B -->|按月再次聚合| C[monthly_order_summary_mv<br/>Doris Async MV]
~~~

#### 1.2.1 创建源表和样例数据

下面的操作对应仓库中的可运行项目
[`examples/data-eng-bench-daily-order-summary`](../examples/data-eng-bench-daily-order-summary/README.md)。
示例默认连接 `127.0.0.1:9030`，使用 `root` 空密码；目标 Database 是
`dbt_demo_daily`，源 Database 是 `dbt_demo_daily_source`。

上游任务读取 `ORDERS.ORDERS`。为了不删除或修改用户已有的业务 Database，本示例把同样的
输入放在专用表 `dbt_demo_daily_source.orders` 中；后面的 Source 配置会把这个专用表映射到
dbt project。

创建 `scripts/setup.sql`：

~~~sql
DROP DATABASE IF EXISTS dbt_demo_daily;
DROP DATABASE IF EXISTS dbt_demo_daily_source;

CREATE DATABASE dbt_demo_daily_source;
CREATE DATABASE dbt_demo_daily;

CREATE TABLE dbt_demo_daily_source.orders (
    order_id BIGINT,
    ordered_at DATETIME,
    grand_total DECIMAL(18, 2),
    status VARCHAR(32)
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ('replication_num' = '1');

INSERT INTO dbt_demo_daily_source.orders VALUES
    (1, '2026-08-01 09:00:00', 100.00, 'COMPLETED'),
    (2, '2026-08-01 10:00:00',  30.00, 'CANCELLED'),
    (3, '2026-08-02 11:00:00',  80.00, 'SHIPPED'),
    (4, '2026-08-02 12:00:00',  20.00, 'RETURNED'),
    (5, '2026-08-03 13:00:00',  25.55, 'FAILED'),
    (6, '2026-08-03 14:00:00',  40.20, 'DELIVERED');
~~~

使用 MySQL Client 连接 Doris FE 并执行：

~~~bash
mysql -h 127.0.0.1 -P 9030 -u root < scripts/setup.sql
~~~

这一步只准备原始数据。dbt 后面负责创建结果 Table 和异步物化视图。

#### 1.2.2 创建 dbt 项目

项目目录如下：

~~~text
data-eng-bench-daily-order-summary/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── staging/
│   │   └── sources.yml
│   └── marts/
│       ├── daily_order_summary.sql
│       ├── daily_order_summary.yml
│       └── monthly_order_summary_mv.sql
└── scripts/
    ├── setup.sql
    ├── run.sh
    └── verify.sh
~~~

`dbt_project.yml` 定义项目名、Profile 名和默认物化方式：

~~~yaml
name: data_eng_bench_daily_order_summary
version: 1.0.0
config-version: 2
profile: data_eng_bench_daily_order_summary

model-paths: ["models"]

models:
  data_eng_bench_daily_order_summary:
    +materialized: table
    +properties:
      replication_num: "1"
~~~

`profile` 必须和下一步 `profiles.yml` 的顶层名称一致。这里把 `replication_num` 设为 1，
使 Demo 可以运行在单 BE Doris 开发集群。

#### 1.2.3 配置 Doris 连接

在项目根目录创建 `profiles.yml`：

~~~yaml
data_eng_bench_daily_order_summary:
  target: dev
  outputs:
    dev:
      type: doris
      host: "{{ env_var('DORIS_HOST', '127.0.0.1') }}"
      port: "{{ env_var('DORIS_PORT', '9030') | int }}"
      username: "{{ env_var('DORIS_USER', 'root') }}"
      password: "{{ env_var('DORIS_PASSWORD', '') }}"
      schema: dbt_demo_daily
      threads: 2
~~~

这里的 `schema` 对应 Doris Database，而不是 Catalog。Profile 不需要再填写 `database`。
运行命令时显式传入 `--profiles-dir .`，dbt 才会读取项目目录中的这个文件。

先验证连接：

~~~bash
dbt debug --project-dir . --profiles-dir .
~~~

预期输出包含 `Connection test: OK connection ok`。

#### 1.2.4 声明 Doris Source

在 `models/staging/sources.yml` 中声明源表：

~~~yaml
version: 2

sources:
  - name: orders
    database: dbt_demo_daily_source
    schema: dbt_demo_daily_source
    tables:
      - name: orders
        identifier: orders
~~~

Model 中的 `{{ source('orders', 'orders') }}` 会被编译为
`dbt_demo_daily_source.orders`。这里通过 Source 的 `database` 和 `schema` 指向源
Database，同时把模型写入 Profile 的目标 Database `dbt_demo_daily`。

#### 1.2.5 编写每日汇总 Table model

创建 `models/marts/daily_order_summary.sql`：

~~~sql
{{
  config(
    materialized='table',
    duplicate_key=['order_date'],
    partition_by=['order_date'],
    partition_type='RANGE',
    partition_by_init=[
      "PARTITION p202608 VALUES LESS THAN ('2026-09-01')",
      "PARTITION pmax VALUES LESS THAN ('9999-12-31')"
    ],
    distributed_by=['order_date'],
    buckets=1,
    properties={'replication_num': '1'}
  )
}}

with valid_orders as (
    select
        cast(ordered_at as date) as order_date,
        grand_total
    from {{ source('orders', 'orders') }}
    where status not in ('CANCELLED', 'RETURNED', 'FAILED')
)

select
    order_date,
    count(*) as order_count,
    round(sum(grand_total), 2) as total_revenue
from valid_orders
group by order_date
~~~

这段配置会让 Adapter 创建：

~~~text
dbt_demo_daily.daily_order_summary
  类型: TABLE
  Key: DUPLICATE KEY(order_date)
  分区: RANGE(order_date)
  分桶: HASH(order_date), 1 bucket
  副本: 1
~~~

`partition_by_init` 是 Demo 的固定初始分区。生产模型应按真实数据保留范围设计静态分区，
或改用适合业务写入方式的 Dynamic Partition 配置。

#### 1.2.6 添加 dbt Data Test

创建 `models/marts/daily_order_summary.yml`：

~~~yaml
version: 2

models:
  - name: daily_order_summary
    columns:
      - name: order_date
        data_tests:
          - not_null
          - unique
      - name: order_count
        data_tests:
          - not_null
      - name: total_revenue
        data_tests:
          - not_null
~~~

运行 Table model 和四个 Data Test：

~~~bash
dbt build \
  --select daily_order_summary \
  --project-dir . \
  --profiles-dir .
~~~

`dbt build` 应显示一个 model 和四个 test 成功。查询 Doris 结果：

~~~sql
SELECT order_date, order_count, total_revenue
FROM dbt_demo_daily.daily_order_summary
ORDER BY order_date;
~~~

预期结果：

~~~text
2026-08-01 | 1 | 100.00
2026-08-02 | 1 |  80.00
2026-08-03 | 1 |  40.20
~~~

只有 `COMPLETED`、`SHIPPED` 和 `DELIVERED` 被计入，另外三种状态被过滤。

#### 1.2.7 编写月度异步物化视图

创建 `models/marts/monthly_order_summary_mv.sql`：

~~~sql
{{
  config(
    materialized='materialized_view',
    build_mode='immediate',
    refresh_method='auto',
    refresh_trigger='manual',
    refresh_on_run=true,
    duplicate_key=['order_month'],
    distributed_by=['order_month'],
    buckets=1,
    properties={'replication_num': '1'}
  )
}}

select
    date_trunc(order_date, 'month') as order_month,
    sum(order_count) as order_count,
    round(sum(total_revenue), 2) as total_revenue
from {{ ref('daily_order_summary') }}
group by date_trunc(order_date, 'month')
~~~

首次运行会创建 `dbt_demo_daily.monthly_order_summary_mv`，`BUILD IMMEDIATE` 让 Doris
启动初始构建：

~~~bash
dbt run \
  --select monthly_order_summary_mv \
  --project-dir . \
  --profiles-dir .
~~~

定义没有变化时再次运行，因为设置了 `refresh_on_run=true`，Adapter 会提交一次
`REFRESH MATERIALIZED VIEW ... AUTO`：

~~~bash
dbt run \
  --select monthly_order_summary_mv \
  --project-dir . \
  --profiles-dir .
~~~

刷新是 Doris 异步任务。`dbt run` 成功表示刷新请求已经被 Doris 接受，不表示异步任务
已经完成。通过 Doris 查询最新任务状态：

~~~sql
SELECT Status
FROM tasks('type'='mv')
WHERE MvDatabaseName = 'dbt_demo_daily'
  AND MvName = 'monthly_order_summary_mv'
ORDER BY CreateTime DESC
LIMIT 1;
~~~

等待状态变成 `SUCCESS` 后查询 MV：

~~~sql
SELECT order_month, order_count, total_revenue
FROM dbt_demo_daily.monthly_order_summary_mv;
~~~

预期结果为：

~~~text
2026-08-01 | 3 | 220.20
~~~

#### 1.2.8 检查 Doris 物理对象

执行下面的 SQL 检查 Table DDL：

~~~sql
SHOW CREATE TABLE dbt_demo_daily.daily_order_summary;
~~~

输出中至少应包含：

~~~text
DUPLICATE KEY(`order_date`)
PARTITION BY RANGE(`order_date`)
DISTRIBUTED BY HASH(`order_date`) BUCKETS 1
"replication_num" = "1"
~~~

检查异步物化视图定义和状态：

~~~sql
SHOW CREATE MATERIALIZED VIEW dbt_demo_daily.monthly_order_summary_mv;

SELECT Name, State, RefreshState
FROM mv_infos('database'='dbt_demo_daily')
WHERE Name = 'monthly_order_summary_mv';
~~~

#### 1.2.9 一条命令运行完整 Demo

仓库已经提供上述文件和验证脚本：

~~~bash
cd extension/dbt-doris/examples/data-eng-bench-daily-order-summary
./scripts/run.sh
~~~

如果 Doris FE 使用其他端口或账号，通过环境变量传入：

~~~bash
DORIS_HOST=127.0.0.1 \
DORIS_PORT=19030 \
DORIS_USER=root \
DORIS_PASSWORD='' \
./scripts/run.sh
~~~

脚本依次执行 `setup.sql`、`dbt debug`、两次 Table model 与 Data Test、两次 MV 创建/刷新，
最后检查每日结果、月度结果、Table DDL 和 MV Task。全部正确时输出：

~~~text
Demo verification passed.
~~~

**这个 Demo 需要证明什么**

- dbt profile 能连接 Doris，<code>source()</code> 能从目标 Database 访问独立源 Database；
- Table model 的 DATE、BIGINT 和 DECIMAL 类型正确；
- <code>SHOW CREATE TABLE</code> 中的 Key、分区、分桶和 properties 与 model config 一致；
- 原任务要求的状态过滤、每日唯一性、订单数、收入和 Table 二次运行幂等性全部正确；
- <code>mv_infos()</code> 能看到 MV，初始构建和 <code>refresh_on_run=true</code>
  触发的手动刷新均成功。

### 1.3 Demo 2：客户地域分析

这个 Demo 展示一个完整的 dbt 分层：两个 Doris 源表先变成 staging View，mart
再通过 <code>ref()</code> 合并成 Table。它验证的是跨 Database Source、DAG 依赖和
重复构建。

可运行项目：
[`examples/data-eng-bench-doris-demos/geographic`](../examples/data-eng-bench-doris-demos/geographic/)。

**业务问题**

业务人员希望按州查看客户数、订单数、收入、客单价，以及每个客户平均贡献多少收入和订单。

**输入、输出和 fixture**

| 对象 | 类型 | 作用 |
| --- | --- | --- |
| <code>dbt_demo_geographic_customer.CUSTOMER_ADDRESSES</code> | Doris 源表 | 客户地址、州、是否默认收货地址 |
| <code>dbt_demo_geographic_orders.ORDERS</code> | Doris 源表 | 订单、客户、状态和金额 |
| <code>stg_customer_addresses</code> | View | 只保留默认收货地址，并排除州为空的记录 |
| <code>stg_orders</code> | View | 只保留 COMPLETED、DELIVERED、SHIPPED 订单，并把空金额转成 0 |
| <code>dbt_demo_geographic.fct_state_customers</code> | Table | 形成每州一行的客户和收入指标 |

`scripts/setup.sql` 创建三个专用 Database。fixture 中有三个默认地址客户和四条订单，
其中 TX 地址不是默认地址，且对应订单是 CANCELLED，因此不会进入结果。

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
    A[dbt_demo_geographic_customer.CUSTOMER_ADDRESSES] -->|默认地址且州非空| B[stg_customer_addresses<br/>View]
    C[dbt_demo_geographic_orders.ORDERS] -->|保留有效订单| D[stg_orders<br/>View]
    B -->|按 customer_id 左连接| E[fct_state_customers<br/>Table]
    D --> E
~~~

<code>LEFT JOIN</code> 很重要：某个州即使有客户但暂时没有有效订单，也应该保留，
对应的订单数和收入为 0。

**dbt 项目如何连接 Doris**

`profiles.yml` 使用 `type: doris`，目标 Database 和 schema 都是
`dbt_demo_geographic`，端口由 `DORIS_PORT` 环境变量传入。`models/sources.yml` 把两个
Source 分别指向两个源 Database：

~~~yaml
sources:
  - name: customer_schema
    database: dbt_demo_geographic_customer
    schema: dbt_demo_geographic_customer
    tables:
      - name: CUSTOMER_ADDRESSES
  - name: orders_schema
    database: dbt_demo_geographic_orders
    schema: dbt_demo_geographic_orders
    tables:
      - name: ORDERS
~~~

三个 model 的执行关系是：

1. `stg_customer_addresses.sql` 用 `source('customer_schema', 'CUSTOMER_ADDRESSES')` 创建 View；
2. `stg_orders.sql` 用 `source('orders_schema', 'ORDERS')` 创建 View；
3. `fct_state_customers.sql` 用两个 `ref()` 创建目标 Table，计算客户数、订单数和收入指标。

Mart model 的核心 SQL 是：

~~~sql
{{ config(materialized='table') }}

select
    ca.state_province,
    count(distinct ca.customer_id) as customer_count,
    count(distinct o.order_id) as order_count,
    round(coalesce(sum(o.grand_total), 0), 2) as total_revenue,
    round(coalesce(avg(o.grand_total), 0), 2) as avg_order_value
from {{ ref('stg_customer_addresses') }} ca
left join {{ ref('stg_orders') }} o on ca.customer_id = o.customer_id
group by ca.state_province
~~~

dbt 会先解析两个 `ref()`，创建 staging View 后再创建 mart Table。`models/geographic.yml`
对 `state_province` 和 `customer_count` 执行 `not_null` Data Test。

**完整执行流程**

直接运行仓库脚本：

~~~bash
cd extension/dbt-doris/examples/data-eng-bench-doris-demos/geographic
DORIS_PORT=19030 DBT_BIN=/path/to/dbt ./scripts/run.sh
~~~

`run.sh` 的顺序固定为：

1. 执行 `scripts/setup.sql`，重建三个专用 Database 和两张源表；
2. 执行 `dbt debug`，验证 Profile 和 Doris 连接；
3. 执行第一次 `dbt build`，创建 2 个 View、1 个 Table 和 2 个 `not_null` 测试；
4. 执行第二次 `dbt build`，检查重复构建不会改变结果；
5. `verify.sh` 查询目标 Table，逐行比对结果。

也可以手动执行同样的步骤：

~~~bash
mysql -h 127.0.0.1 -P 19030 -u root --password='' < scripts/setup.sql
dbt debug --project-dir . --profiles-dir .
dbt build --project-dir . --profiles-dir .
dbt build --project-dir . --profiles-dir .
~~~

最终结果：

~~~text
CA | 2 | 2 | 145.00
NY | 1 | 1 |  50.00
~~~

**verify.sh 检查什么**

- `fct_state_customers` 存在且按州输出两行；
- CA 和 NY 的客户数、订单数、总收入与 fixture 计算结果一致；
- 2 个 staging 是 View，mart 是 Table（由 dbt build 执行日志和 Doris relation 类型确认）；
- 第二次 build 后结果仍一致。

**这个 Demo 需要证明什么**

- 一个 dbt project 可以同时读取两个 Doris Database；
- <code>source()</code>、<code>ref()</code>、View 和 Table materialization 能组成正确 DAG；
- Doris 中的源表标识符和目标 schema <code>dbt_demo_geographic</code> 正确；
- mart 每个州只有一行，指标非负且计算公式正确；
- 第二次 <code>dbt build</code> 结果不变。

### 1.4 Demo 3：广告数据标准化和合并

这个 Demo 不依赖预先存在的业务表，而是从三个小型 CSV 开始。它同时展示
<code>dbt deps</code>、Seed、Package、View、Table 和 Data Test，适合作为完整 dbt 工作流示例。

可运行项目：
[`examples/data-eng-bench-doris-demos/consolidate`](../examples/data-eng-bench-doris-demos/consolidate/)。

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

目标 Database 是 `dbt_demo_consolidate`。`dbt_project.yml` 为 model 和 seed 都设置
`replication_num=1`，`packages.yml` 固定使用 `dbt-labs/dbt_utils` 1.3.0，锁文件记录
了依赖 hash。

合并 model 只负责统一追加三个 staging 结果：

~~~sql
{{ config(materialized='table') }}

select 'google' as source, * from {{ ref('stg__ads_googleads') }}
union all
select 'meta' as source, * from {{ ref('stg__ads_metaads') }}
union all
select 'tiktok' as source, * from {{ ref('stg__ads_tiktokads') }}
~~~

最终 Data Test 使用安装后的 `dbt_utils` macro，要求 `source + ad_date` 唯一：

~~~yaml
data_tests:
  - dbt_utils.unique_combination_of_columns:
      arguments:
        combination_of_columns: [source, ad_date]
~~~

三个 CSV 各有两条 2026-08-01/02 数据，Google 和 Meta 额外各有一条完全重复记录。三个
staging View 的 SQL 先统一字段，再用 `QUALIFY row_number()` 保留一条重复记录。三个 View
还使用 `where ad_date is not null`，避免日期为空的广告记录进入业务结果。

**完整执行流程**

~~~bash
cd extension/dbt-doris/examples/data-eng-bench-doris-demos/consolidate
DORIS_PORT=19030 DBT_BIN=/path/to/dbt ./scripts/run.sh
~~~

`run.sh` 的顺序是：

1. 执行 `setup.sql`，重建专用目标 Database；
2. 执行 `dbt debug`；
3. 执行 `dbt deps`，安装并锁定 `dbt_utils` 1.3.0；
4. 执行 `dbt build --select +int__ads_unified`。这个选择器会先运行 3 个 Seed，再运行
   3 个 staging View、1 个 union Table 和 1 个 Data Test；
5. 再次执行同一个 `dbt build`，验证 Seed、View、Table 和测试可重复运行；
6. `verify.sh` 检查目标表总行数为 6，并输出每个渠道的结果。

也可以拆开执行：

~~~bash
dbt deps --project-dir . --profiles-dir .
dbt build --project-dir . --profiles-dir . --select +int__ads_unified
dbt build --project-dir . --profiles-dir . --select +int__ads_unified
~~~

最终结果为 6 行：

~~~text
google | 2026-08-01 | 10 | 100 |  80 | 2
google | 2026-08-02 | 12 | 120 |  90 | 3
meta   | 2026-08-01 |  8 |  90 |  60 | 1
meta   | 2026-08-02 | 15 | 150 | 100 | 4
tiktok | 2026-08-01 |  7 |  70 |  50 | 1
tiktok | 2026-08-02 | 11 | 110 |  80 | 2
~~~

**这个 Demo 需要证明什么**

- Seed 的日期和数值类型可以稳定映射到 Doris；
- <code>QUALIFY</code>、窗口函数和 <code>UNION ALL</code> 在 Doris 上结果正确；
- dbt Package 的安装、macro dispatch 和 Data Test 能正常执行；
- <code>run_results.json</code> 中 model 和 test 均为 success；
- 重复执行 seed/run 后，行数和结果不发生漂移。

这里的 Seed 只用于小数据 Demo；大型业务数据仍应使用 Stream Load 等 Doris 数据导入方式。

### 1.5 Demo 4：迟到订单增量模型

这个 Demo 解释“订单发生时间”和“数据进入系统时间”不一致时，如何使用订单版本和
Incremental `merge` 更新结果。它验证的是 dbt-doris 的 Incremental materialization、
Unique Key 和重复运行路径。

可运行项目：
[`examples/data-eng-bench-doris-demos/incremental`](../examples/data-eng-bench-doris-demos/incremental/)。

**业务问题**

订单可能在 8 月 1 日发生，但直到 8 月 5 日才进入数仓。同一订单还可能在源表中出现新版本。
如果目标表只追加数据，订单 101 会出现两行，日报收入也会重复。

这里有两个关键时间：

- <code>ordered_at</code>：订单实际发生时间；
- <code>created_at</code>：这条记录进入系统的时间。

**七个 model 分别做什么**

| model | materialization | 职责 |
| --- | --- | --- |
| <code>order_version_history</code> | Table | 用窗口函数按 <code>order_id + created_at + event_id</code> 排版本，生成 <code>valid_from</code>、<code>valid_to</code> 和当前版本标记 |
| <code>incremental_daily_sales</code> | Incremental | 只保留当前版本；首次全量，后续只处理 <code>valid_from</code> 不早于目标表最大 <code>created_at</code> 的记录 |
| <code>channel_latency_analysis</code> | Table | 按渠道计算订单数、平均到达延迟和最大到达延迟 |
| <code>revenue_reconciliation_waterfall</code> | Table | 按日把收入分成 1 天内到达和超过 1 天到达两类 |
| <code>late_arrival_metrics</code> | Table | 按订单发生日统计订单数和最大到达延迟 |
| <code>order_data_quality</code> | Table | 标记负金额、空客户和到达时间早于订单日期的记录 |
| <code>daily_sales_summary</code> | Table | 汇总每日订单数、收入、有效订单数和最大到达延迟 |

<code>order_version_history</code> 虽然保存 SCD2 风格字段，但它是普通 Table model，
不是 <code>dbt snapshot</code>。真正的 Snapshot 生命周期由 Demo 5 覆盖。

~~~mermaid
flowchart LR
    S[dbt_demo_incremental_source.ORDERS] --> H[order_version_history<br/>Table]
    H --> I[incremental_daily_sales<br/>Incremental]
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
dbt build --project-dir . --profiles-dir .
~~~

目标表尚不存在，<code>is_incremental()</code> 为 false。dbt 先创建
<code>order_version_history</code>，再创建 Incremental 目标表，最后创建 5 个下游 Table
和 2 个 Data Test。初始 fixture 有订单 101、102、103，目标表也有 3 行。

**后续运行发生什么**

目标表已经存在时，<code>is_incremental()</code> 为 true。目标 Database 是
<code>dbt_demo_incremental</code>，源表是 <code>dbt_demo_incremental_source.ORDERS</code>。
源表使用 <code>DUPLICATE KEY(event_id)</code> 保存事件版本；目标 Incremental model 配置为：

~~~jinja
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    distributed_by=['order_id'],
    buckets=1,
    properties={'replication_num': '1'}
) }}
~~~

脚本向源表增加两个事件：订单 101 的新版本，以及新订单 104：

~~~sql
INSERT INTO dbt_demo_incremental_source.ORDERS VALUES
    (4, 101, 1, 'web', 125.00, 'COMPLETED',
     '2026-08-01 09:00:00', '2026-08-05 09:00:00'),
    (5, 104, 3, 'mobile', 70.00, 'COMPLETED',
     '2026-08-01 12:00:00', '2026-08-05 10:00:00');
~~~

再次运行：

~~~bash
dbt build --project-dir . --profiles-dir .
~~~

此时 <code>is_incremental()</code> 为 true。模型只选取
<code>valid_from</code> 不早于目标表最大 <code>created_at</code> 的新版本和新订单。
<code>merge</code> 把订单 101 的金额从 100.00 更新为 125.00，不留下两个订单 101。

这个条件只在目标表已经存在的增量运行中编译：

~~~jinja
where h.is_current = 1
{% if is_incremental() %}
  and h.valid_from >= (
    select coalesce(max(created_at), cast('1900-01-01' as datetime))
    from {{ this }}
  )
{% endif %}
~~~

一个行为示意：

| 运行 | 源数据变化 | 目标结果 |
| --- | --- | --- |
| 第一次 | 订单 101、102、103 | 目标表 3 行 |
| 第二次 | 101 出现 125.00 的新版本，新增 104=70.00 | 目标表 4 行，101 只有一行 |
| 无数据变化再次运行 | 源表不再增加事件 | 行数和收入保持不变 |

本 Demo 的 model 配置包含 <code>on_schema_change='append_new_columns'</code>，但当前脚本
没有新增模型列；因此本次验证的是 <code>merge</code> 和二次运行，Schema Change 需要另加
一轮专门回归。

最终目标表为：

~~~text
101 | 2026-08-01 | 125.00 | version 2
102 | 2026-08-01 |  50.00 | version 1
103 | 2026-08-02 |  80.00 | version 1
104 | 2026-08-01 |  70.00 | version 1
~~~

<code>daily_sales_summary</code> 中 2026-08-01 的总收入为 245.00。

**一条命令运行和校验**

~~~bash
cd extension/dbt-doris/examples/data-eng-bench-doris-demos/incremental
DORIS_PORT=19030 DBT_BIN=/path/to/dbt ./scripts/run.sh
~~~

<code>verify.sh</code> 检查目标表有 4 个唯一订单、订单 101 为 125.00，以及 8 月 1 日日报
收入为 245.00；它还检查目标表是 `UNIQUE KEY(order_id)`、按 `order_id` Hash 分桶。脚本在
第二次 build 后再执行一次没有新输入的 build，确认行数和收入不变。

**这个 Demo 需要证明什么**

- 首次全量和后续 Incremental 两条编译路径都能执行；
- <code>merge</code> 更新相同 <code>unique_key</code>，不会重复插入；
- 七个 model 的结构和数据公式正确；
- 迟到事件通过 <code>created_at</code> 过滤进入第二次增量；
- 目标表保持唯一订单，日报收入没有重复计算。

### 1.6 Demo 5：Snapshot 和当前客户维表

这个 Demo 展示客户属性发生变化后，Doris 中既能保留旧值，也能方便地查询当前值。
它验证的不是普通 SQL 重建，而是 dbt Snapshot 的跨次运行状态。

可运行项目：
[`examples/data-eng-bench-doris-demos/snapshot`](../examples/data-eng-bench-doris-demos/snapshot/)。

**业务问题**

客户修改邮箱或电话后，直接覆盖原行会丢失历史。Snapshot 要把旧记录关闭，再创建一条新的
当前记录。下游维表只读取当前版本，同时计算客户一共有多少个历史版本。

**三个对象的职责**

| 对象 | 类型 | 职责 |
| --- | --- | --- |
| <code>dbt_demo_snapshot_source.CUSTOMERS</code> | Doris Unique Key 源表 | 保存当前客户属性 |
| <code>stg_customers</code> | View | 从源表提供标准化客户数据 |
| <code>customer_snapshot</code> | dbt Snapshot | 使用 <code>customer_id</code> 作为唯一键，监控邮箱、类型、电话、姓名和公司名 |
| <code>dim_customer_current</code> | Table | 只保留 <code>dbt_valid_to is null</code> 的当前记录，并增加版本统计字段 |

Snapshot 配置使用 <code>strategy='check'</code>、<code>check_cols</code> 和
<code>invalidate_hard_deletes=True</code>。dbt 会维护
<code>dbt_scd_id</code>、<code>dbt_updated_at</code>、
<code>dbt_valid_from</code> 和 <code>dbt_valid_to</code>。

`snapshots/customer_snapshot.sql` 的关键配置是：

~~~jinja
{% snapshot customer_snapshot %}
{{ config(
    target_database='dbt_demo_snapshot_history',
    target_schema='dbt_demo_snapshot_history',
    unique_key='customer_id',
    distributed_by=['customer_id'],
    buckets=1,
    replication_num='1',
    strategy='check',
    check_cols=['email', 'customer_type', 'phone_primary',
                'first_name', 'last_name', 'company_name'],
    invalidate_hard_deletes=True
) }}

select * from {{ ref('stg_customers') }}
{% endsnapshot %}
~~~

下游 `dim_customer_current` 只选择 `dbt_valid_to is null` 的行，再按 `customer_id` 统计
历史版本数。因此 Snapshot 表保留历史，普通 Table model 提供当前业务视图。

当前 Demo 的 Snapshot history Database 是 <code>dbt_demo_snapshot_history</code>，当前维表
Database 是 <code>dbt_demo_snapshot</code>。

**一次客户变更后的结果**

| customer_id | email | dbt_valid_from | dbt_valid_to | 含义 |
| --- | --- | --- | --- | --- |
| 1 | alice@example.com | T1 | T2 | 已关闭的旧版本 |
| 1 | alice.new@example.com | T2 | NULL | 当前版本 |

如果客户从源表删除，<code>invalidate_hard_deletes=True</code> 应关闭其当前版本，而不是继续
把它当作有效客户。

**model 依赖和执行顺序**

~~~mermaid
flowchart LR
    A[dbt_demo_snapshot_source.CUSTOMERS] --> B[stg_customers<br/>View]
    B --> C[customer_snapshot<br/>SCD Type 2]
    C --> D[dim_customer_current<br/>Table]
~~~

~~~bash
dbt run --project-dir . --profiles-dir . --select stg_customers
dbt snapshot --project-dir . --profiles-dir . --select customer_snapshot --threads 1
dbt run --project-dir . --profiles-dir . --select dim_customer_current
dbt test --project-dir . --profiles-dir . --select dim_customer_current --threads 1
~~~

1. <code>setup.sql</code> 创建源表、目标 Database 和 Snapshot history Database；源表使用
   <code>UNIQUE KEY(customer_id)</code>，允许后续 UPDATE 和 DELETE。
2. <code>dbt debug</code> 验证 Doris 连接，<code>dbt run</code> 创建
   <code>stg_customers</code> View。
3. 第一次 <code>dbt snapshot</code> 创建 history 表，写入两个当前客户版本。
4. <code>dbt run</code> 创建 <code>dim_customer_current</code>，只选择
   <code>dbt_valid_to is null</code> 的版本，并计算 <code>total_versions</code> 和
   <code>has_history</code>。
5. <code>dbt test</code> 执行当前维表的 not-null、unique 和 accepted-values 测试。

**怎样验证第二次运行**

1. 修改客户 1 的 email 和 customer_type，并从源表删除客户 2：

~~~sql
UPDATE dbt_demo_snapshot_source.CUSTOMERS
SET email = 'alice.new@example.com', customer_type = 'INDIVIDUAL_PLUS'
WHERE customer_id = 1;

DELETE FROM dbt_demo_snapshot_source.CUSTOMERS
WHERE customer_id = 2;
~~~

2. 第二次执行 <code>dbt snapshot</code>、<code>dbt run --select dim_customer_current</code>
   和 <code>dbt test</code>。
3. Snapshot 应关闭客户 1 的旧版本并插入新版本；客户 2 的当前版本应被关闭。
4. <code>dim_customer_current</code> 只剩客户 1，且 <code>total_versions=2</code>、
   <code>has_history=1</code>。

仓库脚本把上述步骤封装为：

~~~bash
cd extension/dbt-doris/examples/data-eng-bench-doris-demos/snapshot
DORIS_PORT=19030 DBT_BIN=/path/to/dbt ./scripts/run.sh
~~~

最终 history 表为 3 行：客户 1 两个版本，客户 2 一个已关闭版本；当前维表为 1 行。
<code>verify.sh</code> 检查总行数、客户 1 的旧/当前版本、客户 2 已关闭且没有当前版本，
以及当前维表只保留 `alice.new@example.com`。

这个 Demo 最终证明 Snapshot helper relation、SCD 元数据、更新/删除语义、下游
<code>ref()</code> 和重复运行能够在 Doris 上协同工作。当前配置使用
<code>strategy='check'</code>、<code>invalidate_hard_deletes=True</code>，没有启用
<code>hard_deletes='new_record'</code>，因此删除记录通过关闭原版本表示。

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

2026-08-19 在本地单 FE/单 BE Doris 集群上，用当前 checkout 的 `dbt-doris` 1.0.0、
dbt Core 1.12.2 和 `DORIS_PORT=19030` 从仓库中的五个脚本目录重新执行，结果如下：

| Demo | 执行内容 | 结果 |
| --- | --- | --- |
| 每日订单 | 1 个 Table、4 个 Data Test、2 次 `dbt build`、Async MV 创建和重复运行 | 通过；3 个有效订单，按日收入 100.00/80.00/40.20，月收入 220.20，MV Task 成功 |
| 客户地域 | 2 个 View、1 个 Table、2 次 `dbt build`、2 个 Data Test | 通过；CA=2 客户/2 订单/145.00，NY=1/1/50.00 |
| 广告合并 | 3 个 Seed、3 个 staging View、1 个 union Table、`dbt_utils` 唯一性测试、2 次 build | 通过；去重后 6 行 |
| 迟到订单 | 版本历史 Table、Incremental `merge`、5 个下游 Table、2 个 Data Test、源数据更新后增量 build，再做一次无输入变化 build | 通过；4 个唯一订单，订单 101 更新为 125.00，8 月 1 日收入 245.00 |
| 客户 Snapshot | staging View、Snapshot 首轮、当前维表、3 个 Data Test；修改客户 1 并删除客户 2 后再次 snapshot | 通过；3 条历史记录，客户 1 一条关闭旧版本和一条当前版本，客户 2 无当前版本 |

每个脚本都在开始时重建自己的专用 fixture database，`verify.sh` 再直接查询 Doris
结果表。五个 Demo 彼此不读取对方的 Database，可以按任意顺序独立执行。
