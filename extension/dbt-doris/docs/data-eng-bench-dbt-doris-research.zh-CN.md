# data-eng-bench 的 dbt 任务抽取与 Doris 验证

> 本文只讨论将 `data-eng-bench` 改造成 dbt-doris 场景验证集。重点是整理 103 个 dbt 任务、记录每个任务的执行方式，并用 Doris 验证 dbt-doris 的能力。Agent 属于可选增强。

## 1. 项目目标

`data-eng-bench` 的每个任务都包含数据工程需求、dbt 工程操作和 verifier。我们从这些任务中抽取 Doris 场景，形成 dbt-doris 的真实回归集。

每个任务需要记录：

```text
源表和依赖关系
dbt project 和执行命令
目标模型、schema 和 materialization
Doris SQL 或表设计改动
verifier 的结构和数据断言
dbt-doris 当前支持状态
```

交付物： [103 个任务的 Doris 迁移清单与发布 Demo](data-eng-bench-103-tasks-statistics.zh-CN.md)、最小 dbt project、Doris fixture/loader、统一执行入口、Doris verifier、能力覆盖矩阵和可运行 Demo。

## 2. Doris variant

原始任务使用 DuckDB 或 Snowflake。Doris 版本保留原任务，增加独立 variant，例如：

```text
tasks/dbt-daily-order-summary/
tasks/dbt-daily-order-summary-doris/
```

variant 记录 Doris 的 profile、fixture、SQL、表设计和 verifier 差异，不覆盖原始任务。

## 3. 任务卡格式

每个任务建立一份任务卡或 manifest：

| 字段 | 内容 |
| --- | --- |
| `task` | 原任务和 Doris variant 名称 |
| `business_goal` | 任务生成的业务结果 |
| `sources` | 源表、database、schema 和列 |
| `refs` | dbt `ref` 关系 |
| `models` | 模型及目标 schema |
| `materialization` | view、table、incremental、snapshot |
| `dbt_commands` | deps、seed、run、test、build 命令 |
| `doris_config` | key、分桶、分区、replication、properties |
| `fixture_mode` | 原始表、预物化 staging、Unique Key 表 |
| `verifier` | 结构、数据、类型和幂等性检查 |
| `dialect_changes` | DuckDB SQL 到 Doris SQL 的改动 |
| `status` | 未开始、适配中、参考解法通过、完整验证 |
| `failure_reason` | 失败原因和关联能力 |

任务卡记录任务对 dbt-doris 的能力要求，不把参考答案直接当作能力结论。

## 4. Doris 执行流程

所有任务使用同一套生命周期：

```text
创建干净 Doris FE/BE
  -> 创建任务 database/schema
  -> 导出并装载任务源表
  -> 创建最小 dbt project 和 profiles.yml
  -> 执行 dbt deps/seed/run/test/build
  -> 运行 Doris verifier
  -> 保存日志、compiled SQL、run_results 和 verifier 结果
  -> 清理 database 和容器
```

任务不得依赖上一个任务留下的 Doris 表。DuckDB 文件不能直接作为 Doris 数据库，loader 需要完成类型映射、Doris DDL、Stream Load、行数和 schema 校验。大型源表使用 Stream Load，需要 `UPDATE` 的任务使用 Unique Key 表。

实验环境使用独立的 dbt Core 和 Doris adapter，profile 由 runner 注入：

```yaml
retail:
  target: doris
  outputs:
    doris:
      type: doris
      host: doris
      port: 9030
      user: root
      password: ""
      database: main
      schema: analytics
      threads: 2
```

任务卡记录真实命令，例如 `dbt deps --project-dir /app/dbt_project` 和 `dbt build --project-dir /app/dbt_project --select daily_order_summary`。需要幂等性验证的任务执行两次 build，需要数据变更验证的任务在两次运行之间更新源表。

## 5. 任务分类

### 基础模型

能力：source、ref、join、group by、cast、table/view。

任务：`dbt-daily-order-summary`、`dbt-test-orders-filter`、`dbt-fraud-detection-model`、`dbt-rfm-customer-segmentation`。

### 日期、窗口和统计

能力：日期截断、日期差、interval、窗口函数、percentile、排名和 NULL。

任务：`cohort-retention-matrix`、`dbt-fix-daily-cohorts`、`dbt-fix-refund-reconciliation`、`dbt-customer-churn-cohorts`、`dbt-inventory-turnover-analysis`。

### dbt 生命周期

能力：incremental、snapshot、schema change、二次运行和失败恢复。

### Doris 表设计

能力：Unique Key、Duplicate Key、分区、分桶、replication 和 properties。需要同时检查 model config 和 `SHOW CREATE TABLE`。

### 复杂 DAG 和方言

能力：递归 CTE、复杂 macro、正则、字符串连接、多层 staging/intermediate。代表任务：`marketing-campaigns-harbor`、`workforce-analytics`、`deferred-revenue-recognition`、`dbt-supplier-payment-optimization`。

## 6. 代表任务卡

### `dbt-daily-order-summary`

| 项目 | 内容 |
| --- | --- |
| 源表 | `ORDERS.ORDERS` |
| 目标模型 | `daily_order_summary` |
| materialization | table |
| SQL | 日期转换、过滤、聚合和金额计算 |
| verifier | 列、行数、日期唯一性、收入和幂等性 |
| 用途 | 最小 project、profile、table 和基础类型验证 |

### `dbt-fix-daily-cohorts`

| 项目 | 内容 |
| --- | --- |
| 源关系 | `main.stg_orders__orders` |
| 目标模型 | `rpt_daily_cohorts` |
| 主要差异 | 三参数 `date_diff` 改成 `datediff(end, start)` |
| 用途 | 日期函数、ref 解析和共享 staging fixture 验证 |

### `fifo-inventory-cogs`

| 项目 | 内容 |
| --- | --- |
| 源表 | inventory、product 相关表 |
| 目标模型 | FIFO 分配、月度 COGS、期末库存 |
| 主要差异 | 日期函数和 `LEAST/GREATEST(NULL, x)` 语义 |
| 用途 | 复杂窗口、NULL 语义和 Doris expected result 验证 |

### `dbt-receivables-aging-buckets`

| 项目 | 内容 |
| --- | --- |
| 源表 | finance 下四张可更新表 |
| 表设计 | 源表需要 Unique Key |
| 主要差异 | staging 必须保持对源表的实时 view |
| 用途 | UPDATE、二次运行和数据变更可见性验证 |

### `dbt-supplier-payment-optimization`

| 项目 | 内容 |
| --- | --- |
| 源表 | supplier invoices、suppliers、currency exchange rates |
| 主要差异 | 正则空值转换、lateral join 和日期运算 |
| 状态 | 方言和类型边界任务，适配中 |

## 7. dbt-doris 能力矩阵

| 能力 | 任务证据 | 验证方式 |
| --- | --- | --- |
| 连接和 profile | `dbt debug`、最小 model | Doris 集成测试 |
| source/ref | manifest 和依赖关系 | 最小 project + verifier |
| view/table | 对象和 DDL | materialization 测试 |
| schema tests | `dbt test` 结果 | verifier + dbt test |
| 类型映射 | information_schema.columns | 列类型断言 |
| incremental | 二次运行、新增数据 | Unique Key/增量测试 |
| snapshot | helper 表和历史版本 | snapshot 任务 |
| schema change | 新列或类型变化 | on-schema-change 任务 |
| 分区/分桶/key | `SHOW CREATE TABLE` | Doris DDL verifier |
| NULL/日期语义 | 独立 expected result | 结果级回归测试 |
| dbt_utils | macro 编译和执行 | package 兼容任务 |

一个任务通过，只说明该任务覆盖的能力通过；能力结论需要合并多个任务的证据。

## 8. Doris SQL 适配重点

| DuckDB 写法 | Doris 适配方向 |
| --- | --- |
| `strftime(...)` | `date_format(...)` |
| `date_diff('day', a, b)` | `datediff(b, a)` |
| `expr::type` | `cast(expr as type)` |
| `percentile_cont` | `percentile` 或 `percentile_approx`，按任务语义验证 |
| 日期直接相减 | 显式 `datediff` |
| `LEAST/GREATEST(NULL, x)` | 用 `CASE` 明确 NULL 语义 |

这些改动不能全部使用无条件文本替换，需要通过 Doris 执行和 verifier 检查结果语义。

## 9. Verifier 要求

Verifier 必须同时检查 dbt 过程和 Doris 结果：

```text
删除旧目标
  -> 执行指定 dbt build/test
  -> 检查 manifest/run_results
  -> 检查 information_schema 和 SHOW CREATE TABLE
  -> 独立计算期望数据
  -> 检查二次运行或数据变更行为
```

至少检查目标对象确实由 dbt model 生成、materialization/schema/列类型正确、结果数据正确、schema tests 通过，以及任务要求的 incremental、snapshot、UPDATE 或 schema change 行为。

## 10. Agent 的位置

Agent 建立在已经定义好的 Doris 任务之上：任务卡和 Doris 环境准备好后，Agent 修改 dbt model、执行 `dbt build/test`，再由同一 verifier 检查结果。

Agent 不是任务抽取、数据装载或 verifier 的替代品。参考解法通过后，再记录 Agent result、verifier result、reward 和 trajectory。

## 11. 当前状态和实施顺序

已完成或已验证：

- Doris 4.0.3、dbt Core 1.12.2、dbt-for-apache-doris 1.1.0 实验组合；
- Doris profile、连接、Stream Load 和最小 dbt project；
- 多个基础任务的参考解法和 verifier 执行；
- FIFO、receivables、POS、supplier 等复杂任务的兼容问题定位；
- fast-30 的 Doris clean-room 参考解法回归：30/30 任务、869/869 verifier、30/30 reward；
- 生成器可渲染全部 30 个 Doris Agent 变体，并完成若干 Oracle smoke；
- 四个真实 Codex Doris trial 通过：两个独立 tracer，以及 fast-30 中的
  `dbt-fix-cac-payback-waterfall`、`dbt-fix-daily-cohorts`。

上述 fast-30 和 Codex 数字是本地实验记录。当前工作树没有保留对应的 runner、task manifest、
逐题 result 和日志，因此不能直接作为发布验收证据。发布前需要按本文定义的干净环境重新执行，
并归档版本、命令、dbt artifacts、verifier 输出和 reward。

尚未完成：103 个任务的完整 Doris 任务卡、103 个任务的 clean-room 参考解法回归，以及全部 30 个 fast-30 Doris 变体的真实 Agent 评测。当前 fast-30 的 30/30 结果是参考解法兼容性基线，不是 Agent accuracy。

实施顺序：

1. 固定版本并发布 Doris base image；
2. 完成 `dbt-daily-order-summary` 任务卡、fixture、最小 project、loader 和 verifier；
3. 批量抽取其余任务的 source、ref、model 和命令；
4. 按任务分类迁移并记录 SQL、表设计和 verifier 差异；
5. 将可复现失败转成 dbt-doris 单元测试或集成测试；
6. 建立能力矩阵和任务状态报告；
7. 参考解法稳定后，为代表任务增加 Agent 入口；
8. 在已完成 fast-30 clean-room 基线的基础上，串行运行真实 Agent 变体，再评估 103 个任务的迁移范围。

## 12. 资料

- [data-eng-bench](https://github.com/Snowflake-Labs/data-eng-bench)
- [Apache Doris dbt adapter 文档](https://doris.apache.org/docs/4.x/connection-integration/data-integration/dbt-doris-adapter/)
- [Apache Doris dbt adapter 源码](https://github.com/apache/doris/tree/master/extension/dbt-doris)
- [dbt adapter 创建指南](https://docs.getdbt.com/guides/adapter-creation)
- [Doris Stream Load](https://doris.apache.org/docs/4.x/key-features/stream-load/)
- [Harbor 核心概念](https://www.harborframework.com/docs/core-concepts)
- [Harbor task 与 reward](https://www.harborframework.com/docs/tasks)
