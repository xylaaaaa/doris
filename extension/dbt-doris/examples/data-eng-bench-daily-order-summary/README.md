# data-eng-bench 每日订单汇总 Doris Demo

这个示例来自 `data-eng-bench` 的 `dbt-daily-order-summary` 任务。它用一张订单源表
创建每日订单汇总 Table，并在其上创建一个 Doris 异步物化视图。

执行后会创建：

```text
dbt_demo_daily_source.orders                 源订单表
dbt_demo_daily.daily_order_summary           dbt Table model
dbt_demo_daily.monthly_order_summary_mv      Doris Async Materialized View
```

`scripts/setup.sql` 只会删除和重建 `dbt_demo_daily`、`dbt_demo_daily_source` 两个专用
Demo database，请勿将它们用于业务数据。

## 前提

- 已启动 Doris FE，MySQL 端口可访问；
- 已安装当前 checkout 的 adapter：`cd extension/dbt-doris && pip install .`；
- 可使用 `mysql` 客户端连接 Doris。

## 运行

默认连接本机 `127.0.0.1:9030`。本 checkout 的开发集群使用 19030：

```bash
cd extension/dbt-doris/examples/data-eng-bench-daily-order-summary
DORIS_PORT=19030 ./scripts/run.sh
```

可以用 `DBT_BIN` 指向特定的 Python dbt CLI，例如：

```bash
DORIS_PORT=19030 DBT_BIN=/path/to/venv/bin/dbt ./scripts/run.sh
```

脚本会依次初始化 fixture、执行 `dbt debug`、创建 Table 和四项 data test、创建 MV、
再次选中 MV 提交刷新，然后校验数据、Doris DDL 和最近的 MV Task 状态。

预期每日结果：

| order_date | order_count | total_revenue |
| --- | ---: | ---: |
| 2026-08-01 | 1 | 100.00 |
| 2026-08-02 | 1 | 80.00 |
| 2026-08-03 | 1 | 40.20 |
