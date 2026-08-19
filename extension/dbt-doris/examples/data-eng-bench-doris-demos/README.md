# data-eng-bench Doris demos

这组示例把 `data-eng-bench` 中的四类 dbt 行为改成可独立运行的 Doris 项目：

| 目录 | 演示内容 | 主要 dbt 对象 |
| --- | --- | --- |
| `geographic` | 客户地址与订单关联，按州汇总 | Source、View、Table、`ref()`、Data Test |
| `consolidate` | 三个广告平台 CSV 去重后合并 | Seed、`dbt_utils`、`QUALIFY`、Table |
| `incremental` | 迟到数据和订单版本更新 | Window、Incremental `merge`、Unique Key、Data Test |
| `snapshot` | 客户属性变更和删除的历史追踪 | Snapshot、SCD Type 2、当前维表 |

每个目录都有 `scripts/setup.sql`、`scripts/run.sh` 和 `scripts/verify.sh`。脚本使用专用
Doris database，不依赖其他 demo 的表。

## 前提

- Doris FE 的 MySQL 端口可访问，默认 `127.0.0.1:9030`；
- 已安装当前 checkout 的 `dbt-doris` adapter；
- 已安装 `mysql` 客户端；
- `DBT_BIN` 指向 dbt Core CLI。需要换端口时设置 `DORIS_PORT`。

例如当前开发集群使用 19030：

```bash
export DBT_BIN=/path/to/dbt
export DORIS_PORT=19030
cd extension/dbt-doris/examples/data-eng-bench-doris-demos
./geographic/scripts/run.sh
```

四个 demo 的 `run.sh` 都会重建自己名字空间中的 fixture、执行 dbt，并在最后调用
`verify.sh` 检查 Doris 结果，因此可以按任意顺序独立执行。
