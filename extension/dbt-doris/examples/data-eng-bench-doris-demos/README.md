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

## 在 Mac 浏览器中运行 Jupyter Notebook

[`dbt-for-apache-doris-data-eng-bench-demos.ipynb`](dbt-for-apache-doris-data-eng-bench-demos.ipynb)
把每日订单 Demo 和本目录中的四个 Demo 组织成 5 个可依次执行的单元格。项目和 Doris
位于远程服务器时，Jupyter kernel 也应运行在服务器上，Mac 只负责显示浏览器界面。

不要把示例中的 `/path/to/dbt` 原样设置为 `DBT_BIN`。先在服务器的 Doris 仓库根目录创建
稳定的 Python 环境并安装当前 checkout：

```bash
python3 -m venv .venv-dbt-doris-demo
source .venv-dbt-doris-demo/bin/activate
python -m pip install -e extension/dbt-doris jupyterlab

export DBT_BIN="$PWD/.venv-dbt-doris-demo/bin/dbt"
export DORIS_HOST=127.0.0.1
export DORIS_PORT=9030
export JUPYTER_PORT=18888

extension/dbt-doris/examples/data-eng-bench-doris-demos/scripts/start-notebook.sh
```

启动脚本优先使用服务器已有的 `jupyter-lab`；没有时使用 `uvx` 临时提供 JupyterLab。
服务只监听服务器的 `127.0.0.1`，不要改成 `0.0.0.0`，也不需要在云防火墙开放 18888。

保持服务器进程运行，在 **Mac 的另一个终端**执行 SSH 隧道。`<服务器 SSH 地址>` 使用平时
登录这台服务器的主机名、IP 或 `~/.ssh/config` 别名：

```bash
ssh -N -L 18888:127.0.0.1:18888 chenjunwei@<服务器 SSH 地址>
```

最后在 Mac 浏览器打开服务器日志输出的 URL。因为隧道两端都使用 18888，URL 形式是：

```text
http://127.0.0.1:18888/lab?token=<服务器输出的 token>
```

打开后先执行环境检查，再按顺序点击 5 个 Demo 单元格左侧的运行按钮。每格下方会显示
执行过程表、运行状态、耗时、dbt 成功节点数和 Doris 查询表格；完整 dbt 日志默认折叠在“查看完整运行日志”
中，需要排错时再展开。也可以直接使用 Jupyter 的 **Run All**。
