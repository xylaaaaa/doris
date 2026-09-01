# dbt 与 dbt-doris：概念、应用与演进方向

## 1. 先用一句话说清楚

> Doris 负责存储和计算数据，dbt 负责把数据转换组织成一个工程，
> dbt-doris 负责让 dbt 能够按 Doris 的方式工作。

放到常见数据链路里看：

```text
MySQL / Kafka
      |
Flink CDC / DataX / Routine Load
      |
      v
Doris 原始数据层
      |
dbt + dbt-doris
      |
      v
Doris 明细层、维度层、汇总层
      |
      v
BI 报表、指标平台、数据服务
```

这里有三个容易混淆的角色：

| 角色 | 主要负责什么 |
| --- | --- |
| Doris | 保存数据，真正执行 SQL |
| dbt | 管理转换模型、依赖、测试和文档 |
| dbt-doris | 把 dbt 的通用动作翻译成 Doris 的连接、建表和写入行为 |

原始数据怎么进入 Doris，通常还是 Flink、DataX 或 Routine Load 的工作。
任务几点运行、失败后怎么重试，通常还是 Airflow 或 DolphinScheduler 的工作。

所以 dbt 既不是数据同步工具，也不是 Doris 的替代品。

## 2. dbt 到底是什么

### 2.1 从四段 SQL 说起

假设要做一张销售日报，最开始可能只有四段 SQL：

- 清洗订单；
- 关联客户；
- 按天汇总销售额；
- 检查日报结果。

只有四段时，手工执行也没问题。

但当它变成四百段 SQL，问题就不只是“SQL 会不会写”了：

- 应该先跑哪一段；
- 改了订单清洗，会影响哪些报表；
- 开发环境和生产环境怎么隔离；
- 订单 ID 重复了，谁能及时发现；
- 一张表代表什么，应该问谁；
- 哪些修改经过了评审和测试。

dbt 解决的就是这些 SQL 周围的工程问题。

> dbt 是面向数据转换的项目管理和构建工具。

开发者仍然主要写 SQL，但这些 SQL 不再是一堆散落的脚本，
而是有名称、有依赖、有测试、有说明的 Model。

### 2.2 用订单例子理解 Model 和依赖

订单项目里可以有这些 Model：

```text
原始订单
   |
   v
清洗后的订单
   |
   +------> 订单事实表
   |
   +------> 每日销售汇总
   |
   +------> 客户购买汇总
```

“每日销售汇总”声明自己依赖“清洗后的订单”。

dbt 因而知道：

- 构建日报前，要先有清洗结果；
- 清洗逻辑变化后，哪些下游可能受影响；
- 只想验证日报时，需要选择哪一段依赖链；
- 文档中应该怎样画出上下游关系。

这就是 dbt 中 `ref()` 的核心价值：

> `ref()` 既是引用，也是依赖关系。

SQL 还是 SQL，但开发方式已经从“维护散落的脚本”，
变成了“维护有依赖、有测试、能评审的数据产品”。

## 3. dbt 的核心板块

dbt 的核心板块及其作用如下：

| 板块 | 它是干什么的 | 订单例子 |
| --- | --- | --- |
| Source | 声明已经存在的输入数据 | Flink 写入的原始订单表 |
| Model | 描述一层数据转换 | 清洗订单、生成销售日报 |
| `ref()` | 引用另一个 Model，并建立依赖 | 日报依赖清洗订单 |
| Config | 声明 Model 应该怎样构建 | 建成 Table，并指定 Doris Key、分区和分桶 |
| Materialization | 决定 Model 以什么形态存在 | 视图、表或增量表 |
| Data Test | 检查数据是否符合规则 | 订单 ID 唯一、金额非空 |
| Seed | 管理小型静态数据 | 渠道码和渠道名称的映射 |
| Snapshot | 独立保存一条记录的历史变化 | 保留客户等级每次变化 |
| Docs / Lineage | 说明模型含义和上下游 | 查看哪些报表依赖订单表 |

其中有几个概念容易混淆。

### 3.1 Source 和 Model

Source 是“数据已经在那里，但不是这个 dbt 项目创建的”。
例如，Flink 写入的原始订单就是 Source。

Model 是“这个结果由 dbt 项目负责构建”。
例如，清洗订单、销售日报和客户购买汇总都是 Model。

一句话区分：

> Source 是项目的输入，Model 是项目负责生产的数据结果。

### 3.2 Config 决定怎样构建 Model

Model 中的 `SELECT` 决定“数据内容是什么”，Config 决定“这个结果怎样构建”。

例如：

```sql
{{ config(
    materialized='table',
    duplicate_key=['order_id'],
    distributed_by=['order_id'],
    buckets=16
) }}

select *
from {{ ref('stg_orders') }}
```

其中：

- `materialized='table'` 是 dbt 通用配置，表示把结果建成 Table；
- `duplicate_key`、`distributed_by` 和 `buckets` 是 Doris 专用配置；
- dbt Core 读取 Config 并选择构建方式；
- dbt-doris 把 Doris 专用 Config 转换成对应的 Doris DDL。

Config 可以直接写在 Model 的 `config()` 中，也可以在 `dbt_project.yml`
中按项目或目录统一设置。它主要决定构建行为，不负责定义 Model 的数据内容，
也不单独决定本次命令选择哪些 Model。

### 3.3 Model 最后以什么形态存在

Materialization 决定一个 Model 构建后是什么：

| 方式 | 通俗理解 | 适合的例子 |
| --- | --- | --- |
| View | 只保存查询逻辑 | 轻量的订单字段清洗 |
| Table | 完整生成一张表 | 经常被查询的销售汇总 |
| Incremental | 每次只处理新增或变化部分 | 持续增长的订单事实表 |
| Ephemeral | 不单独落到 Doris，而是嵌入下游查询 | 只为复用的一小段中间逻辑 |

Snapshot 不在这张 Model 物化选择表里。
它是一类独立资源，专门保存记录随时间发生的变化。

例如客户当前是金卡会员，普通业务表可能只保留“金卡”。
Snapshot 可以继续回答：这个客户什么时候是普通会员，什么时候升级为银卡，
又是什么时候升级为金卡。

### 3.4 Test、Seed 和文档

Data Test 把业务规则变成自动检查，例如订单 ID 不能重复、
金额不能为负数、订单必须关联到有效客户。

Seed 适合很小、变化不频繁的 CSV 数据，例如销售渠道映射；
它不适合替代 Flink 或 DataX 同步大规模业务数据。

Docs 和 Lineage 把模型说明、测试和上下游组织起来。
新人可以先看“日报来自哪里、又被谁使用”，快速理解项目。

## 4. dbt-doris 做什么

dbt-doris 是面向 Python dbt Core v1 的 Doris Adapter：

```text
用户定义 Model 和依赖
          |
          v
dbt 解析项目并决定要做什么
          |
          v
dbt-doris 翻译成 Doris 行为
          |
          v
Doris 执行 SQL
```

当前 `master` 工作区的包版本元数据为 `1.0.0`，依赖声明为
`dbt-core~=1.12.0`。下面的“当前能力”以这个工作区为准，
不等同于 PyPI 上某个已经发布版本的固定能力。

状态含义：

- ✅ 已实现：当前 Adapter 已有对应实现；
- 🟡 部分实现：主路径存在，但语义、覆盖范围或可靠性还不完整；
- 🔵 dbt Core：不需要 Doris 专用实现，当前项目可以直接使用；
- ❌ 未实现：Doris 数据库可能具备底层能力，但 dbt-doris 还没有提供接口。

当前工作区最近一次回归使用 Python 3.11、dbt-core 1.12.0 和本地单 FE/
单 BE Doris；无集群单元测试和 43 项 Functional 套件测试全部通过；
后者包含 37 项真实 Doris 测试和 6 项不连接数据库的 Relation 测试。
下面的状态仍按“产品语义是否完整”判断：测试通过一条主路径，不等于异常恢复、
版本矩阵和全部边界都已经验收。

这里把“功能状态”和“验证证据”分开写。证据来自当前工作区的
[Python 适配层](../dbt/adapters/doris/)、[Doris 宏](../dbt/include/doris/macros/)、
[Functional 测试](../test/functional/adapter/)和[单元测试](../test/unit/)；
完整命令、环境和已知问题见[测试记录](dbt-doris-test-results-and-known-issues.zh-CN.md)。
“实测”表示本轮在真实 Doris 上通过，“单测”表示不连接集群的可执行测试，
“代码”表示只做了源码与调用路径审查，包括确认实现存在或缺失，
“Core”表示主要由 dbt Core 提供。括号里的“主路径”“基础”等字样就是验证边界。

### 4.1 dbt v1 和 v2 有什么区别

这里的 v1、v2 指 dbt Framework 的两代运行时，不是 dbt 项目 YAML 的格式版本。
截至 2026-07-27，dbt v2 已经发布：dbt Core 2.0 和 dbt Fusion 是建立在同一套
Rust Engine 上的两个发行版；Core 2.0 是 Apache 2.0 开源发行版，Fusion 在其上
增加了额外能力。官方说明见 [About Fusion](https://docs.getdbt.com/docs/fusion/about-fusion)
和 [dbt Licensing FAQ](https://www.getdbt.com/licenses-faq)。

| 对比项 | dbt Core v1.x | dbt v2 |
| --- | --- | --- |
| 执行引擎 | Python 实现，通过 Python 包运行 | Rust 重写的统一引擎；Core 2.0 和 Fusion 共用基础运行时 |
| Adapter 形态 | 安装 Python Adapter；由 Python 连接管理器、数据库 Driver 和 Jinja Macro 共同适配数据库 | 使用新的 Adapter、数据库 Driver 和 SQL Dialect 组件；连接层以 Arrow/ADBC 等组件为主 |
| 项目代码 | Model、`ref()`、`source()`、Test、Macro 和 YAML | 保留主要 dbt 项目语言和目录结构，但校验更严格，已废弃或依赖 Python 内部实现的行为可能不兼容 |
| SQL 理解 | 主要负责渲染 SQL，很多语法和类型错误要到数据库执行时才能发现 | Engine 原生理解多种 SQL Dialect，可在执行前做语法、类型和影响分析 |
| 开发体验 | Parse、Compile、Docs 和血缘以 Python Core 能力为主 | 新 Engine 提供更快的解析和更严格的校验；Fusion 发行版及其编辑器还提供 LSP、实时错误、精确列级血缘和 Docs v2，部分能力需要登录 |
| 运行依赖 | 依赖 Python、`dbt-core`、Adapter 包和数据库 Connector 的版本组合 | 以编译后的二进制运行；Fusion 可管理其支持的 Driver 和依赖 |
| Doris 当前状态 | ✅ 当前 dbt-doris 1.0.0 就是这一类 Python Adapter；开发、发布依赖和完整回归均以 dbt-core 1.12.x 为基线 | ❌ 官方 v2 Adapter 列表还没有 Doris，当前 Python dbt-doris 不能被 v2 直接加载 |

官方 v2 可用性页面目前列出 Snowflake、BigQuery、Databricks、Redshift、
Spark 和 DuckDB 等 Adapter，没有 Doris，见
[Fusion availability](https://docs.getdbt.com/docs/fusion/fusion-availability)。
项目兼容边界和比 v1 更严格的校验见
[Supported features](https://docs.getdbt.com/docs/fusion/supported-features)。
从 v1 到 v2 需要为 Doris 补 Driver、Adapter 组件、SQL Dialect、
认证和兼容测试。

所以本文第 4 节的现状和已通过测试都属于 dbt Core v1 路线；
第 7 节会把“继续补齐 v1 Adapter”和“验证 v2 Adapter”拆成两条工作流。

### 4.2 为什么从 dbt Core 1.10 升级到 1.12

1.10 和 1.12 都属于 Python 实现的 dbt Core v1。dbt-doris 继续开发 v1
Adapter，后续基线采用仍在维护的最新 Minor 版本。

| 对比项 | dbt Core 1.10 | dbt Core 1.12 |
| --- | --- | --- |
| 生命周期 | 已进入 Deprecated，官方维护结束 | 处于 Active Support，支持到 2027-07-15 |
| Python 要求 | 仍支持 Python 3.9 | 要求 Python 3.10 及以上 |
| 框架能力 | 现有 v1 项目和 Adapter 的历史验证基线 | 增加可选的 v2 Parser、UDF Resource 和新的 Catalog Specification，并改进错误处理和项目校验 |
| 行为变化 | 使用较早的默认行为 | 一批 Behavior Flag 进入新默认行为，对 Macro 参数、Hook 失败和 Microbatch 等场景的检查更严格 |
| dbt-doris 用法 | 保留已有测试结果作为历史基线 | 作为后续开发、CI 和发布兼容验证的主版本 |

dbt Core 承诺 1.x 内已文档化的用户项目行为保持向后兼容，因此 1.12
通常可以运行原来面向 1.10 编写的项目；使用了 1.12 新能力的项目则不能保证
回到 1.10 运行。Adapter 使用的 Python 接口可能随 Minor 版本调整，
所以 dbt-doris 仍需在 1.12 上完成 Unit、Functional 和常用命令回归。
这些新增项由 dbt Core 提供框架机制，涉及 Doris SQL 和数据库对象的部分仍由
dbt-doris 完成适配。

当前路线确定为 **dbt Core 1.12.x + Python 3.10 以上**，并将 Core 依赖限制在
`>=1.12.0,<1.13.0`，避免未经验证的新 Minor 或 v2 Runtime 被自动安装。
官方生命周期和具体变化见 [About dbt versions](https://docs.getdbt.com/docs/dbt-versions)
与 [Upgrading to v1.12](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v1.12)。

### 4.3 dbt-doris 1.0 当前能力明细

| 分类 | 能力 | 状态 | 验证 | 当前行为与边界 |
| --- | --- | :---: | --- | --- |
| 连接与环境 | 连接 Doris FE | ✅ | 实测 | 通过 MySQL Connector 连接 FE Query Port，支持用户名、密码、Host、Port 和 Schema |
| 连接与环境 | Profile 与 Target | 🔵 | Core/代码 | dbt Core 支持定义开发、测试、生产等多个 Target，并负责环境切换；dbt-doris 只提供 Doris 连接参数 |
| 连接与环境 | Database / Schema 映射 | ✅ | 实测 | Doris Database 对应 dbt Schema；支持创建、枚举和删除 Database |
| 连接与环境 | 首次连接不存在的 Database | 🟡 | 实测（无库连接）/代码（建库） | 目标 Database 不存在时无库建连已实测；后续建库有 Adapter 实现，但尚未在同一用例中验证完整组合链路 |
| 对象管理 | Table / View Relation | 🟡 | 实测/代码 | Table、View 的查找、创建和重复构建已实测；枚举、改名、删除有实现，但未逐项组成完整动作矩阵 |
| 对象管理 | 字段与类型元数据 | 🟡 | 实测（基础） | 可以从 `information_schema` 读取常用字段和类型；复杂类型、精度及 Schema Evolution 尚未完整验证 |
| 对象管理 | Catalog 元数据 | 🟡 | 实测（命令） | `dbt docs generate` 已通过，但没有逐字段核对全部 Catalog 内容；Catalog SQL 的两个 CTE 内没有下推 Schema 条件，存在较大范围元数据扫描风险 |
| 对象管理 | internal catalog 跨 Database Source | ✅ | 实测 | Source 可以读取同一 Doris 集群中其他 Database 的表 |
| 对象管理 | External Catalog 三层 Relation | ✅ | 代码/实测 | `catalog.database.table` 可用于 Source、Ref、Relation 查找、列元数据和物化 SQL；External Catalog 本身由 Doris 预先配置 |
| 连接可靠性 | SSL、Timeout、Retry、多 FE Failover | ❌ | 代码 | Profile 尚未暴露这些连接与失败恢复配置 |
| Model | Table | ✅ | 实测 | 先创建中间表，再通过表交换或重命名替换目标，避免直接覆盖正在使用的表 |
| Model | View | ✅ | 实测 | 根据 Model 查询创建或更新 Doris View |
| Model | Ephemeral | 🔵 | Core/实测 | 由 dbt Core 把中间逻辑内联成下游 CTE，不在 Doris 中单独落表 |
| 数据资源 | Seed | ✅ | 实测 | 将小型 CSV 建成 Doris 表，支持类型推断和指定列类型 |
| 数据资源 | Snapshot | 🟡 | 实测（Check） | Check Strategy 已验证首次执行、更新、新增、硬删除和再次执行；Timestamp Strategy、故障恢复尚未完整验证，目标表替换也不是原子操作 |
| 数据质量 | Generic Test / Singular Test | 🟡 | 实测（基础） | 已验证 `not_null` Generic Test 和 Singular Test 主链路；其他通用测试仍取决于生成 SQL，尚未逐类验收 |
| 数据质量 | dbt Unit Test | 🟡 | 实测（基础） | 用户编写的 SQL Unit Test 已通过；单 BE 测试库需把默认副本数设为 1，尚未接入官方 Unit Testing 套件，也未覆盖类型、大小写等边界 |
| 文档与血缘 | Docs、Lineage、Manifest、Run Results | 🔵 | Core/实测（主链路） | 主要由 dbt Core 根据项目资源、`ref()` 和 `source()` 生成；dbt-doris 为 Catalog 提供 Doris Relation 和字段元数据 |
| 文档与血缘 | `persist_docs` | 🟡 | 实测/单测 | Table 和 Incremental 已接入；View 底层会跳过不支持的字段 ALTER |
| Incremental | Append | ✅ | 实测 | 将本轮 Model 产生的记录直接追加到目标表，不自动去重 |
| Incremental | Unique Key Upsert (`merge`) | ✅ | 实测/单测 | MOW Unique Key 单条 `INSERT INTO`，支持单列/复合 Key 和重复 Source Key 拒绝 |
| Incremental | Full Refresh | ✅ | 实测 | Table→Table 用 `swap=true` 原子交换；View→Table 先备份旧对象再改名，失败重试前恢复备份 |
| Incremental | `is_incremental()` 过滤 | ✅ | 实测 | 运行时检查目标是否为 Table、Model 是否为 Incremental/Partition，以及当前是否处于 Full Refresh；Model 再据此决定本轮读取哪些源数据 |
| Incremental | `on_schema_change` | ✅ | 官方测试/实测 | 支持 `ignore`、`fail`、`append_new_columns`、`sync_all_columns`，并等待 Doris 异步 Schema Change |
| Incremental | 字段类型扩展 | ✅ | 实测/单测 | 逻辑/物理 Source 均保留 VARCHAR 长度和 DECIMAL 精度、Scale，扩展后等待异步 ALTER 完成 |
| Incremental | Partition 自定义 Materialization | 🟡 | 实测/单测 | 已验证第二次运行只替换本批分区、保留其他分区并清理临时表；它不是 Incremental Strategy，失败恢复和 Schema Change 仍未闭环 |
| Incremental | 与命名相符的 Insert Overwrite | ✅ | 实测/单测 | 生成 Doris 原生 `INSERT OVERWRITE`，支持整表和显式分区覆盖 |
| Incremental | 动态分区覆盖 | ✅ | 实测/单测 | 标准 `insert_overwrite` 支持 Doris 2.1.3+ 原生 `PARTITION(*)` |
| Incremental | Microbatch | ❌ | 代码 | 尚未按 `event_time` 和 `batch_size` 拆分批次，也未声明 dbt Microbatch 能力 |
| Incremental | Merge / Delete+Insert | ✅ | 实测/单测 | 无 Sequence 的 MOW 路由单条 Upsert；MOR 用物理 Staging 和 Doris 3.0+ 显式事务；Sequence 目标前置拒绝 |
| Doris 表设计 | Duplicate Key | ✅ | 实测 | Table 可以声明 Duplicate Key |
| Doris 表设计 | Unique Key | 🟡 | 实测（增量） | Incremental Upsert 路径可以创建 Unique Key 目标；普通 Table 尚无统一表模型入口 |
| Doris 表设计 | Aggregate Key | ❌ | 代码 | 尚未提供 Aggregate Key 及聚合列配置 |
| Doris 表设计 | Range / List Partition | 🟡 | 实测 | 两类分区建表均已通过，但分区定义仍偏原始 SQL，缺少完整结构化校验 |
| Doris 表设计 | HASH Distribution / 固定 Bucket | ✅ | 实测 | 可以指定 HASH 分布列和整数 Bucket 数 |
| Doris 表设计 | RANDOM Distribution / Auto Bucket | ❌ | 代码 | 尚未支持 RANDOM 分布和 `BUCKETS AUTO` |
| Doris 表设计 | Table Properties | 🟡 | 实测（样例）/代码（透传） | 已验证 `replication_num` 和一个任意 Property；其余键值原样透传，错误配置由 Doris 报错，Adapter 没有参数级校验 |
| Doris 表设计 | Engine | 🟡 | 代码 | Seed 路径可以输出 Engine 配置；普通 Table Model 未接入 Engine 配置，常规 CTAS 的 OLAP 行为来自 Doris，多 Engine 建模尚未形成 |
| Doris 表设计 | Sort / Cluster / Secondary Index | ❌ | 代码 | 尚未暴露排序、Cluster、倒排索引、Bloom Filter、Bitmap 等结构化配置 |
| 高级对象 | Async Materialized View | ❌ | 代码 | Doris 本身支持异步物化视图，但 dbt-doris 还没有对应 Materialization |
| 治理 | Model Contract | 🟡 | 实测（列集合）/单测 | 已验证声明列集合和未声明字段报错；声明类型及宽度是否完整保留尚未纳入 Functional 断言，也尚未完成官方兼容套件 |
| 治理 | Constraints | ❌ | 代码 | 尚未把主键、非空、外键等 dbt Constraint 映射成 Doris 能力声明或 DDL |
| 治理 | Grants | ❌ | 代码 | Table、View 虽调用 dbt 通用授权流程，但缺少 Doris Grants 宏，回落 SQL 与 Doris 权限语法不匹配 |
| 治理 | Source Freshness | 🟡 | 实测（`loaded_at_field` 通过路径） | 基于 `loaded_at_field` 的 Freshness 已通过；基于表元数据修改时间的 Freshness 未实现 |
| 工程生态 | Pre / Post Hooks | 🟡 | 实测（Table） | Table Model 的 Pre/Post Hook 已通过；Doris 事务只维护 dbt 状态标记，不提供数据库级原子性 |
| 工程生态 | Macro、Dispatch | 🔵 | Core/单测（Adapter 宏）/代码（Dispatch） | Dispatch 机制由 dbt Core 提供；Doris 宏的解析和部分行为已有单测，当前没有单独验证 Dispatch 选择过程 |
| 工程生态 | dbt Package 兼容 | 🟡 | Core/代码 | dbt Core 支持安装 Package，但当前没有验证常用 Package，不能据此给出兼容清单；是否可用取决于其 Macro 最终生成的 SQL |
| 工程生态 | Adapter Capability 声明 | ❌ | 代码 | 尚未向 dbt 工具链正式声明 Metadata Freshness、Microbatch、单 Relation Catalog 等能力 |
| 资源治理 | Session Variable / Workload Group | ❌ | 代码 | 尚未支持 Profile 或 Model 级会话变量与工作负载组选择 |
| 可观测性 | Query Label、Query ID、服务端 Cancel | ❌ | 代码 | 尚不能把 dbt Invocation、Model 与 Doris Query 关联；取消连接不等于服务端取消查询 |
| 发布兼容 | Python / dbt Core / Doris 版本矩阵 | 🟡 | 实测（单一组合）/单测 | 当前代码声明 Python 3.10+ 和 dbt Core 1.12.x，并已在 Python 3.11 上完成本地回归；仍需由 CI 跑绿 Python 3.10/3.14 边界矩阵 |

当前 1.0 的核心已经覆盖“连接 Doris、组织 Model、构建 Table/View、
运行测试和基础增量”这一条主链路；差距主要集中在标准增量语义、
Doris 高级对象、治理、运行可靠性和生态兼容。

## 5. 用一个订单日报走完整流程

订单日报可以把前面的概念连成一条完整流程。

### 第一步：原始数据进入 Doris

Flink CDC 把业务库订单同步到 Doris 原始层。

这一步不是 dbt 做的。
dbt 只把原始订单声明为 Source，表示“从这里开始加工”。

### 第二步：清洗订单

建立“清洗后的订单” Model，负责：

- 统一字段命名；
- 统一金额和时间格式；
- 过滤无效记录；
- 把不同系统的订单状态整理成统一口径。

如果这层逻辑较轻，可以先做成 View。

### 第三步：生产业务结果

清洗后的订单可以继续生成：

| 结果 | 用途 |
| --- | --- |
| 订单事实表 | 保存可复用的订单明细 |
| 每日销售汇总 | 给经营日报和看板使用 |
| 客户购买汇总 | 分析客户下单次数和累计消费 |

订单事实表数据量很大时，可以采用增量方式；
日报需要频繁查询时，可以完整落成 Table。

### 第四步：给结果加质量规则

例如：

- 订单 ID 唯一且非空；
- 销售金额不能小于零；
- 日报中同一天、同一渠道只能有一行；
- 客户 ID 必须能在客户数据中找到。

运行一次 `dbt build` 时，dbt 会按依赖构建 Model，并运行相关测试；
实际 SQL 仍然由 Doris 执行。

### 第五步：响应一次需求变化

假设经营日报要增加“销售渠道”：

1. 在订单清洗层统一渠道口径；
2. 在每日销售汇总中增加渠道维度；
3. 增加渠道非空和合法值检查；
4. 构建受影响的模型和下游；
5. 让文档和血缘随项目一起更新。

这个例子体现了 dbt 的核心价值：

> 需求变化不再只是改一段 SQL，而是修改一个有依赖、有测试、
> 有说明的数据项目。

## 6. 从其他 Adapter 重点看什么

Table、View、Incremental、Seed、Snapshot、Test 和 Docs 等属于 dbt Adapter
的通用基础能力。比较其他产品时，没有必要在每个产品下面重复罗列这些能力；
dbt-doris 是否补齐基础功能，统一看第 4.3 节的当前状态和第 7.1 节的计划。

真正值得参考的是：其他 Adapter 怎样把数据库特有的对象、物理设计、增量方式、
资源和治理能力变成 dbt Config，以及这些设计在 Doris 中有没有对应能力。

本节依据 2026-07-27 可访问的官方文档和官方 Adapter 仓库整理，只说明公开接口，
没有在本地搭建这些数据库做端到端验收。各项能力仍受 Adapter、数据库版本和部署
方式限制；“可参考”也不表示 Doris 必须照搬。

### 6.1 值得参考的特色能力

| Adapter | 已公开的特色接入 | 解决的问题 | 对 Doris 的参考 |
| --- | --- | --- | --- |
| [StarRocks](https://github.com/StarRocks/dbt-starrocks) | 把 Primary、Duplicate、Unique 表模型、分区分桶、Materialized View 和 External Catalog 暴露为配置；Dynamic Overwrite 要求 StarRocks 3.4+，Microbatch 可选择 Insert Overwrite，并在 3.4+ 选择 Dynamic Overwrite | 让 OLAP 表设计、分区增量和高级对象进入 dbt Model | 与 Doris 产品形态最接近。应优先参考其表模型配置、增量策略和物化视图生命周期，但不能直接认定 SQL 语义相同 |
| [ClickHouse](https://clickhouse.com/docs/integrations/connectors/data-ingestion/etl-tools/dbt/materializations) | Model 可声明 Engine、`order_by`、`partition_by`、TTL、Table/Query Settings、列 Codec、Skipping Index、Projection 和 Materialized View | 把数据库物理设计与 Model 一起管理 | Doris 可用 Key 模型、分区分桶、Table Properties、Secondary Index 和 Async MV 解决相近问题；这些对象不是一一对应，需按 Doris 语义重新设计 Config |
| [Snowflake](https://docs.getdbt.com/reference/resource-configs/snowflake-configs) | Dynamic Table 支持 `target_lag` 和配置变更策略；Model/Test 可选择 Warehouse；Query Tag 可按 Model 写入会话 | 管理自动刷新对象、计算资源和查询追踪 | 可分别参考到 Doris Async MV、Workload Group、Session Variable 和 Query ID，但底层刷新与资源模型不同 |
| [Databricks](https://docs.getdbt.com/reference/resource-configs/databricks-configs) | Materialized View、Streaming Table 支持刷新计划和配置变化管理；还提供 Liquid Clustering 和治理 Tag | 管理持续刷新对象、存储布局和治理元数据 | Async MV 的创建、刷新和变更处理值得参考；Streaming Table 和 Liquid Clustering 没有直接的 Doris 对应物，不应直接列为 dbt-doris 必做项 |
| [BigQuery](https://docs.getdbt.com/reference/resource-configs/bigquery-configs) | 用结构化 `partition_by` 描述字段、类型和粒度；Insert Overwrite 可静态或动态识别待覆盖分区；还支持 Microbatch、Reservation 和 Job Label | 降低分区配置错误，支持时间增量、资源路由和任务追踪 | 可参考 Doris 分区结构化配置、`INSERT OVERWRITE ... PARTITION(*)`、Workload Group 和查询标识的接入方式 |
| [Trino](https://docs.getdbt.com/reference/resource-configs/trino-configs) | 以 `catalog.schema.table` 表达三层命名空间，通过 Session Property 和 Connector Properties 处理不同数据源的能力差异 | 在一个项目中访问多 Catalog，同时保留各 Connector 的能力边界 | 可参考 Doris External Catalog 的三段式 Relation、Session Variable，以及按 Catalog 区分可用操作 |

其中，Doris 已经原生支持
[`INSERT OVERWRITE ... PARTITION(*)`](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/data-modification/DML/INSERT-OVERWRITE/)
自动识别并覆盖本批涉及的分区（该语法从 Doris 2.1.3 开始提供），也支持
[Async Materialized View](https://doris.apache.org/docs/4.x/query-acceleration/materialized-view/async-materialized-view/overview/)、
[Multi Catalog](https://doris.apache.org/docs/4.x/key-features/multi-catalog/)和
[Workload Group](https://doris.apache.org/docs/4.x/admin-manual/workload-management/workload-group/)。
这里的主要差距是 dbt-doris 还没有把 Doris 能力完整接入 Config、Relation、
Materialization、连接管理和对象生命周期。

### 6.2 竞品能力怎样转成 dbt-doris 的思考

评估一项竞品能力时，按下面的顺序判断：

```text
它解决什么用户问题
        |
Doris 是否已有对应的原生能力
        |
dbt-doris 是否需要把它变成 Config、SQL 或连接行为
        |
当前实现缺少哪一层
```

因此：

1. 通用基础能力不在竞品章节重复比较，但 dbt-doris 自身仍必须补齐；
2. 有明确 Doris 对应能力的设计，才进入 Doris 原生能力或高级场景路线；
3. Doris 已有能力时，dbt-doris 通常负责识别配置、生成 SQL，并管理创建、
   刷新、变更和删除；
4. SSL、Retry、Query ID 等不是 Doris 专有建模能力，应放在生产可用性中；
5. Python Model、Streaming Table、Liquid Clustering 等没有直接对应物的能力，
   只记录其解决的问题，不自动变成 dbt-doris 的实现目标。

这样比较的目标是找出能够帮助 dbt 用户正确使用 Doris 的成熟 Adapter 设计。

## 7. dbt-doris 下一步重点补什么

下一步可以概括成两件事：先补齐 dbt Adapter 应有的基础功能，再把 Doris
有特色、有价值的能力接入 dbt。

### 7.1 先补齐 dbt 基础功能

包括 Incremental、Snapshot、Test、Contract、Docs、Freshness、Grants 等
dbt 用户会直接使用的标准能力，也包括连接稳定性、元数据性能、测试覆盖和版本
兼容。目标是让熟悉 dbt 的用户迁到 Doris 后，常用工作流仍然具有清晰、稳定的
行为。

### 7.2 再接入并做深 Doris 特色能力

重点不是照搬其他数据库的功能，而是让 dbt 能正确表达 Doris 的能力，例如：

- Duplicate、Unique、Aggregate Key 表模型；
- Range、List、Expression、Auto 和 Dynamic Partition；
- HASH、RANDOM Distribution，固定 Bucket 和 Auto Bucket；
- Sort、Cluster、Secondary Index 和常用 Table Properties；
- Dynamic Overwrite、Async Materialized View 和 External Catalog。

dbt-doris 在这里主要负责识别 Config，生成正确的 Doris SQL，并管理对象的创建、
刷新、变更和删除。这样用户才能在 dbt Model 中完成 Doris 表设计和平台能力接入，
而不只是向 Doris 提交一条 `SELECT`。

## 8. 最后总结

核心结论有五点：

1. dbt 是数据转换的工程化工具，不是数据同步工具，也不是计算引擎；
2. Doris 真正保存数据和执行 SQL，dbt 管 Model、依赖、测试、文档和构建；
3. dbt-doris 是二者之间的适配层，也负责表达 Doris 的物理设计；
4. 当前 dbt-doris 1.0.0 是 dbt Core v1 Python Adapter，后续开发和验证基线升级到
   dbt Core 1.12.x，尚不能直接运行在 dbt v2；
5. 后续先修正现有标准语义，再建设 Doris 原生能力、高级场景和运行治理，
   同时用独立最小链路验证 dbt v2。

整体关系可以归纳为：

```text
数据进入 Doris
      |
dbt 组织转换工程
      |
dbt-doris 翻译成 Doris 行为
      |
Doris 执行并产出数据模型
      |
BI 和应用消费结果
```

## 9. 延伸阅读

- [详细版：dbt 与 dbt-doris 的理解、功能与路线图](dbt-doris-status-and-roadmap.zh-CN.md)
- [测试结果与已知问题](dbt-doris-test-results-and-known-issues.zh-CN.md)
- [Apache Doris dbt-doris](https://github.com/apache/doris/tree/master/extension/dbt-doris)
- [dbt 官方文档](https://docs.getdbt.com/docs/introduction)
- [dbt Core 版本与生命周期](https://docs.getdbt.com/docs/dbt-versions)
- [dbt Core 1.12 升级说明](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v1.12)
- [dbt Fusion 与 v2 Engine](https://docs.getdbt.com/docs/fusion/about-fusion)
- [dbt v2 Adapter 可用性](https://docs.getdbt.com/docs/fusion/fusion-availability)
