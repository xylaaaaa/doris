# dbt-doris v1 / v2 技术选型与迁移评估

> 文档状态：技术选型建议稿
>
> 评估日期：2026-07-28
>
> 适用范围：dbt-doris v1 / v2 Adapter 路线选型

## 1. 这里的 v1 和 v2 分别是什么

本文中的 v1、v2 指 dbt Framework 的两代运行时。下面三种写法各有独立含义：

| 写法 | 实际含义 |
| --- | --- |
| `dbt-doris==1.0.0` | Doris Adapter 自身版本 |
| `config-version: 2` | `dbt_project.yml` 格式版本 |
| YAML 中的 `version: 2` | Model、Source、Test 等属性文件格式 |

dbt v1 指 Python 实现的 dbt Core 1.x，以及通过 Python Package
独立安装的数据库 Adapter。当前 dbt-doris 就属于这条路线。

dbt v2 指新的 Rust Engine。它有两个发行形态：

- **dbt Core 2.0**：Apache 2.0 开源发行版；
- **Fusion**：基于同一 Engine 的增强发行版，包含额外产品能力。

两代运行时延续相同的 dbt 项目语言和主要 DAG 语义。
v2 采用新的 Adapter 代码组织、连接 Driver、执行基础设施和发布方式，
Adapter 需要按新架构接入。

## 2. v2 什么时候发布，目前是什么状态

“v2 已经发布”需要拆成几个时间点理解：

| 时间 | 事件 | 准确含义 |
| --- | --- | --- |
| 2025-05-28 | dbt Labs 首次公开 Fusion Engine | 新 Rust Engine 进入公开预览阶段 |
| 2026-06-01 | dbt Labs 公布 v2 Framework 的 Core 2.0 / Fusion 双发行路线，并将共享基础代码以 Apache 2.0 发布到 `dbt-core` | Core 2.0 开源代码和 Alpha 版本开始公开 |
| 2026-07-28 | 本文评估日 | Core 2.0 仍为 Alpha，Initial Release 仍为 `TBD`；Fusion 本地 CLI 和编辑器体验仍为 Preview |

所以，截至本文评估日：

> Fusion 从首次公开算起约 14 个月；Core 2.0 开源路线公布不到两个月；
> v2 已经开放预览和代码贡献，Core 2.0 正式发布日期仍为 `TBD`。

Core Alpha 的官方使用范围是研究和测试，行为与实现仍可能变化。
Fusion Adapter Preview 表示已支持范围内的功能稳定并具备生产使用条件。

官方当前仍在支持的 v1 版本为：

| dbt Core 版本 | 当前状态 | 支持截止时间 |
| --- | --- | --- |
| 1.12 | Active Support | 2027-07-15 |
| 1.11 | Critical Support | 2026-12-18 |
| 1.10 | Deprecated | 官方维护已结束 |

表中日期分别对应各个 Minor 版本。整个 v1 系列当前按各 Minor 版本
分别计算生命周期，后续支持窗口以官方是否发布新的 v1.x 为准。

## 3. 哪些产品已经有 v2 Adapter

[官方 Fusion 可用性矩阵](https://docs.getdbt.com/docs/fusion/fusion-availability)
当前只列出六个 Adapter：

| Adapter | 生命周期 | 当前边界 |
| --- | --- | --- |
| Snowflake | Preview | 已进入官方 v2 可用矩阵，GA 前仍可能变化 |
| BigQuery | Preview | 已进入官方 v2 可用矩阵，认证方式有明确边界 |
| Databricks | Preview | 已进入官方 v2 可用矩阵，认证方式有明确边界 |
| Redshift | Preview | 已进入官方 v2 可用矩阵 |
| Apache Spark | Beta | 仅 CLI |
| DuckDB | Beta | 仅 CLI |

官方 v2 Adapter 当前最高生命周期为 Preview。Doris、StarRocks、
ClickHouse 和 Trino 尚未进入这张可用矩阵。

按官方生命周期定义，Preview 表示已支持范围内的功能稳定、具备生产使用条件，
GA 前仍可能增加能力或出现不向后兼容的变化；Beta 仍可能不完整或不稳定。
这六个产品已经可以按官方支持范围使用，成熟度和支持边界仍在演进。

同时，官方 v2 Adapter 贡献指南还列出了 Athena、Trino、Starburst、
Dremio、Oracle 等源码占位项。这表示 `AdapterType` 等部分样板已经存在，
其认证、Macro 和 Adapter 分支处于待实现状态，产品状态为待完善。
Doris 尚未进入官方列出的占位项。

因此，判断“某产品已经做了 v2”要区分三种状态：

| 状态 | 含义 |
| --- | --- |
| 进入官方可用矩阵 | 用户已经可以按官方边界使用，目前只有上述六个 |
| 源码中存在占位或部分实现 | 已有继续贡献代码的基础，产品状态为待完善 |
| 只有成熟 v1 Adapter | v2 接入方式为按新架构移植 |

## 4. 当前选择 v1 的依据

| 评估项 | 当前选择 v1 | 当前选择 v2 |
| --- | --- | --- |
| Runtime 生命周期 | v1 Adapter 接口和发布机制成熟 | Core 2.0 仍为 Alpha |
| Doris 接入路径 | 第三方 Adapter 可以作为独立 Python Package 开发和发布 | Doris 尚未进入官方矩阵，需要向 Rust 单仓库贡献实现 |
| Driver | 使用成熟的 Python Connector / DB API 生态 | ADBC Driver 是硬前置条件 |
| 用户安装 | Adapter 可以独立发布 | 需要等待 `dbt-core` PR 合入并随官方版本发布 |
| 上游依赖 | 主要依赖受支持的 dbt Core v1 Minor 版本 | 依赖 dbt Labs 评审、社区 CI、Driver 分发和发布周期 |
| 后续成本 | v2 成熟后需要移植 Adapter 接入层 | 当前承担 Runtime、Driver 和接入流程的变化风险 |

官方升级文档建议已支持 Adapter 的新项目使用 v2。Doris 尚未进入 v2
可用矩阵，当前选择 v1 可以获得成熟的第三方 Adapter 开发与发布路径。

选择 v1 的代价是后续需要按 Rust 新架构移植 Adapter。可以复用的核心资产是
Config 语义、Doris SQL、Macro 行为和兼容测试，Python 接入层进入重新实现范围。

## 5. 官方提供的 v1 到 v2 迁移方法

官方迁移方法分为“dbt 用户项目升级”和“数据库 Adapter 移植”两类。

### 5.1 dbt 用户项目升级：有自动辅助工具

[官方 Upgrading to v2 指南](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
提供了：

- `dbt-autofix`：自动修复一部分已废弃项目写法；
- dbt Core 1.12 的 `--use-v2-parser`：在继续使用 v1 执行的同时，
  提前检查 v2 Parser 兼容性；
- Manifest v12 兼容：v1 和 v2 可以使用 State、Defer 等机制进行迁移验证；
- 逐项列出的废弃配置、Jinja、YAML 和 Package 兼容修改。

这些工具用于处理 Model、YAML、Macro、Package 和 Job 等**用户项目代码**。
Doris v2 Adapter 的实现采用下一节的人工移植路线。

### 5.2 Adapter 实现移植：官方人工指南

官方已经发布
[Contribute a dbt Core 2.0 adapter](https://docs.getdbt.com/guides/adapter-creation-v2?step=1)，
其中专门给出了现有 v1 Adapter 到 v2 的映射和逐文件贡献步骤。

Adapter 实现迁移采用以下形态：

- 按官方映射把 Python Adapter 功能逐项改写为 Rust 实现；
- v2 运行时加载新的 Rust Adapter 实现；
- Adapter 编译进 `dbt-core`，通过官方版本发布。

v2 的官方社区贡献方式是：

1. 先有可用的 ADBC Driver；
2. 在 `dbt-core` Rust 单仓库中实现 Adapter；
3. 向 dbt Labs 提交 PR；
4. 配合官方完成评审和 CI；
5. PR 合入并随官方版本发布后，用户才能正式使用。

官方路线可以概括为：

> 按新架构手工移植 Adapter，并向 `dbt-core` 贡献实现。

## 6. v1 Adapter 后续怎样移植到 v2

### 6.1 可以复用的资产

| v1 资产 | v2 中的去向 |
| --- | --- |
| Adapter/Catalog 类 Macro | 移入 `dbt-loader` 的 Doris Macro；其中 SQL 和 Dispatch 行为通常可直接或小幅调整移植 |
| 自定义 Materialization | 继续使用 Jinja SQL；v2 中更严格或待支持的 Jinja 行为需要调整 |
| Profile 字段 | 转成 `dbt-schemas` 中的 Doris `DbConfig` |
| URI、认证和初始化 SQL | 转到 `dbt-auth` |
| Relation 引用和 Quote Policy | 转成 v2 Relation Policy |
| Catalog 和元数据 SQL | 转到 `get_relation` 等元数据实现及相关 Macro |
| Doris Config | 复用名称和行为设计；最终用户写法由 v2 的配置位置、Schema 注册和 Macro 读取 API 验证结果确定 |
| Functional Test | 作为 v1/v2 行为等价的验收基线 |

### 6.2 需要按 v2 重新接入的代码

| v1 组件 | v2 的处理方式 |
| --- | --- |
| Python Cursor、连接池和连接生命周期 | 由 ADBC/XDBC 负责 |
| `execute`、`add_query` 等执行 Plumbing | 由共享 Rust 执行层负责 |
| Python Adapter 类继承体系 | 改为 Rust 各模块中的 `AdapterType` 分支 |
| `setup.py` 和 PyPI 发布 | 改为合入 `dbt-core` 并由 dbt Labs 随版本发布 |
| MySQL Connector 特有异常和行为 | 按最终 ADBC Driver 的错误、取消和认证模型重新验证 |

先做 v1 可以沉淀 Doris SQL、Macro/Materialization 语义、Config 契约、
元数据规则和测试。Python 类属于 v2 的重新接入范围。

### 6.3 Doris Driver 的前置条件

官方把 ADBC Driver 列为 v2 Adapter 的硬前置条件。
v2 移植的第一项基础工作是准备可注册的 Doris ADBC Driver。

Doris 2.1 起已经支持 Arrow Flight SQL，也可以使用标准 Flight SQL ADBC
Driver，这是未来最值得验证的候选路线。迁移阶段需要验证：

- dbt v2 接受的 Driver 注册和分发方式；
- DDL、DML、元数据、类型、认证、TLS、Timeout 和 Cancel；
- FE Flight 端口返回 BE Endpoint 后的网络可达性；
- 多 FE、代理/VIP 和生产部署边界。

官方社区 Driver 当前有两种分发路径：进入官方 CDN 后自动分发，
或由用户手工安装。新增社区 Driver 的签名和自动分发准入需要与 dbt Labs
单独协调，通用机制仍在建设中。社区 Adapter CI 也由 dbt Labs Adapter
团队协作完成。这些上游依赖构成当前开发 v2 的交付风险。

## 7. 最终决策表

| 问题 | 结论 |
| --- | --- |
| 当前开发哪一版？ | v1 |
| v2 什么时候发布的？ | Fusion 于 2025-05-28 首次公开；Core 2.0 开源双发行路线于 2026-06-01 公布；截至 2026-07-28 Core 2.0 仍为 Alpha，GA 日期为 `TBD` |
| 哪些产品已经有 v2？ | 官方可用矩阵只有 Snowflake、BigQuery、Databricks、Redshift、Spark 和 DuckDB，且都仍是 Preview/Beta |
| Doris v2 当前状态？ | 尚未进入官方可用矩阵和官方列出的待完善占位项 |
| 为什么选择 v1？ | 第三方 Adapter 开发与发布机制成熟；v1.12 这个 Minor 版本支持到 2027-07-15；v2 当前仍有 Runtime、Driver 和发布路径风险 |
| dbt 项目的官方迁移方法？ | 使用 `dbt-autofix`、`--use-v2-parser` 和官方兼容检查 |
| Adapter 的官方迁移方法？ | 按官方人工指南将 Python Adapter 功能映射到 Rust 新架构，并向 `dbt-core` 贡献实现 |
| v1 哪些工作可以复用？ | Doris SQL、Macro 行为、Config 契约、元数据规则和 Functional Test |

## 8. 资料来源

dbt 官方资料：

- [About dbt versions](https://docs.getdbt.com/docs/dbt-versions)
- [Fusion availability](https://docs.getdbt.com/docs/fusion/fusion-availability)
- [Product lifecycles](https://docs.getdbt.com/docs/dbt-versions/product-lifecycles)
- [Upgrading to v2](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
- [Contribute a dbt Core 2.0 adapter](https://docs.getdbt.com/guides/adapter-creation-v2?step=1)
- [dbt Licensing FAQ](https://www.getdbt.com/licenses-faq)
- [Support for externally loaded custom adapters](https://github.com/dbt-labs/dbt-core/issues/13138)

Doris 资料：

- [Doris Arrow Flight SQL](https://doris.apache.org/docs/3.0/db-connect/arrow-flight-sql-connect)
