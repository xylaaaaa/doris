# dbt-doris v1 / v2 技术选型与迁移评估

> 文档状态：技术选型建议稿
>
> 评估日期：2026-07-28
>
> 适用范围：Apache Doris `extension/dbt-doris` 当前 `master` 工作区

## 1. 结论

**当前开发主线：dbt-doris v1。**

采用顺序推进路线：

1. 先把现有 v1 Adapter 补成稳定可用的版本；
2. 期间跟踪 v2 的版本、Adapter 接入和 Driver 状态；
3. 等 v2 达到本文定义的迁移门槛后，冻结 v1 的功能开发；
4. 以 v1 已稳定的 Config、Doris SQL 语义和兼容测试为基线，
   集中资源顺序移植到 v2。

现在选择 v1 的主要原因是：

- dbt Core 2.0 仍处于 Alpha，正式发布日期仍为 `TBD`；
- 官方可用的 v2 Adapter 仍全部处于 Preview 或 Beta，Doris 尚未进入矩阵；
- v2 社区 Adapter 采用 `dbt-core` Rust 单仓库贡献模式，
  前置条件包括 ADBC Driver、dbt Labs 的评审、CI 和版本发布；
- 当前 dbt-doris 已经有一套 v1 Python Adapter 和测试基础，
  dbt Core v1.12 仍支持到 2027-07-15。

一句话概括：

> 现在先把 dbt-doris v1 做好；等 v2 的运行时、Doris Driver 和官方
> 接入路径达到可交付条件后，停止扩展 v1，再把已有产品语义顺序移植到 v2。

## 2. 这里的 v1 和 v2 分别是什么

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

## 3. v2 什么时候发布，目前是什么状态

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

这给了 dbt-doris 一个时间边界明确的 v1 建设窗口：应尽快迁移到
1.11/1.12 验证基线，并把建设范围聚焦在公开、可迁移的接口上。

## 4. 哪些产品已经有 v2 Adapter

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

## 5. dbt-doris 当前实现情况

dbt-doris 当前已经具备一套 v1 Python Adapter：

- 有 Credentials、Connection Manager、Adapter、Relation 和 Column；
- 有 Table、View、Incremental、Partition、Seed、Snapshot、
  Freshness、Catalog 等 Macro；
- 有无集群单元测试和连接真实 Doris 的 Functional Test；
- 当前通过 MySQL 协议和 `mysql-connector-python` 连接 Doris。

v1 的基础能力仍待完善，主要包括：

- 当前安装范围为 `dbt-core>=1.10.4`，后续需要增加 `<2.0` 上限；
- Incremental 的策略名称、实际 SQL 和标准行为仍需统一；
- Snapshot、Contract、Constraint、Grants、Schema Change、
  Freshness 等标准能力覆盖不完整；
- SSL、Timeout、Retry、多 FE Failover、Query ID 和服务端 Cancel
  等生产连接能力仍需补齐；
- Doris Key、Partition、Distribution、Property 等配置还需要形成
  一致、可测试的产品契约。

因此，当前状态更准确的描述是：

> v1 已经有可运行骨架和不少功能，当前阶段是补齐完整、稳定的基础版本。

这也是现在选择 v1 的现实基础：先把已有实现交付出来，同时沉淀未来
v2 最需要复用的 Config 语义、Doris SQL 和黑盒测试。

## 6. 当前选择 v1 的依据

| 评估项 | 现在直接只开发 v2 | 先完成 v1，再顺序移植 v2 |
| --- | --- | --- |
| 当前代码基础 | 需要进入 Rust 单仓库重新接入 | 可以继续使用现有 Python Adapter |
| 上游稳定性 | Core 2.0 仍为 Alpha，接口和流程可能变化 | v1 Adapter 接口和发布方式成熟 |
| Doris 可用性 | 官方矩阵无 Doris，Driver 和上游合入尚未验证 | 当前已经可以通过 MySQL 协议连接 Doris |
| 用户安装 | 需要等 PR 合入 `dbt-core` 并随官方版本发布 | 可以继续作为独立 Python Package 发布 |
| 短期交付 | 风险较高，可能长期只有 Fork 中的实现 | 可以较快补齐当前用户需要的基础能力 |
| 后续成本 | 减少一轮 Python 建设，同时承担上游和 Driver 不确定性 | Python 接入层以后需要重写，产品语义和测试可以复用 |

官方升级文档建议有受支持 Adapter 的新项目使用 v2；这个建议对 Snowflake、
BigQuery 等已进入矩阵的产品成立。Doris Adapter 当前采用先完成 v1、
达到迁移门槛后再切换 v2 的路线。

选择 v1 的主要风险是当前已发布 Minor 版本的支持窗口有明确期限，
部分 Python 代码以后需要按 v2 重新接入。
控制方式如下：

- v1 建设范围聚焦基础功能、生产可用性和公开接口；
- Config 名称、默认值和错误边界尽量保持运行时无关；
- 优先积累从用户输入到 Doris 对象和结果的 Functional Test；
- 每季度复核 v2 状态，并在 2027-01-15 前做一次强制重新选型，
  为 v1.12 支持结束预留至少六个月。

## 7. 当前 v1 应该做到什么程度

v1 的目标是形成稳定可用的基础版本，当前范围聚焦基础能力。

近期重点包括：

1. **版本与测试基线**
   - 验证 dbt Core 1.11/1.12，声明明确的支持范围和 `<2.0` 上限；
   - 建立核心版本、Python、Connector 和 Doris 的兼容矩阵。

2. **dbt 基础能力**
   - 完善 Table、View、Incremental、Seed、Snapshot、Data Test、
     Docs/Catalog、Contract、Schema Change、Grants 和 Freshness；
   - 让首次构建、重复构建、Full Refresh 和失败边界都有测试。

3. **Doris 基础建模**
   - 统一 Duplicate、Unique、Aggregate 表模型；
   - 完善 Partition、Distribution、Bucket 和常用 Property；
   - 保证 Table、Incremental 和 Full Refresh 使用一致的配置语义。

4. **生产可靠性**
   - 补齐 SSL、Timeout、Retry、多 FE、Query ID 和服务端 Cancel；
   - 明确连接失败、查询失败以及可安全重试的边界。

当前交付计划只包含 v1。ADBC PoC、Rust Adapter 和 v2 SQL 支持安排在
未来的 v2 迁移阶段；当前对 v2 进行版本和官方接入状态的定期复核。

## 8. 官方提供的 v1 到 v2 迁移方法

官方迁移方法分为“dbt 用户项目升级”和“数据库 Adapter 移植”两类。

### 8.1 dbt 用户项目升级：有自动辅助工具

[官方 Upgrading to v2 指南](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
提供了：

- `dbt-autofix`：自动修复一部分已废弃项目写法；
- dbt Core 1.12 的 `--use-v2-parser`：在继续使用 v1 执行的同时，
  提前检查 v2 Parser 兼容性；
- Manifest v12 兼容：v1 和 v2 可以使用 State、Defer 等机制进行迁移验证；
- 逐项列出的废弃配置、Jinja、YAML 和 Package 兼容修改。

这些工具用于处理 Model、YAML、Macro、Package 和 Job 等**用户项目代码**。
Doris v2 Adapter 的实现采用下一节的人工移植路线。

### 8.2 Adapter 实现移植：官方人工指南

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

## 9. v1 Adapter 后续怎样移植到 v2

### 9.1 可以复用的资产

| v1 资产 | v2 中的去向 |
| --- | --- |
| Adapter/Catalog 类 Macro（当前主要位于 `macros/adapters/*.sql`，Catalog 在 `metadata.sql`） | 移入 `dbt-loader` 的 Doris Macro；其中 SQL 和 Dispatch 行为通常可直接或小幅调整移植 |
| 自定义 Materialization | 继续使用 Jinja SQL；v2 中更严格或待支持的 Jinja 行为需要调整 |
| Profile 字段 | 转成 `dbt-schemas` 中的 Doris `DbConfig` |
| URI、认证和初始化 SQL | 转到 `dbt-auth` |
| Relation 引用和 Quote Policy | 转成 v2 Relation Policy |
| Catalog 和元数据 SQL | 转到 `get_relation` 等元数据实现及相关 Macro |
| Doris Config | 复用名称和行为设计；最终用户写法由 v2 的配置位置、Schema 注册和 Macro 读取 API 验证结果确定 |
| Functional Test | 作为 v1/v2 行为等价的验收基线 |

### 9.2 需要按 v2 重新接入的代码

| v1 组件 | v2 的处理方式 |
| --- | --- |
| Python Cursor、连接池和连接生命周期 | 由 ADBC/XDBC 负责 |
| `execute`、`add_query` 等执行 Plumbing | 由共享 Rust 执行层负责 |
| Python Adapter 类继承体系 | 改为 Rust 各模块中的 `AdapterType` 分支 |
| `setup.py` 和 PyPI 发布 | 改为合入 `dbt-core` 并由 dbt Labs 随版本发布 |
| MySQL Connector 特有异常和行为 | 按最终 ADBC Driver 的错误、取消和认证模型重新验证 |

先做 v1 可以沉淀 Doris SQL、Macro/Materialization 语义、Config 契约、
元数据规则和测试。Python 类属于 v2 的重新接入范围。

### 9.3 Doris Driver 的前置条件

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

## 10. 什么时候开始 v2，届时怎么切换

### 10.1 开始 v2 移植的门槛

满足以下条件后，再停止扩展 v1 并启动 v2：

1. Core 2.0 和社区 Adapter 接口至少进入团队可以承诺兼容的稳定阶段，
   例如 Beta、RC 或 GA；
2. dbt Labs 明确接受 Doris Adapter 的贡献，并确认评审、CI、合入和
   版本发布路径；
3. 已存在可供 dbt Core v2 注册的 Doris ADBC Driver 候选，
   并且安装和分发路径明确；
4. v1 的基础功能、Config 契约和 Functional Test 已经稳定，可作为迁移基线；
5. 团队有足够时间在当时使用的 v1 Minor 版本 EOL 前完成移植和用户迁移。

Adapter 接口、Driver 和发布路径共同决定 v2 移植的启动时间，
GA 生命周期作为稳定性判断依据之一。

### 10.2 顺序迁移步骤

达到门槛后按以下顺序推进：

1. **冻结 v1 功能**：v1 进入维护模式，只处理严重缺陷；
2. **验证 Driver**：确认 Doris ADBC 的连接、执行、元数据、错误和网络行为；
3. **接入 v2 基础模块**：注册 `AdapterType` 和 Driver，接入 Profile、
   Auth、Relation、Column、类型和元数据；
4. **移植 Doris Macro**：移植 Table、View、Incremental、Seed、
   Snapshot、Catalog 及 Doris Config；
5. **做行为等价验证**：复用 v1 Functional Test，对比 Doris 对象、
   数据结果、错误和 Artifact；
6. **准备安装文档**：说明 `profiles.yml`、Driver 共享库的准确名称、
   获取方式和安装位置；
7. **走官方发布流程**：提交 `dbt-core` PR，由 dbt Labs Adapter
   团队协作运行社区 CI，等待合入和发布；
8. **迁移用户项目**：使用 `dbt-autofix`、`--use-v2-parser` 和官方升级指南
   处理项目级兼容问题；
9. **切换默认版本**：v2 达到基础能力等价且迁移、回退文档就绪后，
   再把 v2 设为默认。

迁移末期可以有一段短期 v1/v2 双运行来做验收和回退验证，
该阶段的研发主线只有 v2。

## 11. 最终决策表

| 问题 | 结论 |
| --- | --- |
| 当前开发哪一版？ | v1。先补齐基础能力和生产可用性 |
| v2 什么时候发布的？ | Fusion 于 2025-05-28 首次公开；Core 2.0 开源双发行路线于 2026-06-01 公布；截至 2026-07-28 Core 2.0 仍为 Alpha，GA 日期为 `TBD` |
| 哪些产品已经有 v2？ | 官方可用矩阵只有 Snowflake、BigQuery、Databricks、Redshift、Spark 和 DuckDB，且都仍是 Preview/Beta |
| Doris v2 当前状态？ | 尚未进入官方可用矩阵和官方列出的待完善占位项 |
| 为什么还要做 v1？ | 当前已有 v1 实现和用户交付路径；v1.12 这个 Minor 版本支持到 2027-07-15；先补基础功能能形成稳定产品和迁移基线 |
| dbt 项目的官方迁移方法？ | 使用 `dbt-autofix`、`--use-v2-parser` 和官方兼容检查 |
| Adapter 的官方迁移方法？ | 按官方人工指南将 Python Adapter 功能映射到 Rust 新架构，并向 `dbt-core` 贡献实现 |
| v1 哪些工作可以复用？ | Doris SQL、Macro 行为、Config 契约、元数据规则和 Functional Test |
| 什么时候转 v2？ | Core/Adapter 接口、Doris ADBC Driver、上游合入发布路径和 v1 行为基线均达到迁移门槛后，冻结 v1 再顺序移植 |

## 12. 资料来源

dbt 官方资料：

- [About dbt versions](https://docs.getdbt.com/docs/dbt-versions)
- [Fusion availability](https://docs.getdbt.com/docs/fusion/fusion-availability)
- [Product lifecycles](https://docs.getdbt.com/docs/dbt-versions/product-lifecycles)
- [Upgrading to v2](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
- [Contribute a dbt Core 2.0 adapter](https://docs.getdbt.com/guides/adapter-creation-v2?step=1)
- [dbt Licensing FAQ](https://www.getdbt.com/licenses-faq)
- [Support for externally loaded custom adapters](https://github.com/dbt-labs/dbt-core/issues/13138)

Doris 与当前实现：

- [Doris Arrow Flight SQL](https://doris.apache.org/docs/3.0/db-connect/arrow-flight-sql-connect)
- [当前 dbt-doris Python Adapter](../dbt/adapters/doris/)
- [当前 dbt-doris Macro](../dbt/include/doris/macros/)
- [当前 dbt-doris 测试](../test/)
