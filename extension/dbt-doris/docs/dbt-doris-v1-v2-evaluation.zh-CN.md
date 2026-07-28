# dbt-doris v1 / v2 技术选型与迁移评估

> 文档状态：技术选型建议稿
>
> 评估日期：2026-07-28
>
> 适用范围：Apache Doris `extension/dbt-doris` 当前 `master` 工作区

## 1. 结论

**现阶段只开发 dbt-doris v1，不同时启动 v2 Adapter 开发。**

这不是永久放弃 v2，而是按顺序推进：

1. 先把现有 v1 Adapter 补成稳定可用的版本；
2. 期间只跟踪 v2 的版本、Adapter 接入和 Driver 状态，不并行写 v2 代码；
3. 等 v2 达到本文定义的迁移门槛后，冻结 v1 的功能开发；
4. 以 v1 已稳定的 Config、Doris SQL 语义和兼容测试为基线，
   集中资源顺序移植到 v2。

现在选择 v1 的主要原因是：

- dbt Core 2.0 仍处于 Alpha，正式发布日期仍为 `TBD`；
- 官方可用的 v2 Adapter 仍全部处于 Preview 或 Beta，且没有 Doris；
- v2 已有社区 Adapter 贡献指南，但必须把代码合入 `dbt-core` Rust
  单仓库，并依赖 ADBC Driver、dbt Labs 的评审、CI 和版本发布；
- 当前 dbt-doris 已经有一套 v1 Python Adapter 和测试基础，
  dbt Core v1.12 仍支持到 2027-07-15。

一句话概括：

> 现在先把 dbt-doris v1 做好；等 v2 的运行时、Doris Driver 和官方
> 接入路径达到可交付条件后，停止扩展 v1，再把已有产品语义顺序移植到 v2。

## 2. 这里的 v1 和 v2 分别是什么

本文中的 v1、v2 是 dbt Framework 的两代运行时，不是以下版本号：

| 写法 | 实际含义 | 是否代表 dbt v2 |
| --- | --- | :---: |
| `dbt-doris==1.0.0` | Doris Adapter 自身版本 | 否 |
| `config-version: 2` | `dbt_project.yml` 格式版本 | 否 |
| YAML 中的 `version: 2` | Model、Source、Test 等属性文件格式 | 否 |

dbt v1 指 Python 实现的 dbt Core 1.x，以及通过 Python Package
独立安装的数据库 Adapter。当前 dbt-doris 就属于这条路线。

dbt v2 指新的 Rust Engine。它有两个发行形态：

- **dbt Core 2.0**：Apache 2.0 开源发行版；
- **Fusion**：基于同一 Engine 的增强发行版，包含额外产品能力。

两代运行时延续相同的 dbt 项目语言和主要 DAG 语义，但 Adapter 的代码组织、
连接 Driver、执行基础设施和发布方式已经改变。因此 v2 不是给
`dbt-core` 改一个依赖版本就能完成的升级。

## 3. v2 什么时候发布，目前是什么状态

“v2 已经发布”需要拆成几个时间点理解：

| 时间 | 事件 | 准确含义 |
| --- | --- | --- |
| 2025-05-28 | dbt Labs 首次公开 Fusion Engine | 新 Rust Engine 开始公开预览，不是 Core 2.0 GA |
| 2026-06-01 | dbt Labs 公布 v2 Framework 的 Core 2.0 / Fusion 双发行路线，并将共享基础代码以 Apache 2.0 发布到 `dbt-core` | Core 2.0 已有开源代码和 Alpha 版本，但不是正式版 |
| 2026-07-28 | 本文评估日 | Core 2.0 仍为 Alpha，Initial Release 仍为 `TBD`；Fusion 本地 CLI 和编辑器体验仍为 Preview |

所以，截至本文评估日：

> Fusion 从首次公开算起约 14 个月；Core 2.0 开源路线公布不到两个月；
> v2 已经可以预览和贡献代码，但 Core 2.0 正式版尚未发布。

官方对 Core Alpha 的定义是行为和实现都不作兼容承诺，也不用于生产工作。
这与 Fusion Adapter 的 Preview 生命周期不是同一个成熟度概念。

官方当前仍在支持的 v1 版本为：

| dbt Core 版本 | 当前状态 | 支持截止时间 |
| --- | --- | --- |
| 1.12 | Active Support | 2027-07-15 |
| 1.11 | Critical Support | 2026-12-18 |
| 1.10 | Deprecated | 不应继续作为唯一验证基线 |

这给了 dbt-doris 一个明确但有限的 v1 建设窗口：应尽快迁移到
1.11/1.12 验证基线，同时避免继续积累只适用于 Python 内部接口的复杂设计。

## 4. 哪些产品已经有 v2 Adapter

[官方 Fusion 可用性矩阵](https://docs.getdbt.com/docs/fusion/fusion-availability)
当前只列出六个 Adapter：

| Adapter | 生命周期 | 当前边界 |
| --- | --- | --- |
| Snowflake | Preview | 已进入官方 v2 可用矩阵，但 GA 前仍可能变化 |
| BigQuery | Preview | 已进入官方 v2 可用矩阵，但认证方式有明确边界 |
| Databricks | Preview | 已进入官方 v2 可用矩阵，但认证方式有明确边界 |
| Redshift | Preview | 已进入官方 v2 可用矩阵，但仍不是 GA |
| Apache Spark | Beta | 仅 CLI |
| DuckDB | Beta | 仅 CLI |

没有任何一个官方 v2 Adapter 处于 GA。Doris、StarRocks、ClickHouse
和 Trino 都不在这张“可用”矩阵中。

按官方生命周期定义，Preview 表示已支持范围内的功能稳定、具备生产使用条件，
但 GA 前仍可能增加能力或出现不向后兼容的变化；Beta 仍可能不完整或不稳定。
所以这六个产品不是“不可用”，而是成熟度和支持边界尚未全部固定。

同时，官方 v2 Adapter 贡献指南还列出了 Athena、Trino、Starburst、
Dremio、Oracle 等源码占位项。这表示 `AdapterType` 等部分样板已经存在，
但认证、Macro 和 Adapter 分支仍待实现，**不能等同于产品已经可用**。
官方列出的待完善占位项中也没有 Doris。

因此，判断“某产品已经做了 v2”要区分三种状态：

| 状态 | 含义 |
| --- | --- |
| 进入官方可用矩阵 | 用户已经可以按官方边界使用，目前只有上述六个 |
| 源码中存在占位或部分实现 | 方便继续贡献代码，但还不是可用产品 |
| 只有成熟 v1 Adapter | 不能直接被 v2 加载，仍需按新架构移植 |

## 5. dbt-doris 当前实现情况

dbt-doris 不是空白项目，当前已经是一套 v1 Python Adapter：

- 有 Credentials、Connection Manager、Adapter、Relation 和 Column；
- 有 Table、View、Incremental、Partition、Seed、Snapshot、
  Freshness、Catalog 等 Macro；
- 有无集群单元测试和连接真实 Doris 的 Functional Test；
- 当前通过 MySQL 协议和 `mysql-connector-python` 连接 Doris。

但 v1 的基础能力还没有全部完善，主要包括：

- 当前安装基线仍从 `dbt-core>=1.10.4` 起步，且没有 `<2.0` 上限；
- Incremental 的策略名称、实际 SQL 和标准行为仍需统一；
- Snapshot、Contract、Constraint、Grants、Schema Change、
  Freshness 等标准能力覆盖不完整；
- SSL、Timeout、Retry、多 FE Failover、Query ID 和服务端 Cancel
  等生产连接能力仍需补齐；
- Doris Key、Partition、Distribution、Property 等配置还需要形成
  一致、可测试的产品契约。

因此，当前状态更准确的描述是：

> v1 已经有可运行骨架和不少功能，但尚未形成完整、稳定的基础版本。

这也是现在选择 v1 的现实基础：先把已有实现交付出来，同时沉淀未来
v2 最需要复用的 Config 语义、Doris SQL 和黑盒测试。

## 6. 为什么现在选择 v1，而不是直接开发 v2

| 评估项 | 现在直接只开发 v2 | 先完成 v1，再顺序移植 v2 |
| --- | --- | --- |
| 当前代码基础 | 需要进入 Rust 单仓库重新接入 | 可以继续使用现有 Python Adapter |
| 上游稳定性 | Core 2.0 仍为 Alpha，接口和流程可能变化 | v1 Adapter 接口和发布方式成熟 |
| Doris 可用性 | 官方矩阵无 Doris，Driver 和上游合入尚未验证 | 当前已经可以通过 MySQL 协议连接 Doris |
| 用户安装 | 需要等 PR 合入 `dbt-core` 并随官方版本发布 | 可以继续作为独立 Python Package 发布 |
| 短期交付 | 风险较高，可能长期只有 Fork 中的实现 | 可以较快补齐当前用户需要的基础能力 |
| 后续成本 | 少一轮 Python 建设，但现在承担上游和 Driver 不确定性 | Python 接入层以后确定需要重写，但产品语义和测试可复用 |

官方升级文档建议有受支持 Adapter 的新项目使用 v2；这个建议对 Snowflake、
BigQuery 等已进入矩阵的产品成立。Doris 尚未进入矩阵，Adapter 维护者面对的
前提不同，不能把“用户项目应升级 v2”直接等同于“Doris 现在应停止 v1”。

选择 v1 的主要风险是支持窗口有限，并且部分 Python 代码以后不会迁移。
控制方式不是并行开发两套 Adapter，而是：

- v1 只补基础功能和生产可用性，不围绕 Python 私有 API 无限扩展；
- Config 名称、默认值和错误边界尽量保持运行时无关；
- 优先积累从用户输入到 Doris 对象和结果的 Functional Test；
- 每季度复核 v2 状态，并在 2027-01-15 前做一次强制重新选型，
  为 v1.12 支持结束预留至少六个月。

## 7. 当前 v1 应该做到什么程度

v1 的目标是形成稳定可用的基础版本，不是把所有 Doris 高级能力一次做完。

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

当前阶段不同时开发 v2 Adapter，不把 ADBC PoC、Rust Adapter 或 v2 SQL
支持列入近期交付计划。对 v2 只做版本和官方接入状态的定期复核。

## 8. 官方有没有提供从 v1 迁移到 v2 的方法

有，但必须区分“dbt 用户项目升级”和“数据库 Adapter 移植”。

### 8.1 dbt 用户项目升级：有自动辅助工具

[官方 Upgrading to v2 指南](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
提供了：

- `dbt-autofix`：自动修复一部分已废弃项目写法；
- dbt Core 1.12 的 `--use-v2-parser`：在继续使用 v1 执行的同时，
  提前检查 v2 Parser 兼容性；
- Manifest v12 兼容：v1 和 v2 可以使用 State、Defer 等机制进行迁移验证；
- 逐项列出的废弃配置、Jinja、YAML 和 Package 兼容修改。

这些工具处理的是 Model、YAML、Macro、Package 和 Job 等**用户项目代码**，
不会生成 Doris v2 Adapter，也不会把 Python Adapter 自动转成 Rust。

### 8.2 Adapter 实现移植：有官方人工指南，没有自动转换器

官方已经发布
[Contribute a dbt Core 2.0 adapter](https://docs.getdbt.com/guides/adapter-creation-v2?step=1)，
其中专门给出了现有 v1 Adapter 到 v2 的映射和逐文件贡献步骤。

但官方没有提供：

- Python Adapter 到 Rust Adapter 的一键转换工具；
- 可以继续加载 v1 Python Adapter 的兼容层；
- 与 v1 一样通过 `pip`/PyPI 独立安装的外部 v2 Adapter SDK。

v2 的官方社区贡献方式是：

1. 先有可用的 ADBC Driver；
2. 在 `dbt-core` Rust 单仓库中实现 Adapter；
3. 向 dbt Labs 提交 PR；
4. 配合官方完成评审和 CI；
5. PR 合入并随官方版本发布后，用户才能正式使用。

所以“官方提供了迁移方法”的准确含义是：

> 官方提供了清晰的手工移植路线，但不是原地升级，也不是自动转换。

## 9. v1 Adapter 后续怎样移植到 v2

### 9.1 可以复用的资产

| v1 资产 | v2 中的去向 |
| --- | --- |
| Adapter/Catalog 类 Macro（当前主要位于 `macros/adapters/*.sql`，Catalog 在 `metadata.sql`） | 移入 `dbt-loader` 的 Doris Macro；其中 SQL 和 Dispatch 行为通常可直接或小幅调整移植 |
| 自定义 Materialization | 继续使用 Jinja SQL，但要处理 v2 尚不支持或更严格的 Jinja 行为 |
| Profile 字段 | 转成 `dbt-schemas` 中的 Doris `DbConfig` |
| URI、认证和初始化 SQL | 转到 `dbt-auth` |
| Relation 引用和 Quote Policy | 转成 v2 Relation Policy |
| Catalog 和元数据 SQL | 转到 `get_relation` 等元数据实现及相关 Macro |
| Doris Config | 复用名称和行为设计；配置位置、Schema 注册和 Macro 读取 API 需按 v2 验证，不能预先保证用户写法完全不变 |
| Functional Test | 作为 v1/v2 行为等价的验收基线 |

### 9.2 不会原样迁移的代码

| v1 代码 | v2 的处理方式 |
| --- | --- |
| Python Cursor、连接池和连接生命周期 | 由 ADBC/XDBC 负责 |
| `execute`、`add_query` 等执行 Plumbing | 由共享 Rust 执行层负责 |
| Python Adapter 类继承体系 | 改为 Rust 各模块中的 `AdapterType` 分支 |
| `setup.py` 和 PyPI 发布 | 改为合入 `dbt-core` 并由 dbt Labs 随版本发布 |
| MySQL Connector 特有异常和行为 | 按最终 ADBC Driver 的错误、取消和认证模型重新验证 |

因此先做 v1 不会让全部工作作废，但真正能稳定复用的主要是：

> Doris SQL、Macro/Materialization 语义、Config 契约、元数据规则和测试，
> 而不是 Python 类本身。

### 9.3 Doris Driver 的前置条件

官方把 ADBC Driver 列为 v2 Adapter 的硬前置条件：没有 Driver，
就应先停止 Adapter 工作并解决 Driver。

Doris 2.1 起已经支持 Arrow Flight SQL，也可以使用标准 Flight SQL ADBC
Driver，这是未来最值得验证的候选路线，但目前不能直接视为已经满足要求。
迁移时仍需验证：

- dbt v2 接受的 Driver 注册和分发方式；
- DDL、DML、元数据、类型、认证、TLS、Timeout 和 Cancel；
- FE Flight 端口返回 BE Endpoint 后的网络可达性；
- 多 FE、代理/VIP 和生产部署边界。

另外，官方 CDN 已经可以自动分发一部分 Driver，但新增社区 Driver 的通用
签名和自动分发准入机制尚未完成，需要与 dbt Labs 单独协调；未进入 CDN
的 Driver 仍需要用户手工安装。社区 Adapter CI 目前也要与 dbt Labs
Adapter 团队协调。这些都是“现在直接做 v2”仍有交付风险的原因。

## 10. 什么时候开始 v2，届时怎么切换

### 10.1 开始 v2 移植的门槛

满足以下条件后，再停止扩展 v1 并启动 v2：

1. Core 2.0 和社区 Adapter 接口至少进入团队可以承诺兼容的稳定阶段，
   不再以 Alpha 接口作为唯一基础；
2. dbt Labs 明确接受 Doris Adapter 的贡献，并确认评审、CI、合入和
   版本发布路径；
3. 已存在可供 dbt Core v2 注册的 Doris ADBC Driver 候选，
   并且安装和分发路径明确；
4. v1 的基础功能、Config 契约和 Functional Test 已经稳定，可作为迁移基线；
5. 团队有足够时间在当前受支持 v1 版本 EOL 前完成移植和用户迁移。

这里不要求机械地等待所有 v2 功能 GA，但至少不能再被 Adapter 接口、
Driver 和发布路径这三个基础问题阻塞。

### 10.2 顺序迁移步骤

达到门槛后按以下顺序推进：

1. **冻结 v1 功能**：v1 只处理严重缺陷，不再同时增加新能力；
2. **验证 Driver**：确认 Doris ADBC 的连接、执行、元数据、错误和网络行为；
3. **接入 v2 基础模块**：注册 `AdapterType` 和 Driver，接入 Profile、
   Auth、Relation、Column、类型和元数据；
4. **移植 Doris Macro**：移植 Table、View、Incremental、Seed、
   Snapshot、Catalog 及 Doris Config；
5. **做行为等价验证**：复用 v1 Functional Test，对比 Doris 对象、
   数据结果、错误和 Artifact；
6. **准备安装文档**：说明 `profiles.yml`、Driver 共享库的准确名称、
   获取方式和安装位置；
7. **走官方发布流程**：提交 `dbt-core` PR，配合 dbt Labs Adapter
   团队运行社区无法独立执行的 CI，等待合入和发布；
8. **迁移用户项目**：使用 `dbt-autofix`、`--use-v2-parser` 和官方升级指南
   处理项目级兼容问题；
9. **切换默认版本**：v2 达到基础能力等价且迁移、回退文档就绪后，
   再把 v2 设为默认。

迁移末期可以有一段短期 v1/v2 双运行来做验收和回退验证，
但这不是现在同时维护两条开发主线。

## 11. 最终决策表

| 问题 | 结论 |
| --- | --- |
| 现在要直接开发 v2 吗？ | 不要。现阶段只完善 v1，不同时启动 v2 Adapter 开发 |
| v2 什么时候发布的？ | Fusion 于 2025-05-28 首次公开；Core 2.0 开源双发行路线于 2026-06-01 公布；截至 2026-07-28 Core 2.0 仍为 Alpha，GA 日期为 `TBD` |
| 哪些产品已经有 v2？ | 官方可用矩阵只有 Snowflake、BigQuery、Databricks、Redshift、Spark 和 DuckDB，且都仍是 Preview/Beta |
| Doris 当前有 v2 吗？ | 没有，不在官方可用矩阵，官方列出的待完善占位项也没有 Doris |
| 为什么还要做 v1？ | 当前已有 v1 实现和用户交付路径，v1.12 支持到 2027-07-15；先补基础功能能形成稳定产品和迁移基线 |
| 官方能自动迁移 dbt 项目吗？ | 能自动辅助一部分，主要是 `dbt-autofix`、`--use-v2-parser` 和兼容检查 |
| 官方能自动迁移 Adapter 吗？ | 不能。官方有人工移植指南，但 Python Adapter 需要按 Rust 新架构重新接入 |
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
