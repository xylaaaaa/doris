# dbt-doris v1 / v2 技术选型与迁移评估

> 文档状态：技术选型建议稿
>
> 评估日期：2026-07-27
>
> 适用范围：Apache Doris `extension/dbt-doris` 当前 `master` 工作区

## 1. 结论

当前不建议停止 v1、把全部资源直接投入 v2，也不建议只维护 v1 而暂不研究 v2。

建议采用双轨方案：

1. **v1 作为近期交付主线**：把现有 Python Adapter 补到基础功能完整、
   语义清晰、测试和版本边界明确，使用户现在就能稳定使用 Doris；
2. **v2 作为独立验证路线**：立即启动最小可行性验证，但在官方第三方
   Adapter 接入方式、Doris Driver 和 SQL Dialect 没有验证完成前，
   不承诺替代 v1；
3. **共享产品语义和测试**：Table、View、Incremental、Snapshot、
   Doris Key/Partition/Distribution 等行为先定义成与运行时无关的规范，
   v1 和 v2 分别实现，避免将来重新设计；
4. **达到迁移门槛后再切换主线**：只有 v2 的上游接入路径明确、
   Doris 最小链路通过、基础能力与 v1 对齐，才把 v2 从 PoC 升级为正式产品。

一句话概括：

> 继续实现 v1，但只建设可迁移的标准能力；同时现在就验证 v2，
> 不把 v2 的上游不确定性变成 dbt-doris 当前无法交付的理由。

## 2. 这里的 v1 和 v2 分别是什么

本文中的 v1、v2 是 dbt Framework 的两代运行时，不是以下版本号：

| 写法 | 实际含义 | 是否代表 dbt v2 |
| --- | --- | :---: |
| `dbt-doris==1.0.0` | Doris Adapter 自身版本 | 否 |
| `config-version: 2` | `dbt_project.yml` 格式版本 | 否 |
| YAML 中的 `version: 2` | Model、Source、Test 等属性文件格式 | 否 |

dbt v1 指 Python 实现的 dbt Core 1.x，以及通过 Python Package 安装的
数据库 Adapter。当前 dbt-doris 就属于这条路线。

dbt v2 指新的 Rust Engine。官方把它分成两个发行形态：

- dbt Core 2.0：Apache 2.0 开源基础运行时；
- Fusion：建立在同一运行时之上，增加完整产品体验和部分登录后能力。

两代运行时继续使用相同的 dbt 项目语言和主要 DAG 语义，但 Adapter
实现方式、连接 Driver、SQL Dialect、错误校验和部分运行行为已经变化。

## 3. v2 到底发布多久了

“v2 已经发布”需要拆成三个时间点理解：

| 时间 | 事件 | 截至 2026-07-27 的含义 |
| --- | --- | --- |
| 2025-05-28 | dbt Labs [首次公开 Fusion Engine](https://github.com/dbt-labs/dbt-fusion)，并发布初始代码和 Snowflake Driver | 距今 425 天；这是新 Engine 首次公开，不是 Core 2.0 GA |
| 2026-06-01 | dbt Labs [公布 v2 Framework、Core 2.0 与 Fusion 双发行路线](https://www.getdbt.com/licenses-faq)，并把相应基础代码以 Apache 2.0 发布 | 距今 56 天；可以开始研究和构建，但不等于 2.0 正式版已经 GA |
| 尚未确定 | dbt Core 2.0 正式版 | [官方版本页](https://docs.getdbt.com/docs/dbt-versions)仍把 Initial Release 标为 `TBD`，生命周期为 Alpha |

截至评估日：

- 官方 Fusion Release 页面显示的本地稳定通道版本是
  `v2.0.0-preview.202`；
- 本地 CLI、VS Code 等 Fusion 使用方式仍标为 Preview；
- dbt Core 2.0 正式版发布日期仍未确定；
- 官方仍在补齐 GA 前的功能和兼容性。

因此，更准确的表述是：

> dbt v2 已经公开并可预览使用，但 Core 2.0 正式版尚未 GA。

这意味着现在适合做 v2 技术验证和早期 Adapter 协作，
但不适合假设接口和发布方式已经完全稳定。

## 4. v1 和 v2 的主要区别

| 对比项 | dbt Core v1.x | dbt v2 / Fusion |
| --- | --- | --- |
| Engine | Python | Rust |
| 安装形态 | `dbt-core`、Python Adapter 和数据库 Connector 组合安装 | 编译后的 v2/Fusion Runtime，配套 Driver 和 Adapter 组件 |
| Adapter | Python Credentials、Connection Manager、Adapter、Relation、Column，加 Jinja Macro | 新 Driver、Adapter 和 SQL Dialect 组件；官方实现主要使用 Arrow/ADBC/ODBC 等连接基础 |
| SQL 处理 | 主要渲染 Jinja，很多 SQL 错误由数据库执行时发现 | Engine 原生理解支持的 SQL Dialect，可提前做语法、类型和影响分析 |
| 项目语言 | Model、Source、`ref()`、Test、Macro 和 YAML | 主要项目语言与 DAG 语义保留，但校验更严格 |
| 已废弃能力 | 一部分历史写法仍能运行或只告警 | 已废弃写法被移除，错误通常更早暴露 |
| Artifact | 生成 Manifest、Run Results 等 | Fusion 生成兼容 Core 的 Manifest v12，并增加可选字段 |
| 生态成熟度 | Adapter 和 Package 生态完整，第三方 Adapter 开发流程成熟 | 官方 Adapter 数量仍少，第三方 Adapter 的动态接入和发布流程尚未达到 v1 成熟度 |

对 dbt 项目用户来说，迁移重点是修复废弃配置、严格校验和不兼容 Package。

对 dbt-doris 维护者来说，迁移重点不是改一个依赖版本，而是重新接入：

```text
Driver
连接、认证、执行、取数、取消
        |
        v
Adapter
Relation、Schema、元数据、Materialization 和 Config
        |
        v
SQL Dialect
Doris 语法、类型、函数、引用和静态分析
```

## 5. 目前哪些产品已经有 v2 Adapter

[官方 v2/Fusion 可用性矩阵](https://docs.getdbt.com/docs/fusion/fusion-availability)
当前只列出六个 Adapter：

| Adapter | 官方生命周期 | 使用边界 |
| --- | --- | --- |
| Snowflake | Preview | 本地和 dbt Platform 的生命周期可能不同 |
| BigQuery | Preview | 仅支持官方列出的认证方式 |
| Databricks | Preview | 仅支持官方列出的认证方式 |
| Redshift | Preview | 仅支持官方列出的认证方式 |
| Apache Spark | Beta | 仅 CLI |
| DuckDB | Beta | 仅 CLI |

这里的 Preview 表示官方认为已支持的功能可以用于生产验证，
但 GA 前仍可能增加能力或发生不向后兼容的变化；Beta 的稳定性更低。

如果只看串讲文档中对比的六个产品：

| 产品 | 是否进入官方 v2 矩阵 |
| --- | :---: |
| Snowflake | 是 |
| Databricks | 是 |
| BigQuery | 是 |
| StarRocks | 否 |
| ClickHouse | 否 |
| Trino | 否 |

Doris、StarRocks、ClickHouse 和 Trino 都不在当前官方矩阵中。
这说明 v2 还不是所有成熟 Python Adapter 都能自行升级后直接接入的阶段。

还要注意：

> “Snowflake 已支持 v2”不代表原来的 `dbt-snowflake` Python 代码原样迁移了。
> v2 使用的是新 Engine 中对应的 Driver、Adapter 和 Dialect 实现。

## 6. dbt-doris 当前站在哪里

当前工作区已经是一套可运行的 v1 Python Adapter：

- `setup.py` 依赖 `dbt-core>=1.10.4` 和 `mysql-connector-python`；
- `connections.py` 实现 Credentials、连接、执行、取消等行为；
- `impl.py`、`relation.py` 和 `column.py` 实现 Doris 对象映射和元数据接口；
- Jinja Macro 实现 Table、View、Incremental、Partition、Seed、
  Snapshot、Freshness 和 Catalog SQL；
- 已有无集群单元测试和真实 Doris Functional 测试。

当前已经投入的工作不是应该丢弃的旧实现。它至少沉淀了三类可以迁移的资产：

1. Doris 应该如何创建和替换 Table、View、Incremental、Snapshot；
2. Doris Key、Partition、Distribution、Property 等 Config 应该表达什么语义；
3. 哪些输入、SQL、运行结果和异常行为需要测试。

但当前 v1 还没有达到可以停止建设的状态：

- Incremental 策略命名和实际行为仍有不一致；
- Snapshot、Contract、Freshness 等只覆盖部分主路径；
- Grants、Schema Change、标准 Adapter Capability 尚未补齐；
- SSL、Timeout、Retry、多 FE 和服务端 Cancel 等生产连接能力尚未完善；
- 只完整验证过 dbt Core 1.10.4，而官方已经把 1.10 标为 Deprecated。

官方当前仍支持：

| dbt Core v1 版本 | 官方状态 | 支持截止时间 |
| --- | --- | --- |
| 1.12 | Active Support | 2027-07-15 |
| 1.11 | Active Support | 2026-12-18 |
| 1.10 | Deprecated | 已不应作为唯一验证基线 |

因此 v1 不是一个已经没有用户价值的历史版本。
dbt-doris 应把验证主线升级到 1.11/1.12，并在测试通过后声明明确的
`<2.0` 上限，而不是继续使用没有上限的 `dbt-core>=1.10.4`。

## 7. 三种开发路线比较

### 7.1 方案 A：停止 v1，直接只开发 v2

优点：

- 新代码直接面向长期架构；
- 不需要长期维护两套 Runtime 接入；
- 可以更早使用 Rust Engine、SQL Dialect 和 Arrow 生态。

主要问题：

- Core 2.0 正式版尚未 GA，接口仍可能变化；
- Doris 不在官方 v2 Adapter 列表；
- 官方关于[第三方自定义 Adapter 的接入议题](https://github.com/dbt-labs/dbt-fusion/issues/46)
  仍未关闭，
  尚无与 v1 Python Plugin 等价的成熟发布路径；
- 即使自己 Fork v2 Engine 加入 Doris，也会承担长期同步上游的成本；
- 在 v2 可供普通 Doris 用户安装前，团队没有可交付的稳定 Adapter；
- 当前 v1 用户无法获得急需的基础语义和可靠性修复。

结论：**不建议作为当前主线。**

### 7.2 方案 B：只完善 v1，暂不投入 v2

优点：

- 开发接口成熟，现有代码和测试可以直接演进；
- 可以较快向当前用户交付；
- 第三方 Adapter 的开发、测试和安装路径已经明确。

主要问题：

- 无法提前发现 Doris Driver、Dialect 和 v2 Adapter API 的真实缺口；
- 等 v2 生态成熟后才开始，可能出现迁移时间不足；
- 容易继续积累 Python Core 内部接口和 Connector 特有实现。

结论：**适合作为近期交付路线，但不能作为唯一长期路线。**

### 7.3 方案 C：v1 主线交付，v2 并行验证

| 维度 | 评估 |
| --- | --- |
| 当前用户价值 | v1 可以持续交付 |
| 长期方向 | v2 现在开始消除关键不确定性 |
| 重复工作 | 通过共享语义、SQL 和测试控制 |
| 上游风险 | v2 尚未具备发布条件时，不影响 v1 使用 |
| 切换成本 | 达到能力对齐后逐步迁移，不做一次性重写切换 |

结论：**推荐采用。**

这里的“双轨”不是同时完整实现两套所有功能。
主力资源放在 v1 基础能力，v2 前期只做能够回答关键问题的垂直链路。

## 8. v1 还需要实现到什么程度

继续做 v1 的目标不是追求无限功能，而是形成稳定的兼容基线。

### 8.1 v1 必须补齐

1. **版本基线**
   - 在 dbt Core 1.11 和 1.12 上运行单测和官方 Adapter Functional Test；
   - 根据实测结果声明支持范围，并限制 `<2.0`；
   - 建立 Python、dbt Core、Connector 和 Doris 的 CI 矩阵。

2. **基础 Materialization**
   - Table、View、Incremental、Seed、Snapshot 的首次构建、重复构建、
     Full Refresh 和失败边界明确；
   - 修正 Incremental 策略名称与实际 SQL 不一致的问题；
   - 补齐 `on_schema_change` 等标准行为。

3. **标准 dbt 能力**
   - Data Test、Unit Test、Docs、Catalog、Persist Docs；
   - Contract、Constraint、Grants、Freshness；
   - Adapter Capability 和官方 Adapter 测试。

4. **运行可靠性**
   - SSL、Timeout、Retry、多 FE Failover；
   - Query ID、正确取消服务端查询；
   - 清楚区分连接失败、查询失败和可安全重试。

5. **Doris 基础建模**
   - 统一 Duplicate、Unique、Aggregate 表模型配置；
   - Partition、Distribution、Bucket 和常用 Property；
   - 保证 Table、Incremental 和 Full Refresh 使用相同表配置。

### 8.2 v1 不需要为了迁移而过度投入

以下工作不应成为 v2 PoC 的前置条件：

- 围绕 dbt Core Python 私有 API 开发大量定制能力；
- 对 `mysql-connector-python` 做只适用于单一 Driver 的复杂封装；
- 一次性接入所有 Doris 高级对象；
- 在基础语义未稳定时继续增加含义模糊的 Config；
- 为了代码复用而把 v2 设计强行做成 v1 Python 类的翻译。

Async Materialized View、Microbatch、External Catalog 等高级能力可以根据
用户优先级继续在 v1 实现，但要先定义运行时无关的产品语义和测试用例。

## 9. v2 应该怎样开始

Doris 已经提供一个有价值的 v2 前置条件：

- Doris 2.1 起支持
  [Arrow Flight SQL](https://doris.apache.org/docs/3.0/db-connect/arrow-flight-sql-connect)；
- 官方 Doris 文档给出了通过标准 Flight SQL ADBC Driver
  执行 DDL、DML、Session Variable、`SHOW` 和查询的示例；
- 这与 v2 使用 Arrow/ADBC 等连接组件的方向一致。

因此 v2 第一阶段不需要先发明新的网络协议，可以优先验证
Flight SQL ADBC 路线。

但 Flight SQL 可连接不等于 dbt Adapter 已完成，还必须验证：

| 层次 | 最小验证内容 |
| --- | --- |
| Driver | 用户名密码、TLS、超时、执行、批量取数、错误、取消 |
| 网络 | FE Flight 端口、BE Endpoint、代理/VIP、多 FE、外网访问边界 |
| 元数据 | Catalog、Database、Table、View、Column、类型和大小写 |
| DDL/DML | Create/Drop/Rename、CTAS、Insert、分区覆盖、事务边界 |
| Dialect | Doris 引号、数据类型、函数、Key/Partition/Distribution 语法 |
| Adapter | Relation、Schema 生命周期、Materialization、Config 和 Macro |
| 发布 | v2 如何发现、签名、下载和加载 Doris Driver/Adapter |

Flight SQL 需要客户端能够访问返回结果的 BE Endpoint，
这与当前 v1 只连接 FE MySQL Query Port 的网络模型不同。
PoC 必须覆盖内网、代理/VIP 和 TLS 场景，不能只验证本机单节点。

建议第一个 v2 垂直链路只回答以下问题：

```text
dbt debug
    |
创建 Database
    |
Table / View 首次构建和重复构建
    |
Seed
    |
Generic / Singular Data Test
    |
基础 Incremental 第二次运行和 Full Refresh
    |
Snapshot
    |
Catalog / Docs 元数据
```

如果这条链路不能以官方可接受的 Adapter 方式安装和运行，
就不应继续扩展 v2 高级能力。

## 10. 从 v1 迁移到 v2，哪些能复用

### 10.1 可以直接或经过小幅调整复用

| 资产 | 迁移方式 |
| --- | --- |
| dbt 项目结构 | 保留 Model、Source、Test、Seed、Snapshot 和依赖关系 |
| 用户 Config | 保持名称和语义稳定，适配层分别翻译 |
| Doris SQL | 复用已经验证的 DDL、DML 和元数据查询语义 |
| Jinja Macro | 优先保持标准 Macro 接口；根据 v2 Adapter API 调整调用点 |
| Functional Test 场景 | 同一输入、操作和预期结果分别在 v1/v2 执行 |
| 文档 | 共用能力定义，分别标记运行时和版本边界 |
| Artifact | 利用官方的 Manifest v12 兼容性做并行和状态迁移验证 |

### 10.2 需要重新实现

| v1 组件 | v2 对应工作 |
| --- | --- |
| Python `DorisCredentials` | v2 Profile、认证和 Driver 配置 |
| `DorisConnectionManager` | ADBC/Flight SQL 或最终选定 Driver 的连接实现 |
| Python `DorisAdapter` | v2 Adapter 接口和注册 |
| `DorisRelation`、`DorisColumn` | v2 对象、类型和引用模型 |
| MySQL Connector 异常处理 | v2 Driver 错误、Retry、Cancel 和 Query ID |
| Python Package 发布 | v2 Driver/Adapter 的构建、签名、发现和分发 |
| 无 Doris Dialect 的通用解析 | Doris SQL Grammar、类型和函数支持 |

因此“先实现 v1”不会导致所有代码重写，
但也不能期待 Python Adapter 类原封不动进入 v2。

最重要的复用资产不是 Python 类，而是：

> 稳定的用户配置、明确的 Doris 行为、可执行的兼容测试。

## 11. 推荐迁移阶段

### 阶段 0：冻结共享语义

产出：

- v1/v2 共用能力矩阵；
- Config 名称、默认值和错误边界；
- Table、View、Incremental、Snapshot 的行为规范；
- 可在两套 Runtime 运行的测试场景。

完成标准：同一个配置不会在 v1 和未来 v2 表达两个不同含义。

### 阶段 1：完成 v1 生产基线

产出：

- dbt Core 1.11/1.12 兼容矩阵；
- 基础能力和标准 Adapter Test；
- 正确的 Incremental、Snapshot、Contract、Grants 和 Freshness 边界；
- 安装、升级和限制文档。

完成标准：v1 能作为 v2 开发期间的稳定用户版本。

### 阶段 2：完成 v2 技术 PoC

产出：

- 与 dbt Labs 确认第三方 Adapter 的接入和发布方式；
- Doris Flight SQL ADBC 连接验证；
- 最小 Doris SQL Dialect；
- `dbt debug -> run -> test -> docs` 垂直链路；
- 网络、安全、版本和性能报告。

完成标准：不是本地 Fork 中“能跑一次”，而是存在可维护、可分发的上游路径。

### 阶段 3：实现 v2 基础能力对齐

产出：

- Relation、Schema、Table、View、Seed、Test、Docs；
- Incremental、Snapshot 和 Full Refresh；
- Doris 基础 Table Config；
- 与 v1 共用的行为测试全部通过。

完成标准：同一个样例项目在 v1/v2 产生等价 Doris 对象和数据结果。

### 阶段 4：双运行预览

做法：

- v1 保持默认；
- v2 作为 Preview；
- 选取真实项目同时执行 Parse、Compile 和非生产 Target 构建；
- 对比 SQL、Relation、数据结果、Artifact、性能和错误行为；
- 建立已知不兼容清单和迁移工具。

完成标准：没有阻断常用项目迁移的未知差异。

### 阶段 5：切换默认版本

只有同时满足以下条件才切换：

1. Doris 进入官方 v2 Adapter 可用路径，或第三方 Adapter
   安装和发布机制已经正式可用；
2. v2 Runtime 和 Doris Adapter 的生命周期适合生产使用；
3. v1 基础兼容测试在 v2 全部通过；
4. 常用 Package、认证和部署方式已经验证；
5. 已发布用户迁移文档、回退方式和版本支持政策。

切换后建议让 v1 进入 12 个月维护期，只处理严重缺陷和安全问题，
再根据用户采用率决定停止维护时间。

## 12. 近期可执行工作

| 优先级 | 工作 | 目的 |
| --- | --- | --- |
| P0 | 把 v1 CI 升级到 dbt Core 1.11/1.12 | 离开已经 Deprecated 的唯一测试基线 |
| P0 | 修正 v1 Incremental 名称和语义 | 形成可以迁移的稳定产品契约 |
| P0 | 向 dbt Labs 确认 Doris v2 Adapter 的贡献、签名和发布路径 | 消除最大的非技术阻塞 |
| P0 | 用 Doris Flight SQL + ADBC 做连接与元数据 PoC | 验证 v2 Driver 基础 |
| P1 | 建立 v1/v2 共用 Adapter 行为测试清单 | 控制重复开发 |
| P1 | 实现最小 Doris SQL Dialect | 验证引用、类型、函数和 DDL 解析 |
| P1 | 跑通 v2 `debug/run/test/docs` | 判断是否可以进入正式开发 |
| P2 | 完成 v2 Incremental、Snapshot 和 Doris Table Config | 达到基础能力对齐 |
| P2 | 开始真实项目双运行 | 为默认版本切换提供证据 |

建议每季度重新核对一次：

- Core 2.0 是否 GA；
- 官方 Adapter 矩阵是否增加 Doris 或第三方 Adapter；
- v2 Adapter API 和 Driver 发布方式是否稳定；
- v1 最新支持版本及其 EOL 时间；
- v2 PoC 的阻塞是否已经解除。

## 13. 最终决策

| 问题 | 结论 |
| --- | --- |
| 现在直接只开发 v2 吗？ | 不建议。v2/Core 2.0 尚未 GA，Doris 不在官方 Adapter 矩阵，第三方接入路径仍不成熟 |
| v2 发布多久了？ | Fusion 首次公开 425 天；v2/Core 2.0 路线公开 56 天；Core 2.0 正式版仍未发布 |
| 哪些产品已有 v2？ | Snowflake、BigQuery、Databricks、Redshift 为 Preview；Spark、DuckDB 为 CLI Beta |
| 还有必要实现 v1 吗？ | 有。当前代码、用户和成熟 Adapter 生态都在 v1，且 1.11/1.12 仍受官方支持 |
| v1 要实现到什么程度？ | 补齐基础标准能力、Doris 基础建模、生产可靠性和兼容测试，不围绕 Python 私有接口无限扩展 |
| v1 后续怎样迁移？ | 保持 Config/SQL/测试稳定，重写 Driver、Adapter 和 Dialect；通过 PoC、能力对齐、双运行后再切换 |

## 14. 资料来源

dbt 官方资料：

- [About the dbt Fusion engine](https://docs.getdbt.com/docs/fusion/about-fusion)
- [About dbt versions](https://docs.getdbt.com/docs/dbt-versions)
- [Fusion availability](https://docs.getdbt.com/docs/fusion/fusion-availability)
- [Fusion releases](https://docs.getdbt.com/docs/fusion/fusion-releases)
- [Supported features](https://docs.getdbt.com/docs/fusion/supported-features)
- [Upgrading to v2](https://docs.getdbt.com/docs/dbt-versions/core-upgrade/upgrading-to-v2)
- [Product lifecycles](https://docs.getdbt.com/docs/dbt-versions/product-lifecycles)
- [dbt Licensing FAQ](https://www.getdbt.com/licenses-faq)
- [dbt Fusion Engine repository and release timeline](https://github.com/dbt-labs/dbt-fusion)
- [Support for custom adapters](https://github.com/dbt-labs/dbt-fusion/issues/46)

Doris 与当前实现：

- [Doris Arrow Flight SQL](https://doris.apache.org/docs/3.0/db-connect/arrow-flight-sql-connect)
- [当前 dbt-doris Python Adapter](../dbt/adapters/doris/)
- [当前 dbt-doris Macro](../dbt/include/doris/macros/)
- [当前 dbt-doris 测试](../test/)
