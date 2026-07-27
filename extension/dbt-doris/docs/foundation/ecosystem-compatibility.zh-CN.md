# dbt-doris 基础功能实施方案：生态兼容

> 状态：实施方案。本文的“目标”和“建议”不代表当前已经支持，当前能力以第 2 节为准。

## 1. 目标

“能运行几个 Doris 自有测试”不等于 dbt Adapter 已经兼容 dbt 生态。
生态兼容需要回答：

- 哪些 Python、dbt Core、Connector 和 Doris 版本经过验证；
- dbt 官方定义的 Adapter 标准语义是否通过；
- 常用跨数据库 Macro 和 Package 能否在 Doris 上运行；
- Adapter 对 dbt 声明了哪些 Capability；
- 发布包能否安装、升级和复现测试结果。

本阶段的产物不是一句“支持 dbt v1”，而是一张可执行、可持续维护的兼容矩阵。

## 2. 当前基础

### 2.1 已有测试

当前工作区有：

- 60 个不需要 Doris 集群的 Macro 单元测试；
- 基础 Table、View、Incremental、Ephemeral、Seed、Snapshot、Test 等 Functional；
- Doris 自有的 Partition、Contract、Freshness、Hook 和 Unit Test 用例；
- 部分 `dbt-tests-adapter` Basic Base Test。

现有测试入口见 [`test/`](../../test/)；后续应由 CI 结果和发布兼容矩阵提供
可重复的验证证据。

### 2.2 当前版本配置

| 项目 | 当前配置 |
| --- | --- |
| dbt-doris | `1.0.0` |
| Python | `>=3.9` |
| dbt Core | 安装要求 `>=1.10.4`，开发环境固定在 1.10 系列 |
| MySQL Connector | 运行要求 `>=8.0.0`，开发环境 Floor 为 8.0.33 |

当前 `dbt-core>=1.10.4` 没有上界，会让未来不兼容的 dbt Core 主版本也满足安装条件。
在 dbt v2 Adapter 路线独立完成前，发布依赖必须限制在经过验证的 v1 范围。

### 2.3 当前缺口

- 官方 Incremental、Grants、Constraints、Persist Docs、Snapshot、Catalog、
  Store Failures 和 Unit Testing 套件未系统接入；
- `utils` 跨数据库 Macro 未系统验证；
- `dbt_utils` 等 Package 没有正式兼容清单；
- 当前 Adapter 没有 `_capabilities` 声明；
- 无集群 CI 已有工作区方案，但还没有真实 Workflow 跑绿证据；
- 没有自动启动 Doris 的 Functional CI；
- Tox 声明的 Python 矩阵没有全部实跑；
- Wheel/Sdist 安装和发布 Smoke Test 不完整。

## 3. 建立兼容矩阵

### 3.1 矩阵维度

| 维度 | 至少覆盖 |
| --- | --- |
| Python | 最低支持版本、主力版本、最高验证版本 |
| dbt Core | 最低支持 Minor、当前主力 Minor、下一 Minor 预兼容 |
| dbt-tests-adapter | 与每个 dbt Core 测试环境独立 Pin |
| MySQL Connector | 最低版本和最新兼容版本 |
| Doris | 最低支持版本、当前稳定版本和开发分支 |
| 部署 | 单 FE/BE、生产多 FE/BE 的关键连接场景 |

### 3.2 第一版门禁

第一版建议把以下组合作为发布门禁：

```text
Python 3.9  + dbt Core 1.10.x + Connector Floor + Doris Minimum
Python 3.12 + dbt Core 1.10.x + Connector Latest + Doris Stable
```

其他组合可以标记为：

- Supported and Tested；
- Supported, Not Every Release Tested；
- Experimental；
- Unsupported。

如果原生 Insert Overwrite 被纳入基础 Incremental，Doris 最低版本必须满足该 SQL
能力，或者 Adapter 对低版本明确禁用该策略。

### 3.3 依赖约束

发布依赖应表达真实范围，例如：

```text
dbt-core >= 已验证最低版本, < 未适配的下一主版本
mysql-connector-python >= 安全和兼容 Floor, < 已知不兼容版本
```

开发依赖、Tox、CI 和 `setup.py` 必须来自同一组版本常量或自动一致性测试，
不能各写一套。

## 4. 接入 dbt 官方 Adapter 测试

按基础功能依赖顺序接入：

| 阶段 | 官方测试领域 | 作用 |
| --- | --- | --- |
| A1 | Basic、Relations、Hooks、Caching | 保住 Adapter 基础骨架 |
| A2 | Incremental | Unique Key、Schema Change、Predicates |
| A3 | Simple Snapshot | Check 和 Timestamp |
| A4 | Persist Docs、Catalog | 文档和元数据 |
| A5 | Constraints、Unit Testing | Contract、类型和 Model SQL |
| A6 | Store Test Failures | Test 失败记录落表 |
| A7 | Grants | Model、Seed、Snapshot、Incremental 权限 |
| A8 | Concurrency | 连接与多线程边界 |

接入原则：

- 优先继承上游 Base Test，不复制测试内容；
- 只有 Doris 必需的差异才覆写 Fixture；
- 每个 Skip 都写明原因和解除条件；
- 记录 `dbt-tests-adapter` 版本，不能只记录用例数量；
- 上游升级时审阅新增和改变的 Base Test。

## 5. 验证跨数据库 Utils

`dbt-tests-adapter` 的 `utils` 测试覆盖常用跨库函数。建议按类别接入：

### 5.1 第一批

- `current_timestamp`；
- `dateadd`、`datediff`、`date_trunc`；
- `cast`、`safe_cast`；
- `concat`、`replace`、`length`、`position`；
- `hash`；
- `listagg`；
- `bool_or`；
- `any_value`。

### 5.2 第二批

- `date_spine`；
- `generate_series`；
- `split_part`；
- `last_day`；
- `array_construct`、`array_append`、`array_concat`；
- `intersect`、`except`；
- Timestamp 和主要 Doris 数据类型。

实现方式：

1. 先运行 dbt 默认 Macro；
2. 默认 SQL 能被 Doris 正确执行就直接复用；
3. 只有语法或结果语义不同才增加 `doris__` Macro；
4. 每个 Doris 覆写都同时有输入、输出和 NULL 边界测试。

## 6. 常用 Package 兼容

第一阶段选择 `dbt_utils`，覆盖真实项目常用能力：

- `generate_surrogate_key`；
- `union_relations`；
- `star`；
- `date_spine`；
- `get_column_values`；
- `expression_is_true`；
- `unique_combination_of_columns`；
- `relationships_where`。

建立独立示例项目：

```text
Seed / Source
    |
使用 dbt_utils 构建 Model
    |
运行 Package Data Test
    |
生成 Docs 和 Artifact
```

兼容清单必须记录：

| Package | 版本 | 能力 | 结果 | 限制 |
| --- | --- | --- | --- | --- |
| dbt_utils | 固定版本 | generate_surrogate_key | Pass/Fail | 类型限制 |

不应笼统写“支持 dbt_utils”；只声明实际执行过的 Macro 和 Test。

## 7. Adapter Capability

当前所有 Capability 都是 Unknown。应按实现进度逐项声明：

| Capability | 声明前置条件 |
| --- | --- |
| `SchemaMetadataByRelations` | 能按 Relation 批量取字段并通过缓存测试 |
| `GetCatalogForSingleRelation` | 单 Relation Catalog 接口通过 |
| `TableLastModifiedMetadata` | 单表元数据 Freshness 通过 |
| `TableLastModifiedMetadataBatch` | 批量 Freshness 通过 |
| `MicrobatchConcurrency` | Microbatch 并发语义实现并验证 |
| `CatalogsV2` | 三层 Catalog Relation 完整实现 |

流程固定为：

```text
实现 -> 单元测试 -> 真实 Doris 测试 -> 声明 Capability
```

Unknown 不能仅为了消除警告改成 Supported。

## 8. CI 分层

### 8.1 每个 PR

不启动 Doris：

- License；
- Flake8；
- Macro 解析和行为单测；
- 三处版本一致性；
- 依赖 Dry-run；
- Build Wheel/Sdist；
- 在干净环境安装 Wheel；
- Python 最低和最高版本。

### 8.2 Functional CI

启动单 FE/BE Doris：

- 健康检查；
- 注册 Backend；
- 设置单副本测试配置；
- 运行全部 Functional 和官方 Adapter 套件；
- 失败时收集 FE、BE、dbt 日志和 Artifact；
- 清理进程和临时 Database。

### 8.3 定时兼容 CI

每日或每周运行：

- dbt Core 支持范围内各 Minor；
- Connector Floor 和 Latest；
- Doris 最低支持版、稳定版和开发分支；
- `dbt_utils` 示例项目；
- 多线程；
- 大规模 Catalog。

## 9. 发布工程

发布前自动验证：

1. `setup.py`、Python `__version__` 和 Adapter Project 版本一致；
2. Wheel 与 Sdist 都能构建；
3. 在空虚拟环境中安装 Wheel；
4. `dbt --version` 能发现 Doris Plugin；
5. `dbt debug` 能读取 Profile；
6. 最小 Seed、Run、Test、Snapshot 和 Docs 流程通过；
7. Changelog、Release Note 和兼容矩阵已更新；
8. 不包含测试缓存、凭据和本地配置。

版本行为变化，例如重新定义 `insert_overwrite`，必须按兼容策略发布，不能只更新
实现而不写迁移说明。

## 10. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| E1 | 固化版本上下界和一致性检查 | 安装范围不再接受未适配主版本 |
| E2 | 跑通无集群 PR CI | Python Floor/Ceiling 均有真实结果 |
| E3 | 建立单 FE/BE Functional CI | 官方和 Doris 自有测试自动运行 |
| E4 | 分领域接入官方 Adapter 套件 | Skip 有明确原因和解除条件 |
| E5 | 接入 Utils 测试 | 第一、二批函数有结果矩阵 |
| E6 | 建立 dbt_utils 示例项目 | 常用 Macro/Test 有明确兼容清单 |
| E7 | 按实现声明 Capability | 声明与测试一致 |
| E8 | 建立发布 Smoke Test | Wheel/Sdist 和最小项目可重复验证 |

## 11. 完成定义

- 公开 Python、dbt Core、Connector 和 Doris 兼容矩阵；
- 每个发布组合都有自动化证据；
- dbt 官方基础套件持续通过；
- 常用 Utils 和 Package 按功能声明兼容；
- Capability 不再全部 Unknown，且没有虚假 Supported；
- 发布包可在干净环境安装并完成最小端到端工作流；
- dbt v2 仍作为独立路线，不被当前 v1 依赖范围误接入。
