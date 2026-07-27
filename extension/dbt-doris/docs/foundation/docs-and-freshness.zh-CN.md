# dbt-doris 基础功能实施方案：Docs 与 Freshness

> 状态：实施方案。本文的“目标”和“建议”不代表当前已经支持，当前能力以第 2 节为准。

## 1. 目标

这项工作包含两个相邻但不同的能力：

| 能力 | 用户问题 |
| --- | --- |
| Persist Docs / Catalog | 这张表和字段是什么意思，dbt 文档里能否看到真实 Doris 元数据 |
| Source Freshness | 上游数据最后什么时候更新，现在是否已经过期 |

目标是让 Table、View、Incremental、Seed 和 Snapshot 的说明持久化行为一致，
让 `dbt docs generate` 返回准确且高效的 Catalog，并同时支持字段时间与 Doris
元数据时间两种 Freshness。

## 2. 当前实现

### 2.1 Persist Docs

当前已经有：

- Table 的 Relation Comment；
- Table 的 Column Comment；
- 注释中的单引号和反斜杠转义；
- View 跳过 Doris 不支持的 `ALTER ... MODIFY COMMENT`；
- Table 字段说明真实 Doris 测试。

实现位于
[`columns.sql`](../../dbt/include/doris/macros/adapters/columns.sql)。

当前覆盖不完整：

- Incremental 中 `persist_docs` 被注释；
- View 的 Relation/Column 说明没有完整落到创建 SQL；
- Seed 和 Snapshot 虽会进入 dbt Core 的 Persist Docs 流程，但没有系统验证；
- Partial Column Docs、空说明、Quoted Column 和复杂字符缺少完整测试；
- 首次创建和后续修改的行为还未统一。

### 2.2 Catalog 和 Docs Generate

当前
[`metadata.sql`](../../dbt/include/doris/macros/adapters/metadata.sql)
从 `information_schema.tables` 和 `information_schema.columns` 返回：

- Database/Schema/Table；
- Relation Type；
- Table Comment；
- Column Name、Position、Type 和 Comment。

`dbt docs generate` 主路径可以运行，但目前：

- Schema 过滤只写在外层查询；
- 两个 CTE 可能先扫描较大范围的元数据；
- 只明确排除了 `information_schema`；
- 没有单 Relation Catalog；
- 没有声明相关 Adapter Capability；
- Catalog 内容断言和大规模性能基线不足。

### 2.3 Source Freshness

当前支持：

```yaml
loaded_at_field: loaded_at
```

dbt 计算：

```text
当前 Doris 时间 - max(loaded_at_field)
```

当前工作区的 `test/functional/adapter/test_doris_freshness.py`
已经覆盖真实 Doris 的通过路径。

当前不支持省略 `loaded_at_field` 后直接使用 Doris 表修改时间。原因是 Adapter
没有实现并声明 `TableLastModifiedMetadata` Capability。

## 3. Persist Docs 具体实现

### 3.1 建立资源行为矩阵

| 资源 | Relation Comment | Column Comment | 更新方式 |
| --- | --- | --- | --- |
| Table | 支持 | 支持 | 创建后统一 Alter |
| View | 取决于 Doris View DDL | 通过 View 字段列表或标记不支持 | Create/Replace 时写入 |
| Incremental | 支持 | 支持 | 每次成功构建后执行 |
| Seed | 支持 | 支持 | 建表和写入成功后执行 |
| Snapshot | 支持 | 支持 | 原子替换成功后执行 |

如果 Doris 某类对象不支持某种 Comment，应给出一次清晰 Warning，并在能力文档中
标记，不生成必然失败的 SQL。

### 3.2 统一注释渲染

增加共用的 Doris 字符串 Literal 转义宏，统一处理：

- 单引号；
- 反斜杠；
- 换行和制表符；
- 中文；
- 空字符串；
- 超过 Doris 长度限制的说明。

`doris__table_comment`、`doris__alter_relation_comment` 和
`doris__alter_column_comment` 必须复用同一实现，不能首次建表和后续 Alter
采用不同转义规则。

### 3.3 不让文档改变数据

未启用 Contract 时：

- YAML 中只说明部分字段是合法的；
- 未说明字段仍必须出现在 Model 结果中；
- Column Docs 不能被当成 SELECT 投影；
- 空 `columns:` 不能改变表结构。

这条规则需要继续由宏单元测试和真实 Doris 测试共同守住。

### 3.4 配置变化

至少定义：

| 变化 | 行为 |
| --- | --- |
| 新增说明 | 写入 Doris |
| 修改说明 | 更新 Doris |
| 删除说明 | 清空 Doris Comment，不能保留旧说明 |
| 删除 YAML 字段条目 | 不删除物理字段，只处理说明 |
| Model 改为 View | 按 View 支持边界重新处理 |

## 4. Catalog 具体实现

### 4.1 谓词下推

把 Schema 条件直接写进 Tables 和 Columns CTE：

```sql
from information_schema.tables
where table_schema in (...)
```

```sql
from information_schema.columns
where table_schema in (...)
```

并统一排除：

- `information_schema`；
- `__internal_schema`；
- `mysql`；
- 其他经确认不应进入用户 Docs 的系统 Database。

Schema 名必须用参数化或安全 Literal 渲染，不能直接拼接未转义输入。

### 4.2 单 Relation Catalog

实现 dbt 需要的单 Relation 元数据查询：

```text
database + schema + identifier
        |
information_schema 精确谓词
        |
返回一张表及其字段
```

实现并验证后再声明 `GetCatalogForSingleRelation`，不能先声明 Capability 再留空实现。

### 4.3 Catalog 正确性

验证：

- Table、View、Seed、Snapshot 和 Incremental；
- 无字段说明、部分字段说明和全部字段说明；
- Decimal、Datetime、Array、Map、Struct 等类型展示；
- Quoted Identifier 和大小写；
- 空 Schema；
- 跨 Database Source；
- Relation 被并发删除时的容错。

### 4.4 性能基线

建立包含大量 Database、Table 和 Column 的测试环境，记录：

- `dbt docs generate` 总耗时；
- `information_schema` 查询耗时；
- 扫描行数；
- 返回 Catalog 大小；
- 单 Relation 与全项目查询差异。

## 5. Freshness 具体实现

### 5.1 字段时间 Freshness

继续支持：

```yaml
sources:
  - name: ods
    tables:
      - name: orders
        loaded_at_field: update_time
        freshness:
          warn_after: {count: 1, period: hour}
          error_after: {count: 2, period: hour}
```

补齐：

- Pass、Warn、Error；
- 空表；
- `loaded_at_field` 全 NULL；
- Date、Datetime 和 Datetime 精度；
- `filter`；
- Session 时区；
- Future Timestamp；
- Doris 查询错误。

### 5.2 元数据 Freshness

Doris `information_schema.tables` 提供 `UPDATE_TIME`。第一版只对经过验证的
Internal Catalog OLAP 表启用：

```sql
select update_time
from information_schema.tables
where table_schema = ?
  and table_name = ?;
```

需要实现：

- 单表最后修改时间读取；
- dbt Freshness Result 所需的数据结构；
- Table 不存在、`UPDATE_TIME` 为 NULL 和 View 的行为；
- Batch 查询，避免每个 Source 单独请求；
- Internal 与 External Catalog 的能力区分。

只有真实 Doris 测试证明写入、Truncate 和其他需要支持的变化会正确推进
`UPDATE_TIME` 后，才声明：

- `TableLastModifiedMetadata`；
- `TableLastModifiedMetadataBatch`。

External Catalog 的 `UPDATE_TIME` 取决于 Connector 和元数据缓存，不能沿用
Internal Catalog 的承诺。

### 5.3 两种 Freshness 的优先级

```text
配置 loaded_at_query -> 使用自定义查询
否则有 loaded_at_field -> 使用字段最大值
否则 Adapter 支持元数据 Freshness -> 使用 UPDATE_TIME
否则 -> 明确报错
```

元数据 Freshness 不支持用户 `filter` 时应提示原因。

## 6. 代码改造

| 文件或模块 | 改造 |
| --- | --- |
| `macros/adapters/columns.sql` | 共用转义、Table/View Comment 行为 |
| 各 Materialization | 成功后统一调用 Persist Docs |
| `macros/adapters/metadata.sql` | 谓词下推、系统库过滤、单 Relation 查询 |
| Python Adapter | 元数据 Freshness 单表/批量接口和 Capability |
| Functional Tests | 各资源 Docs、Catalog 内容和 Freshness 状态 |

Capability 必须按“实现一个、测试一个、声明一个”的顺序增加。

## 7. 测试计划

### 7.1 Persist Docs

- Table/View/Incremental/Seed/Snapshot；
- Relation 和 Column；
- 新增、修改和删除说明；
- 部分字段说明；
- Quote、反斜杠、换行、中文和空说明；
- Full Refresh 和第二次 Incremental；
- Contract 开启和关闭。

### 7.2 Catalog

- 接入 dbt 官方 Basic Docs Generate 和 Catalog 测试；
- 内容与 `information_schema` 对照；
- 单 Relation 查询；
- 大规模元数据性能；
- Schema 谓词实际进入执行计划。

### 7.3 Freshness

- `loaded_at_field` 的 Pass/Warn/Error；
- Metadata Freshness 的 Pass/Warn/Error；
- 空表、NULL、Future Time 和时区；
- Batch 获取多张表；
- 不支持的 External Catalog 明确失败；
- `sources.json` 内容断言。

## 8. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| D1 | 统一 Comment 转义和删除语义 | 首次与更新行为一致 |
| D2 | 覆盖五类资源 Persist Docs | 每类都有真实 Doris 测试 |
| D3 | 下推 Catalog 谓词 | 查询计划和耗时基线证明有效 |
| D4 | 实现单 Relation Catalog | 测试通过后声明 Capability |
| D5 | 补字段时间 Freshness 边界 | Pass/Warn/Error 和异常路径完整 |
| D6 | 实现 Internal Catalog 元数据 Freshness | 单表和 Batch 测试通过 |
| D7 | 接入官方 Persist Docs/Catalog 套件 | 支持项全部通过 |

## 9. 完成定义

- 五类 dbt 资源的说明行为有公开矩阵；
- 文档配置不会改变 Model 字段或数据；
- Catalog 内容准确且过滤在扫描层生效；
- 字段时间和 Internal Catalog 元数据时间都可计算 Freshness；
- Capability 与真实实现、测试完全一致；
- External Catalog 等未支持边界有明确错误信息。
