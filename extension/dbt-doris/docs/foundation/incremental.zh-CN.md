# dbt-doris 基础功能实施方案：Incremental（历史设计）

> 状态：历史实施方案。本文记录改造前的问题和设计，不再用第 2 节判断当前能力。
> 当前实现见[《dbt-doris Incremental》](../incremental.zh-CN.md)。
>
> 当前代码到底支持哪些策略、实际生成什么 Doris SQL，以及从 Model 到结果的完整
> 示例，见[《dbt-doris Incremental 策略：代码实现与完整实战》](../dbt-doris-incremental-code-guide.zh-CN.md)。

## 1. 目标

Incremental 的目标不是简单地“把本批数据插入 Doris”，而是让用户选择的
`incremental_strategy` 具有稳定、可解释的行为：

| 策略 | 用户期望 |
| --- | --- |
| `append` | 本批结果全部追加，不更新历史行 |
| `unique_key_upsert` | 按业务 Key 更新已有行并插入新行 |
| `insert_overwrite` | 用本批结果覆盖整表或明确指定的 Doris 分区 |
| `delete_insert` | 先删除本批 Key 对应的旧数据，再插入本批结果 |

首次构建、第二次增量、字段变化和 `--full-refresh` 都必须保持相同的表配置，
错误配置必须在执行前失败，不能静默退化成另一种策略。

本文只补齐标准 Incremental。根据数据自动识别分区的 Dynamic Overwrite 和按时间
窗口拆批的 Microbatch 属于高级能力，另行建设。

## 2. 当前实现

主要实现位于
[`incremental.sql`](../../dbt/include/doris/macros/materializations/incremental/incremental.sql)。

当前已经具备：

- `append`：首次创建 Duplicate Key 表，后续通过临时表追加；
- 当前名为 `insert_overwrite` 的 Unique Key Upsert；
- `is_incremental()` 条件编译；
- `--full-refresh` 重建；
- 目标表由 View 或其他策略切换过来时的类型检查；
- 缺少 `unique_key` 或策略名错误时的编译期检查；
- 临时 Relation 清理和 Pre/Post Hook。

现有真实 Doris 测试覆盖：

- Append 首次构建和第二次追加；
- Unique Key 的一条更新、一条新增和历史行保留；
- Append 的 Full Refresh。

测试见
[`test_doris_incremental.py`](../../test/functional/adapter/test_doris_incremental.py)。

## 3. 当前最关键的问题

### 3.1 `insert_overwrite` 名称与行为不一致

当前实现依赖 Doris Unique Key 表的 Upsert：

```text
本批出现的 Key       -> 更新或新增
本批没有出现的旧 Key -> 继续保留
```

真正的 Insert Overwrite 应该用本批数据替换整表或指定分区。两者的数据正确性语义
不同，不能继续共用一个名称。

### 3.2 尚未接入标准策略接口

当前 Materialization 在一个大分支中直接完成建表、临时表和写入，没有形成
“策略名 -> 策略宏”的稳定接口。新增策略会继续放大分支复杂度，也难以接入
dbt 官方 Incremental 测试。

### 3.3 `on_schema_change` 未实现

当 Model 新增、删除或改变列类型时，目前没有完整支持：

- `ignore`；
- `fail`；
- `append_new_columns`；
- `sync_all_columns`。

结果可能是临时表与目标表列集合不一致，或者用户只能依赖 Full Refresh。

### 3.4 Full Refresh 边界不完整

当前 Full Refresh 能重建主路径，但还需要保证：

- Duplicate/Unique Key、Partition、Distribution、Bucket 和 Properties 不丢失；
- 中间表构建失败时原表仍然可查；
- 交换成功后再清理旧表；
- Adapter Response 返回真实影响行数，而不是占位查询的结果；
- 策略切换必须明确要求 Full Refresh。

### 3.5 Incremental 尚未执行 `persist_docs`

当前代码中 `persist_docs` 被注释。Incremental 表无法稳定持久化 Model 和 Column
说明，与 Table 的行为不一致。

## 4. 目标用户接口

### 4.1 Append

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        duplicate_key=["order_id"],
        distributed_by=["order_id"]
    )
}}

select *
from {{ source("ods", "orders") }}

{% if is_incremental() %}
where update_time > (select max(update_time) from {{ this }})
{% endif %}
```

Append 不要求 `unique_key`，也不承诺去重。重复运行同一批 SQL 会产生重复数据，
这是策略本身的语义。

### 4.2 Unique Key Upsert

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="unique_key_upsert",
        unique_key=["order_id"],
        distributed_by=["order_id"]
    )
}}
```

要求：

- 必须配置 `unique_key`；
- 首次构建必须创建 Doris Unique Key 表；
- 后续写入必须确认目标仍是 Unique Key 表；
- Model SQL 必须给出完整更新行；
- 本批未出现的 Key 保持不变。

### 4.3 Insert Overwrite

整表覆盖：

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite"
    )
}}
```

显式分区覆盖：

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        overwrite_partitions=["p202607", "p202608"]
    )
}}
```

对应 Doris 行为：

```sql
insert overwrite table target
select ... from staged_batch;

insert overwrite table target partition (p202607, p202608)
select ... from staged_batch;
```

`overwrite_partitions` 中的名称必须经过标识符校验，不能接受任意 SQL 片段。
指定分区时，本批数据落到配置范围以外应由 Doris 报错，而不是自动扩大覆盖范围。

### 4.4 Delete+Insert

```sql
{{
    config(
        materialized="incremental",
        incremental_strategy="delete_insert",
        unique_key=["order_id"]
    )
}}
```

执行顺序：

```text
Model SQL -> 临时表
              |
              +-> DELETE target USING temp，按 unique_key 匹配
              |
              +-> INSERT INTO target SELECT ... FROM temp
```

第一版只支持 Doris 能可靠执行 `DELETE ... USING` 的表模型和版本组合。
不满足条件时编译失败，不回退成 Append。

## 5. 策略迁移

当前 `insert_overwrite` 已经被部分用户当作 Unique Key Upsert 使用，不能在补功能时
无提示改变结果。

建议分两步迁移：

1. 先增加 `unique_key_upsert`，并把当前实现迁移到该名称；
2. 旧 `insert_overwrite + unique_key` 给出弃用提示和明确迁移说明；
3. 在声明的行为变更版本中，让 `insert_overwrite` 切换到 Doris 原生语义；
4. 通过兼容测试确认旧项目改名后结果不变。

发布说明必须明确：

```text
旧配置：
incremental_strategy: insert_overwrite
unique_key: [id]

迁移为：
incremental_strategy: unique_key_upsert
unique_key: [id]
```

## 6. 代码改造

### 6.1 拆分策略选择与执行

保留 Doris Incremental Materialization 负责统一生命周期：

```text
读取配置
  -> 校验策略
  -> 处理首次构建 / Full Refresh
  -> 创建本批临时表
  -> 处理 Schema Change
  -> Dispatch 到策略宏
  -> Persist Docs / Grants / Hook
  -> 清理临时对象
```

每个策略单独提供宏：

```text
doris__get_incremental_append_sql
doris__get_incremental_unique_key_upsert_sql
doris__get_incremental_insert_overwrite_sql
doris__get_incremental_delete_insert_sql
```

策略宏只生成当前策略的 DML，不自行管理完整 Materialization 生命周期。

### 6.2 建立统一配置校验

在执行 Model SQL 前完成：

- 策略是否受支持；
- `unique_key_upsert`、`delete_insert` 是否有 `unique_key`；
- `overwrite_partitions` 是否只用于 `insert_overwrite`；
- `unique_key`、分区和分布列是否存在；
- 目标表模型是否与策略相容；
- 策略切换是否必须 `--full-refresh`。

错误信息必须包含 Model 名、错误配置和可执行的恢复命令。

### 6.3 接入 Schema Change

复用 dbt 的 `incremental_validate_on_schema_change` 和
`process_schema_changes` 流程：

| 配置 | Doris 行为 |
| --- | --- |
| `ignore` | 继续使用目标列集合，只写入双方都存在的列 |
| `fail` | 检测到新增、删除或类型变化立即失败 |
| `append_new_columns` | 通过 `ALTER TABLE ADD COLUMN` 增加新列 |
| `sync_all_columns` | 增加、删除和修改字段，使目标与 Model 对齐 |

第一版对 Doris 不支持的类型变化直接失败。不能为了让命令成功而自动 Full Refresh。

### 6.4 保证 Full Refresh 安全

统一采用：

```text
按当前 Config 创建 intermediate
        |
完整执行 Model SQL
        |
REPLACE WITH TABLE 原子交换
        |
清理旧 Relation
```

禁止出现先删除目标表、再重命名新表的不可恢复窗口。

### 6.5 补齐公共生命周期

所有策略都要执行：

- Pre-hook；
- `persist_docs`；
- Grants；
- Post-hook；
- 临时对象清理；
- 正确的 Adapter Response；
- 失败后可重跑。

## 7. 测试计划

### 7.1 无集群单元测试

- 策略名到宏的 Dispatch；
- 每种无效配置的错误信息；
- `insert_overwrite` 生成整表和指定分区 SQL；
- 分区名和 Key 标识符转义；
- 每个 dbt Statement 只包含一条 SQL；
- Schema Change 四种配置的分支；
- Full Refresh 不使用占位 SQL 报告行数；
- Incremental 调用 Persist Docs 和 Grants。

### 7.2 真实 Doris Functional

| 场景 | 验收结果 |
| --- | --- |
| Append 连跑两次 | 第二批全部追加 |
| Upsert 连跑两次 | 同 Key 更新、新 Key 新增、其他 Key 保留 |
| 整表 Insert Overwrite | 第二次后只保留本批结果 |
| 指定分区 Insert Overwrite | 指定分区被替换，其他分区不变 |
| Delete+Insert | 匹配 Key 被替换，不匹配 Key 保留 |
| 四种 Schema Change | 结果与配置语义一致 |
| 策略切换 | 普通运行失败，Full Refresh 后成功 |
| 中间步骤失败 | 原目标可查，无残留临时表 |
| Persist Docs / Grants | 首次和第二次运行均保持 |

还应接入 `dbt-tests-adapter` 的 Incremental Unique Key、On Schema Change 和
Incremental Predicates 套件。Microbatch 测试等高级策略实现后再接入。

## 8. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| I1 | 增加 `unique_key_upsert` 并整理旧名称迁移 | 旧 Upsert 结果不变，错误配置不再静默 Append |
| I2 | 拆分策略宏 | Materialization 不再包含各策略的大段 DML |
| I3 | 实现原生整表和指定分区 Insert Overwrite | 真实 Doris 正反例通过 |
| I4 | 实现 `on_schema_change` | 四种模式有测试和文档 |
| I5 | 实现 Delete+Insert 和 Incremental Predicates | 扫描、删除范围可控 |
| I6 | 统一 Full Refresh、Docs、Grants 和结果报告 | 所有策略公共生命周期一致 |
| I7 | 接入官方 Incremental 套件 | 支持项全部通过，不支持项明确跳过原因 |

## 9. 完成定义

Incremental 只有同时满足以下条件才算补齐：

- 每个公开策略名称与实际数据行为一致；
- 首次构建、增量、字段变化和 Full Refresh 都有明确语义；
- 不支持的配置在写数据前失败；
- 失败不会丢失原表，也不会残留临时对象；
- 单元测试、真实 Doris 测试和官方 Adapter 测试共同覆盖；
- 用户文档包含配置示例、迁移方法和版本边界。
