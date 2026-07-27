# dbt-doris 基础功能实施方案：Snapshot

> 状态：实施方案。本文的“目标”和“建议”不代表当前已经支持，当前能力以第 2 节为准。

## 1. 目标

Snapshot 用 SCD Type 2 方式保存一条业务记录的历史版本。dbt-doris 需要保证：

- Check Strategy 和 Timestamp Strategy 都可用；
- 新增、修改和硬删除都产生正确的历史；
- 重复执行不会制造重复版本；
- 最终替换失败时，旧快照历史仍可查询；
- Snapshot 的表配置、文档和权限具有稳定生命周期。

Snapshot 保存的是历史事实，通常不能只靠当前源表重建。因此它的失败安全要求应高于
普通 Table Model。

## 2. 当前实现

dbt Core 负责识别记录变化和生成 Snapshot 临时结果，dbt-doris 主要提供：

- Doris 时间字符串转换；
- Snapshot Key 的 Hash 表达式；
- 将当前快照、更新/删除记录和新增记录合并到新表；
- 最终替换目标 Snapshot 表。

相关代码：

- [`snapshot.sql`](../../dbt/include/doris/macros/materializations/snapshot/snapshot.sql)
- [`strategies.sql`](../../dbt/include/doris/macros/materializations/snapshot/strategies.sql)

当前真实 Doris 测试
[`test_doris_snapshot.py`](../../test/functional/adapter/test_doris_snapshot.py)
已经覆盖 Check Strategy 的：

- 首次写入；
- 一条记录更新；
- 一条记录新增；
- 一条记录硬删除并失效；
- 当前版本和历史版本查询。

## 3. 当前缺口

### 3.1 最终替换不是原子的

当前最后执行：

```sql
drop table if exists target;
alter table target__snapshot_upsert rename target;
```

两条语句之间目标表不存在。如果连接中断、FE 切换或 Rename 失败，历史表会丢失。

### 3.2 失败恢复不可靠

Upsert 表使用 `create table if not exists`。如果上次运行中途失败并留下部分数据，
下次运行可能继续使用不干净的 Upsert 表。

每次运行必须先识别并清理本次专属中间对象，不能复用未知状态的残留表。

### 3.3 Timestamp Strategy 未验证

当前只验证了：

```yaml
strategy: check
check_cols: [...]
```

还需要验证：

```yaml
strategy: timestamp
updated_at: updated_at
```

包括时间精度、相同时间戳、NULL、迟到更新和时区行为。

### 3.4 Snapshot 配置生命周期不完整

需要补齐：

- `invalidate_hard_deletes`；
- 新版 Hard Deletes 配置；
- `dbt_valid_to_current`；
- `snapshot_meta_column_names`；
- Schema Change；
- Contract、Persist Docs 和 Grants；
- Full Refresh 或目标 Relation 类型冲突时的处理。

## 4. 目标用户接口

### 4.1 Check Strategy

```sql
{% snapshot snap_customers %}

{{
    config(
        target_schema="analytics",
        unique_key="customer_id",
        strategy="check",
        check_cols=["level", "address"],
        hard_deletes="invalidate"
    )
}}

select *
from {{ source("ods", "customers") }}

{% endsnapshot %}
```

适合源表没有可靠更新时间的场景。只比较 `check_cols` 中的业务字段。

### 4.2 Timestamp Strategy

```sql
{% snapshot snap_customers %}

{{
    config(
        target_schema="analytics",
        unique_key="customer_id",
        strategy="timestamp",
        updated_at="updated_at",
        hard_deletes="invalidate"
    )
}}

select *
from {{ source("ods", "customers") }}

{% endsnapshot %}
```

适合源表有可靠更新时间的场景。Timestamp Strategy 的正确性依赖：

- `updated_at` 在业务变化时单调更新；
- 时间字段类型和精度稳定；
- 多条相同 `unique_key` 输入已经由用户处理；
- 迟到数据的产品行为在文档中明确。

## 5. 目标执行流程

```text
读取 Snapshot 配置
        |
校验 unique_key / strategy / updated_at / check_cols
        |
清理本次运行可能残留的 staging 和 upsert Relation
        |
dbt Core 生成 staging 变化记录
        |
按目标表结构创建全新的 upsert Relation
        |
写入未变化的历史记录
        |
写入已关闭有效期的旧版本
        |
写入新版本
        |
校验 upsert Relation
        |
REPLACE WITH TABLE 原子替换目标
        |
Persist Docs / Grants / 清理临时 Relation
```

任何一步失败时：

- 原目标表保持可查询；
- 不把不完整的 Upsert 表改名为目标；
- 下次运行会清理残留对象后重新计算；
- dbt 返回 Error，而不是 Partial Success。

## 6. 具体代码改造

### 6.1 使用原子替换

复用 dbt-doris 已有的 `exchange_relation`，底层使用 Doris：

```sql
ALTER TABLE target
REPLACE WITH TABLE target__snapshot_upsert
PROPERTIES('swap' = 'false');
```

替换前不再删除目标表。只有 Upsert 表完整构建成功后才执行交换。

### 6.2 每次构建全新的 Upsert 表

运行开始时：

1. 查找 `target__snapshot_upsert`；
2. 如果存在，先删除；
3. 用 `CREATE TABLE ... LIKE target` 创建空表；
4. 分步骤写入全部历史；
5. 交换成功后确认临时表不存在。

不能继续使用 `CREATE TABLE IF NOT EXISTS` 接受上次失败的内容。

### 6.3 把合并过程拆成可验证步骤

将 `doris__snapshot_merge_sql` 拆分为职责明确的宏：

```text
doris__create_snapshot_upsert_relation
doris__insert_unchanged_snapshot_rows
doris__close_changed_snapshot_rows
doris__insert_new_snapshot_rows
doris__validate_snapshot_upsert_relation
doris__replace_snapshot_relation
```

每个 Statement 只执行一条 SQL，避免 MySQL Connector 多结果集污染连接。

### 6.4 增加构建后校验

交换前至少验证：

- `(unique_key, dbt_valid_to is null)` 不存在多个当前版本；
- `dbt_valid_from <= dbt_valid_to`；
- `dbt_scd_id` 非空；
- Upsert 表字段与目标 Snapshot 协议一致。

如果全表校验成本过高，应提供可关闭的内部校验开关，但测试环境默认开启。

### 6.5 明确 Schema Change

Snapshot 源新增字段时：

- 新增字段可通过 `ALTER TABLE ADD COLUMN` 接入；
- 删除或改变历史字段类型默认失败；
- 需要破坏性同步时，要求用户明确执行迁移；
- 不自动 Full Refresh，因为 Full Refresh 会丢失历史。

Snapshot 不应照搬普通 Incremental 的 `sync_all_columns` 删除历史列。

## 7. 测试计划

### 7.1 Check Strategy

- 首次运行；
- 新增记录；
- 修改一个和多个 `check_cols`；
- 未检查字段变化；
- 硬删除：Ignore、Invalidate 和 New Record；
- `check_cols: all`；
- NULL 与非 NULL 相互变化；
- 连续三次无变化运行不增加历史版本。

### 7.2 Timestamp Strategy

- 时间增加产生新版本；
- 时间不变不产生新版本；
- 相同 Key 的时间倒退；
- DATETIME 不同精度；
- NULL `updated_at`；
- Date/Datetime 类型边界；
- 时区输入和 Session 时区；
- 硬删除。

### 7.3 失败与恢复

- Upsert 表创建后失败；
- 写入未变化记录后失败；
- 原子交换前断开连接；
- FE 切换；
- 留下残余 Upsert 表后重新运行；
- 交换成功但清理阶段失败；
- 任何失败后原历史仍可查询。

### 7.4 标准兼容测试

接入：

- dbt 官方 Snapshot Check 测试；
- dbt 官方 Snapshot Timestamp 测试；
- `dbt-tests-adapter` 的 Simple Snapshot 套件；
- Persist Docs、Grants 和 Schema Change 的组合测试。

## 8. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| S1 | 用 `REPLACE WITH TABLE` 替换 Drop+Rename | 故障注入后旧历史仍可查询 |
| S2 | 清理残留并每次新建 Upsert 表 | 失败后重跑不重复、不污染 |
| S3 | 补 Timestamp Strategy | 正常、NULL、精度和迟到场景通过 |
| S4 | 补 Hard Deletes 配置矩阵 | 每种公开配置都有数据结果断言 |
| S5 | 补 Schema Change、Docs 和 Grants | 不破坏历史，生命周期一致 |
| S6 | 接入官方 Snapshot 测试 | 支持项全部通过 |

## 9. 完成定义

- Check 和 Timestamp 两种策略均有真实 Doris 验证；
- 新增、更新、硬删除和重复执行结果正确；
- 任何失败都不会先删除现有 Snapshot；
- 残留临时对象不会污染下一次运行；
- 历史 Schema 变化有明确策略；
- Snapshot 的文档、权限和 Artifact 与其他 dbt 资源一致。
