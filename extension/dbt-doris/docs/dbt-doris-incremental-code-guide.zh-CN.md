# dbt-doris Incremental 策略：代码实现与完整实战

> 本文同时面向 dbt 使用者和 dbt-doris 开发者。
> 当前状态以仓库中的 dbt-doris 1.0.0 实现为准；[`setup.py`](../setup.py) 声明的
> dbt Core 最低版本为 1.10.4。当前支持状态统一看第 1 节；第 4～9 节只说明
> 每种策略正确的配置、SQL 和结果。

## 1. 先看结论

dbt Core 把 Incremental 的通用概念定义为：第一次运行创建完整目标表，后续运行只
处理用户选出的新增或变化数据，再由 Adapter 决定怎样把本批数据写入目标表。

[dbt 官方 Incremental Strategy 文档](https://docs.getdbt.com/docs/build/incremental-strategy)
列出五种内置策略。Adapter 不会自动获得全部策略，每个 Adapter 必须分别接入：

| dbt 内置策略 | dbt 标准策略宏 | 用户期望 | 当前 dbt-doris |
| --- | --- | --- | --- |
| `append` | `get_incremental_append_sql` | 本批结果全部追加，不检查重复 | ✅ 已实现并有 Doris Functional Test |
| `delete+insert` | `get_incremental_delete_insert_sql` | 删除本批 Key 对应的旧行，再插入本批完整行 | ❌ 配置校验直接拒绝 |
| `merge` | `get_incremental_merge_sql` | 匹配 Key 时更新，不匹配时插入 | ❌ 配置校验直接拒绝 |
| `insert_overwrite` | `get_incremental_insert_overwrite_sql` | 用本批数据替换受影响的分区 | ❌ 官方语义未实现 |
| `microbatch` | `get_incremental_microbatch_sql` | 按 `event_time` 拆成多个可独立执行的时间批次 | ❌ 配置校验直接拒绝 |

此外，dbt-doris 当前还有一条“名字叫 `insert_overwrite`、实际是 Unique Key
Upsert”的路径：

| 当前配置 | 配置是否接受 | 实际写入方式 | 判断 |
| --- | :---: | --- | --- |
| `incremental_strategy='append'` | ✅ | Duplicate Key 目标表 + `INSERT INTO` | 与名称一致 |
| `incremental_strategy='insert_overwrite'` + `unique_key` | ✅ | Unique Key 目标表 + `INSERT INTO` | 实际是 Upsert，与名称不一致 |
| `incremental_strategy='insert_overwrite'`，无 `unique_key` | ❌ | 编译期失败，没有 Model DDL/DML | 当前明确禁止 |
| 不写 `incremental_strategy`，有 `unique_key` | ✅ | 默认进入上述 Unique Key Upsert | 默认策略也是 `insert_overwrite` |
| 不写 `incremental_strategy`，也无 `unique_key` | ❌ | 编译期失败，没有 Model DDL/DML | 容易踩坑 |

所以，如果问题是“dbt-doris 当前支持哪些 Incremental”：

1. 可以正常使用的标准策略只有 `append`；
2. 可以使用的另一种实际能力是 Unique Key Upsert，但配置名暂时仍是
   `insert_overwrite`；
3. 与 dbt 官方语义一致的 `delete+insert`、`merge`、`insert_overwrite` 和
   `microbatch` 都还没有实现。

## 2. 从代码看一次 Incremental 怎样执行

主要代码入口如下：

- [`incremental.sql`](../dbt/include/doris/macros/materializations/incremental/incremental.sql)：
  Incremental Materialization、策略校验以及首次构建、增量、Full Refresh 分支；
- [`help.sql`](../dbt/include/doris/macros/materializations/incremental/help.sql)：
  `is_incremental()`、`tmp_insert()`、`tmp_delete()` 和 Unique Key 表检查；
- [`create_table_as.sql`](../dbt/include/doris/macros/materializations/table/create_table_as.sql)：
  Duplicate Key 与 Unique Key 的 CTAS；
- [`relation.sql`](../dbt/include/doris/macros/adapters/relation.sql)：
  Key、分区、分布、Properties 和表交换 SQL；
- [`test_doris_incremental.py`](../test/functional/adapter/test_doris_incremental.py)：
  当前 Append、Upsert 和 Full Refresh 的 Doris Functional Test。

### 2.1 `help.sql`、`create_table_as.sql` 和 `relation.sql` 分别做什么

这三个文件都是 Incremental Materialization 使用的“零件”，真正决定调用顺序的
总入口仍然是 `incremental.sql`：

```text
incremental.sql：组织完整运行流程
    |
    +-- help.sql：判断运行状态、检查目标表、把临时表写入目标表
    |
    +-- create_table_as.sql：通过 Model 查询创建并填充新表
            |
            +-- relation.sql：生成 Doris Key、分区、分布和 Properties，
                              并执行删除、重命名、表交换等 Relation 操作
```

#### 2.1.1 `help.sql`：增量运行辅助工具

[`help.sql`](../dbt/include/doris/macros/materializations/incremental/help.sql)
主要处理“当前处于什么运行状态”和“目标表已存在后怎样写入”：

| 宏 | 作用 | 当前是否使用 |
| --- | --- | :---: |
| `is_incremental()` | 判断本轮是不是普通增量运行 | ✅ |
| `tmp_insert()` | 把临时表的列按目标表列顺序写入目标表 | ✅ |
| `show_create()` | 生成 `SHOW CREATE TABLE` | ✅ |
| `is_unique_model()` | 根据 `SHOW CREATE TABLE` 判断目标是不是 Unique Key 表 | ✅ |
| `tmp_delete()` | 尝试通过 `__DORIS_DELETE_SIGN__` 写入删除标记 | ❌ 没有调用点 |

例如第二次执行 Upsert 时，`tmp_insert()` 会生成：

```sql
INSERT INTO `dbt_incremental_lab`.`delivery_order_current`
    (`order_id`, `order_status`, `amount`)
(
    SELECT
        `order_id`, `order_status`, `amount`
    FROM `dbt_incremental_lab`.`delivery_order_current__dbt_tmp`
);
```

这条 SQL 本身只是普通 `INSERT INTO`：

- 目标是 Duplicate Key 表时，相同 Key 的记录继续追加；
- 目标是 Unique Key 表时，相同 Key 的新记录覆盖旧记录。

因此 Append 和当前 Upsert 的结果差异来自目标表模型，不是
`tmp_insert()` 生成了两种不同的 DML。

`tmp_delete()` 虽然已经写在文件里，但当前 Materialization 没有调用它，也没有
接入 `get_incremental_delete_insert_sql`。存在一个未使用的辅助宏，不等于
dbt-doris 已经支持 Delete+Insert。

#### 2.1.2 `create_table_as.sql`：创建并填充新表

[`create_table_as.sql`](../dbt/include/doris/macros/materializations/table/create_table_as.sql)
负责生成 CTAS。CTAS 是 `CREATE TABLE AS SELECT`，意思是用 Model 查询同时确定
表字段并写入查询结果。

`doris__create_table_as()` 用于创建 Duplicate Key 表：

```sql
CREATE TABLE `dbt_incremental_lab`.`game_event_fact`
DUPLICATE KEY (`event_id`)
DISTRIBUTED BY HASH (`event_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
SELECT
    event_id,
    player_id,
    event_time
FROM `dbt_incremental_lab`.`raw_game_events`;
```

它主要用于 `append` 的首次构建。

`doris__create_unique_table_as()` 用于创建 Unique Key 表：

```sql
CREATE TABLE `dbt_incremental_lab`.`delivery_order_current`
UNIQUE KEY (`order_id`)
DISTRIBUTED BY HASH (`order_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
SELECT
    order_id,
    order_status,
    amount
FROM `dbt_incremental_lab`.`raw_delivery_order_changes`;
```

它用于当前名为 `insert_overwrite`、实际为 Unique Key Upsert 的首次构建。

这个文件还会在以下场景使用：

- 第一次创建 Incremental 目标表；
- 第二次运行时创建只包含本批结果的临时表；
- `--full-refresh` 时创建用于替换目标的 Backup 表；
- 开启 Model Contract 时，通过 `doris__table_colume_type()` 校验列集合并对声明
  类型进行 `CAST`。

#### 2.1.3 `relation.sql`：把 Config 翻译成 Doris 表结构

[`relation.sql`](../dbt/include/doris/macros/adapters/relation.sql)
负责生成 Doris 特有的建表片段：

| 宏 | 生成内容 |
| --- | --- |
| `doris__duplicate_key()` | `DUPLICATE KEY(...)` |
| `doris__unique_key()` | `UNIQUE KEY(...)` |
| `doris__partition_by()` | `PARTITION BY RANGE/LIST ...` |
| `doris__distributed_by()` | `DISTRIBUTED BY HASH ... BUCKETS ...` |
| `doris__properties()` | `PROPERTIES(...)` |
| `doris__table_comment()` | Doris 表注释 |

例如用户在 Model 中写：

```jinja
{{
    config(
        unique_key=['order_date', 'order_id'],
        partition_by=['order_date'],
        partition_type='RANGE',
        partition_by_init=[
            "PARTITION p202607 VALUES LESS THAN ('2026-08-01')"
        ],
        distributed_by=['order_id'],
        buckets=3,
        properties={'replication_num': '1'}
    )
}}
```

这些宏会把配置翻译成：

```sql
UNIQUE KEY (`order_date`, `order_id`)
PARTITION BY RANGE (`order_date`) (
    PARTITION p202607 VALUES LESS THAN ('2026-08-01')
)
DISTRIBUTED BY HASH (`order_id`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
)
```

`relation.sql` 还负责数据库对象的生命周期操作：

- `doris__drop_relation()`：删除 Table 或 View；
- `doris__rename_relation()`：重命名 Relation；
- `exchange_relation()`：用 Doris 表交换替换目标表。

例如 Full Refresh 构建好 Backup 后，`exchange_relation()` 会生成类似：

```sql
ALTER TABLE `dbt_incremental_lab`.`delivery_order_current`
REPLACE WITH TABLE `delivery_order_current__dbt_backup`
PROPERTIES ('swap' = 'false');
```

#### 2.1.4 三个文件怎样配合

第一次运行当前 Upsert Model：

```text
incremental.sql
    |
    +-- 判断目标表不存在
    |
    v
create_table_as.sql
    |
    +-- 选择 doris__create_unique_table_as()
    |
    v
relation.sql
    |
    +-- 生成 UNIQUE KEY、PARTITION、DISTRIBUTED BY、PROPERTIES
    |
    v
Doris 执行 CREATE TABLE ... AS SELECT ...
```

第二次运行：

```text
incremental.sql
    |
    +-- help.sql / is_unique_model()
    |      检查目标是不是 Unique Key 表
    |
    +-- create_table_as.sql
    |      创建只包含本批 Model 结果的临时表
    |
    +-- help.sql / tmp_insert()
    |      INSERT INTO 目标表 SELECT ... FROM 临时表
    |
    +-- relation.sql / doris__drop_relation()
           清理临时表
```

一句话概括：

- `help.sql`：增量运行时怎样判断、检查和写入；
- `create_table_as.sql`：怎样用查询创建并填充一张新表；
- `relation.sql`：怎样把 dbt Config 翻译成 Doris 表结构和对象操作。

### 2.2 `config`、Model SQL 和 Doris SQL 各负责什么

下面这个 Model 包含两类信息：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

select *
from {{ source('game', 'events') }}

{% if is_incremental() %}
where ingest_seq > (select max(ingest_seq) from {{ this }})
{% endif %}
```

- `config` 告诉 dbt Core：这是 Incremental Model，并把策略名等配置交给
  dbt-doris；
- Model 的 `select` 决定“本轮要处理哪些数据”；
- `is_incremental()` 决定本轮是否加入增量过滤条件；
- dbt-doris Materialization 决定“把这批查询结果怎样写入 Doris”；
- Doris 最终执行 CTAS、`INSERT INTO`、`ALTER TABLE ... REPLACE WITH TABLE`
  等 SQL。

可以把它理解为：

```text
Model config
    |
    v
dbt_doris_validate_get_incremental_strategy
    |
    +-- append ----------------------> Duplicate Key / INSERT INTO
    |
    +-- 当前 insert_overwrite ------> Unique Key / INSERT INTO（实际 Upsert）
    |
    +-- 其他策略 --------------------> 编译错误

Model SELECT + is_incremental()
    |
    v
本轮查询结果
    |
    v
上述策略生成的 Doris DDL / DML
```

`is_incremental()` 在首次运行和 `--full-refresh` 时为 `False`，目标表已存在的
普通增量运行才为 `True`。它只控制 Model 是否加入过滤条件，不会自动判断哪些
数据是新的。

还要区分两类 SQL：`target/compiled/...sql` 主要是 Jinja 展开后的 Model 查询；
Materialization 运行时发送的 CTAS、`SHOW CREATE TABLE`、`INSERT INTO` 和清理
临时表等 SQL，需要通过 `dbt run --debug` 的日志查看。后面的完整案例会直接展示
这两部分。

## 3. 示例运行环境

后面每个示例都给出自己的源表、Model、命令和结果。为了不重复连接配置，统一使用
下面这个最小项目。

`dbt_project.yml`：

```yaml
name: doris_incremental_lab
version: 1.0.0
config-version: 2
profile: doris_incremental_lab
model-paths: ["models"]
```

`profiles/profiles.yml`：

```yaml
doris_incremental_lab:
  target: dev
  outputs:
    dev:
      type: doris
      host: 127.0.0.1
      port: 9030
      username: root
      password: ""
      schema: dbt_incremental_lab
      threads: 1
```

先确认连接：

```bash
dbt debug --profiles-dir profiles
```

示例使用单副本表，便于在单 BE 开发集群执行。生产环境应按实际集群设置副本数。

## 4. Append：游戏事件日志

### 4.1 场景

游戏服务不断产生战斗事件。`ingest_seq` 是严格递增的接入序号，Model 每次只读取
更大的序号。业务 `event_id` 偶尔可能被上游重复投递；Append 不负责去重，所以
重复事件会被保留。

### 4.2 准备第一批源数据

```sql
CREATE DATABASE IF NOT EXISTS dbt_incremental_lab;
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_game_events;
DROP TABLE IF EXISTS game_event_fact;

CREATE TABLE raw_game_events (
    event_id VARCHAR(32),
    ingest_seq BIGINT,
    player_id BIGINT,
    event_type VARCHAR(32),
    event_time DATETIME
)
DUPLICATE KEY(event_id)
DISTRIBUTED BY HASH(event_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_game_events VALUES
    ('evt-1001', 1, 7,  'login',        '2026-07-28 09:00:00'),
    ('evt-1002', 2, 7,  'defeat_slime', '2026-07-28 09:03:00'),
    ('evt-1003', 3, 18, 'open_chest',   '2026-07-28 09:05:00');
```

`models/game/sources.yml`：

```yaml
version: 2

sources:
  - name: game
    schema: dbt_incremental_lab
    tables:
      - name: events
        identifier: raw_game_events
```

### 4.3 编写 Model

`models/game/game_event_fact.sql`：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        duplicate_key=['event_id'],
        distributed_by=['event_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    event_id,
    ingest_seq,
    player_id,
    event_type,
    event_time
from {{ source('game', 'events') }}

{% if is_incremental() %}
where ingest_seq > (
    select coalesce(max(ingest_seq), 0)
    from {{ this }}
)
{% endif %}
```

### 4.4 第一次执行

```bash
dbt run --profiles-dir profiles --select game_event_fact
```

目标表不存在，所以 `is_incremental()` 为 `False`。运行时 Model 查询是：

```sql
select
    event_id,
    ingest_seq,
    player_id,
    event_type,
    event_time
from `dbt_incremental_lab`.`raw_game_events`
```

Adapter 发送的核心 Doris SQL 形态是：

```sql
CREATE TABLE `dbt_incremental_lab`.`game_event_fact`
DUPLICATE KEY (`event_id`)
COMMENT ''
DISTRIBUTED BY HASH (`event_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
select
    event_id,
    ingest_seq,
    player_id,
    event_type,
    event_time
from `dbt_incremental_lab`.`raw_game_events`;
```

验证：

```sql
SELECT event_id, ingest_seq, player_id, event_type
FROM dbt_incremental_lab.game_event_fact
ORDER BY ingest_seq;
```

结果：

| event_id | ingest_seq | player_id | event_type |
| --- | ---: | ---: | --- |
| evt-1001 | 1 | 7 | login |
| evt-1002 | 2 | 7 | defeat_slime |
| evt-1003 | 3 | 18 | open_chest |

### 4.5 第二批数据与第二次执行

```sql
INSERT INTO dbt_incremental_lab.raw_game_events VALUES
    ('evt-1004', 4, 18, 'craft_sword', '2026-07-28 09:10:00'),
    ('evt-1005', 5, 7,  'defeat_boss', '2026-07-28 09:20:00');
```

```bash
dbt run --profiles-dir profiles --select game_event_fact
```

这次 `is_incremental()` 为 `True`，运行时 Model 查询增加过滤：

```sql
select
    event_id,
    ingest_seq,
    player_id,
    event_type,
    event_time
from `dbt_incremental_lab`.`raw_game_events`
where ingest_seq > (
    select coalesce(max(ingest_seq), 0)
    from `dbt_incremental_lab`.`game_event_fact`
)
```

Materialization 执行的核心 SQL 形态是：

```sql
DROP TABLE IF EXISTS `game_event_fact__dbt_tmp`;

CREATE TABLE `dbt_incremental_lab`.`game_event_fact__dbt_tmp`
DUPLICATE KEY (`event_id`)
COMMENT ''
DISTRIBUTED BY HASH (`event_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
select
    event_id,
    ingest_seq,
    player_id,
    event_type,
    event_time
from `dbt_incremental_lab`.`raw_game_events`
where ingest_seq > (
    select coalesce(max(ingest_seq), 0)
    from `dbt_incremental_lab`.`game_event_fact`
);

INSERT INTO `dbt_incremental_lab`.`game_event_fact`
    (`event_id`, `ingest_seq`, `player_id`, `event_type`, `event_time`)
(
    SELECT
        `event_id`, `ingest_seq`, `player_id`, `event_type`, `event_time`
    FROM `dbt_incremental_lab`.`game_event_fact__dbt_tmp`
);

DROP TABLE IF EXISTS `game_event_fact__dbt_tmp`;
```

临时 Relation 的具体标识符由 dbt 生成，日志中的名字可能带额外后缀，但执行顺序和
核心 SQL 与上面一致。

再次查询得到 5 行：

| event_id | ingest_seq | player_id | event_type |
| --- | ---: | ---: | --- |
| evt-1001 | 1 | 7 | login |
| evt-1002 | 2 | 7 | defeat_slime |
| evt-1003 | 3 | 18 | open_chest |
| evt-1004 | 4 | 18 | craft_sword |
| evt-1005 | 5 | 7 | defeat_boss |

### 4.6 演示 Append 不去重

假设上游把 `evt-1005` 以新的接入序号重放：

```sql
INSERT INTO dbt_incremental_lab.raw_game_events VALUES
    ('evt-1005', 6, 7, 'defeat_boss', '2026-07-28 09:20:00');
```

再次执行：

```bash
dbt run --profiles-dir profiles --select game_event_fact
```

查询重复业务事件：

```sql
SELECT event_id, count(*) AS copies
FROM dbt_incremental_lab.game_event_fact
GROUP BY event_id
HAVING count(*) > 1;
```

结果：

| event_id | copies |
| --- | ---: |
| evt-1005 | 2 |

这不是 dbt-doris 的异常，而是 `append` 的标准语义。若业务要求同 Key 更新而不是
保留重复行，需要 Upsert、Merge 或 Delete+Insert。

## 5. Unique Key Upsert：外卖订单状态

### 5.1 场景

外卖订单会从“已接单”变成“配送中”或“已送达”。本批可能既有旧订单的完整新状态，
也有第一次出现的新订单。目标表只需要保留每个 `order_id` 的最新完整状态。

这个策略按 `order_id` 判断记录是否已经存在：存在时更新完整订单状态，不存在时
插入新订单，本批没有出现的订单继续保留。

### 5.2 准备第一批源数据

```sql
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_delivery_order_changes;
DROP TABLE IF EXISTS delivery_order_current;

CREATE TABLE raw_delivery_order_changes (
    order_id BIGINT,
    change_seq BIGINT,
    customer_name VARCHAR(64),
    order_status VARCHAR(32),
    amount DECIMAL(10, 2),
    updated_at DATETIME
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_delivery_order_changes VALUES
    (101, 1, 'Alice', 'accepted', 36.00, '2026-07-28 11:00:00'),
    (102, 2, 'Bob',   'cooking',  58.00, '2026-07-28 11:02:00'),
    (103, 3, 'Carol', 'accepted', 25.00, '2026-07-28 11:04:00');
```

`models/delivery/sources.yml`：

```yaml
version: 2

sources:
  - name: delivery
    schema: dbt_incremental_lab
    tables:
      - name: order_changes
        identifier: raw_delivery_order_changes
```

### 5.3 编写 Model

`models/delivery/delivery_order_current.sql`：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='unique_key_upsert',
        unique_key=['order_id'],
        distributed_by=['order_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    order_id,
    change_seq,
    customer_name,
    order_status,
    amount,
    updated_at
from {{ source('delivery', 'order_changes') }}

{% if is_incremental() %}
where change_seq > (
    select coalesce(max(change_seq), 0)
    from {{ this }}
)
{% endif %}
```

这里的 Model 必须返回完整的新行。

### 5.4 第一次执行

```bash
dbt run --profiles-dir profiles --select delivery_order_current
```

首次运行的 Model 查询读取三条源数据，并生成 Unique Key CTAS：

```sql
CREATE TABLE `dbt_incremental_lab`.`delivery_order_current`
UNIQUE KEY (`order_id`)
COMMENT ''
DISTRIBUTED BY HASH (`order_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
select
    order_id,
    change_seq,
    customer_name,
    order_status,
    amount,
    updated_at
from `dbt_incremental_lab`.`raw_delivery_order_changes`;
```

结果：

| order_id | change_seq | customer_name | order_status | amount |
| ---: | ---: | --- | --- | ---: |
| 101 | 1 | Alice | accepted | 36.00 |
| 102 | 2 | Bob | cooking | 58.00 |
| 103 | 3 | Carol | accepted | 25.00 |

### 5.5 第二批数据与第二次执行

第二批包含一条更新和一条新增：

```sql
INSERT INTO dbt_incremental_lab.raw_delivery_order_changes VALUES
    (101, 4, 'Alice', 'delivered', 36.00, '2026-07-28 11:35:00'),
    (104, 5, 'Dave',  'accepted',  42.00, '2026-07-28 11:36:00');
```

```bash
dbt run --profiles-dir profiles --select delivery_order_current
```

运行时 Model 查询：

```sql
select
    order_id,
    change_seq,
    customer_name,
    order_status,
    amount,
    updated_at
from `dbt_incremental_lab`.`raw_delivery_order_changes`
where change_seq > (
    select coalesce(max(change_seq), 0)
    from `dbt_incremental_lab`.`delivery_order_current`
)
```

策略先确认目标是 Unique Key 表：

```sql
SHOW CREATE TABLE `dbt_incremental_lab`.`delivery_order_current`;
```

然后创建临时表并执行：

```sql
INSERT INTO `dbt_incremental_lab`.`delivery_order_current`
    (`order_id`, `change_seq`, `customer_name`, `order_status`, `amount`, `updated_at`)
(
    SELECT
        `order_id`, `change_seq`, `customer_name`, `order_status`, `amount`, `updated_at`
    FROM `dbt_incremental_lab`.`delivery_order_current__dbt_tmp`
);
```

这条 SQL 使用 `INSERT INTO` 写入 Unique Key 表，更新行为来自目标表的
`UNIQUE KEY(order_id)`。

验证：

```sql
SELECT order_id, change_seq, customer_name, order_status, amount
FROM dbt_incremental_lab.delivery_order_current
ORDER BY order_id;
```

结果：

| order_id | change_seq | customer_name | order_status | amount |
| ---: | ---: | --- | --- | ---: |
| 101 | 4 | Alice | delivered | 36.00 |
| 102 | 2 | Bob | cooking | 58.00 |
| 103 | 3 | Carol | accepted | 25.00 |
| 104 | 5 | Dave | accepted | 42.00 |

这个结果证明：

- 本批出现的 `order_id=101` 被更新；
- 本批新出现的 `order_id=104` 被插入；
- 本批没有出现的 `order_id=102` 和 `103` 继续保留；
- 总行数从 3 变成 4，而不是只剩本批的 2 行。

### 5.6 实现要求

- 必须配置 `unique_key`；
- 首次运行必须创建 Doris Unique Key 表；
- 第二次运行前必须确认目标仍然是 Unique Key 表；
- Model 必须为本批 Key 返回完整的新行；
- 本批出现的 Key 更新或新增，本批没有出现的 Key 保持不变；
- 目标表原来是其他 Key 模型时，必须通过 Full Refresh 重建。

## 6. 官方 Insert Overwrite：正确实现

### 6.1 场景

广告归因表按日期分区。7 月 27 日第一次计算出 Campaign A 和 B；后来归因规则
修正，整天重新计算后的完整结果只有 A 和 D。

Insert Overwrite 应该用 A、D 替换 7 月 27 日整个分区，因此旧的 B 消失；
7 月 26 日属于其他分区，继续保留。

### 6.2 准备第一批数据

```sql
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_ad_attribution_batches;
DROP TABLE IF EXISTS daily_ad_attribution;

CREATE TABLE raw_ad_attribution_batches (
    stat_date DATE,
    campaign_id BIGINT,
    batch_id BIGINT,
    attributed_orders BIGINT,
    attributed_revenue DECIMAL(12, 2)
)
DUPLICATE KEY(stat_date, campaign_id)
DISTRIBUTED BY HASH(campaign_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_ad_attribution_batches VALUES
    ('2026-07-26', 900, 1,  5,  250.00),
    ('2026-07-27', 101, 1, 10, 1000.00),
    ('2026-07-27', 102, 1, 20, 2400.00);
```

`models/ads/sources.yml`：

```yaml
version: 2

sources:
  - name: ads
    schema: dbt_incremental_lab
    tables:
      - name: attribution_batches
        identifier: raw_ad_attribution_batches
```

### 6.3 编写 Model

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        duplicate_key=['stat_date', 'campaign_id'],
        partition_by=['stat_date'],
        partition_type='RANGE',
        partition_by_init=[
            "PARTITION p20260726 VALUES LESS THAN ('2026-07-27')",
            "PARTITION p20260727 VALUES LESS THAN ('2026-07-28')",
            "PARTITION pmax VALUES LESS THAN (MAXVALUE)"
        ],
        overwrite_partitions=['p20260727'],
        distributed_by=['campaign_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    stat_date,
    campaign_id,
    batch_id,
    attributed_orders,
    attributed_revenue
from {{ source('ads', 'attribution_batches') }}

{% if is_incremental() %}
where batch_id > (
    select coalesce(max(batch_id), 0)
    from {{ this }}
)
{% endif %}
```

这里的关键配置是：

| 配置 | 作用 |
| --- | --- |
| `partition_by=['stat_date']` | 目标表按日期进行 Range 分区 |
| `partition_by_init` | 创建目标表时定义初始 Doris 分区 |
| `overwrite_partitions=['p20260727']` | 本次增量运行完整替换 7 月 27 日分区 |

Insert Overwrite 的匹配单位是分区，不是业务 Key，因此不需要 `unique_key`。

### 6.4 第一次执行

```bash
dbt run --profiles-dir profiles --select daily_ad_attribution
```

第一次运行读取全部源数据，并创建分区目标表：

```sql
CREATE TABLE `dbt_incremental_lab`.`daily_ad_attribution`
DUPLICATE KEY (`stat_date`, `campaign_id`)
COMMENT ''
PARTITION BY RANGE (`stat_date`) (
    PARTITION p20260726 VALUES LESS THAN ('2026-07-27'),
    PARTITION p20260727 VALUES LESS THAN ('2026-07-28'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH (`campaign_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
SELECT
    stat_date,
    campaign_id,
    batch_id,
    attributed_orders,
    attributed_revenue
FROM `dbt_incremental_lab`.`raw_ad_attribution_batches`;
```

第一次结果：

| stat_date | campaign_id | batch_id | attributed_orders |
| --- | ---: | ---: | ---: |
| 2026-07-26 | 900 | 1 | 5 |
| 2026-07-27 | 101 | 1 | 10 |
| 2026-07-27 | 102 | 1 | 20 |

### 6.5 第二次执行

写入 7 月 27 日的完整重算结果：

```sql
INSERT INTO dbt_incremental_lab.raw_ad_attribution_batches VALUES
    ('2026-07-27', 101, 2, 12, 1200.00),
    ('2026-07-27', 104, 2,  8,  880.00);
```

执行：

```bash
dbt run --profiles-dir profiles --select daily_ad_attribution
```

`is_incremental()` 过滤后，本批临时表只包含 A 和 D。策略随后执行 Doris 原生
分区覆盖：

```sql
INSERT OVERWRITE TABLE `dbt_incremental_lab`.`daily_ad_attribution`
PARTITION (p20260727)
    (`stat_date`, `campaign_id`, `batch_id`,
     `attributed_orders`, `attributed_revenue`)
SELECT
    `stat_date`, `campaign_id`, `batch_id`,
    `attributed_orders`, `attributed_revenue`
FROM `dbt_incremental_lab`.`daily_ad_attribution__dbt_tmp`;
```

最终结果：

| stat_date | campaign_id | batch_id | attributed_orders |
| --- | ---: | ---: | ---: |
| 2026-07-26 | 900 | 1 | 5 |
| 2026-07-27 | 101 | 2 | 12 |
| 2026-07-27 | 104 | 2 | 8 |

7 月 27 日分区原来的 `campaign_id=102` 被移除，7 月 26 日分区保持不变。

### 6.6 实现要求

- Model 必须返回被覆盖分区的完整新数据，不能只返回发生变化的行；
- `overwrite_partitions` 只能接受经过标识符校验的目标分区名；
- 本批数据必须全部属于指定分区，越界数据应执行失败；
- 第一次运行创建完整目标表，普通增量运行才执行分区覆盖；
- 整表覆盖使用 `INSERT OVERWRITE TABLE target SELECT ...`；
- 自动识别本批分区可以使用 Doris
  `INSERT OVERWRITE TABLE target PARTITION (*) SELECT ...`；
- 覆盖失败时必须保留原分区，并清理本轮临时 Relation。

## 7. Delete+Insert：订单明细修正

### 7.1 场景

餐厅结算系统收到一批订单明细修正。`order_id` 相同的旧记录需要整行删除，再插入
本批完整新记录；本批没有出现的订单保持不变。

与 Merge 相比，Delete+Insert 不逐列生成 Update 表达式，而是先删后插，适合
“整行替换”的场景。

### 7.2 准备数据

```sql
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_order_corrections;
DROP TABLE IF EXISTS restaurant_order_detail;

CREATE TABLE raw_order_corrections (
    order_id BIGINT,
    correction_seq BIGINT,
    restaurant_id BIGINT,
    dish_count INT,
    payable_amount DECIMAL(10, 2),
    corrected_at DATETIME
)
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_order_corrections VALUES
    (201, 1, 81, 2, 45.00, '2026-07-28 12:00:00'),
    (202, 2, 81, 1, 28.00, '2026-07-28 12:01:00'),
    (203, 3, 93, 4, 96.00, '2026-07-28 12:02:00');
```

`models/restaurant/sources.yml`：

```yaml
version: 2

sources:
  - name: restaurant
    schema: dbt_incremental_lab
    tables:
      - name: order_corrections
        identifier: raw_order_corrections
```

`models/restaurant/restaurant_order_detail.sql`：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['order_id'],
        distributed_by=['order_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    order_id,
    correction_seq,
    restaurant_id,
    dish_count,
    payable_amount,
    corrected_at
from {{ source('restaurant', 'order_corrections') }}

{% if is_incremental() %}
where correction_seq > (
    select coalesce(max(correction_seq), 0)
    from {{ this }}
)
{% endif %}
```

### 7.3 第一次执行

```bash
dbt run --profiles-dir profiles --select restaurant_order_detail
```

第一次运行读取全部源数据并创建目标表：

```sql
CREATE TABLE `dbt_incremental_lab`.`restaurant_order_detail`
UNIQUE KEY (`order_id`)
COMMENT ''
DISTRIBUTED BY HASH (`order_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
SELECT
    order_id,
    correction_seq,
    restaurant_id,
    dish_count,
    payable_amount,
    corrected_at
FROM `dbt_incremental_lab`.`raw_order_corrections`;
```

第一次结果：

| order_id | correction_seq | restaurant_id | dish_count | payable_amount |
| ---: | ---: | ---: | ---: | ---: |
| 201 | 1 | 81 | 2 | 45.00 |
| 202 | 2 | 81 | 1 | 28.00 |
| 203 | 3 | 93 | 4 | 96.00 |

### 7.4 第二次执行

第二批把 201 的菜品数量和金额改正，同时新增 204：

```sql
INSERT INTO dbt_incremental_lab.raw_order_corrections VALUES
    (201, 4, 81, 3, 63.00, '2026-07-28 12:30:00'),
    (204, 5, 93, 2, 52.00, '2026-07-28 12:31:00');
```

```bash
dbt run --profiles-dir profiles --select restaurant_order_detail
```

策略先把本批 Model 结果放入临时表，再按 `unique_key` 删除旧行并插入完整新行：

```sql
DELETE FROM `dbt_incremental_lab`.`restaurant_order_detail` DBT_INTERNAL_DEST
USING `dbt_incremental_lab`.`restaurant_order_detail__dbt_tmp` DBT_INTERNAL_SOURCE
WHERE DBT_INTERNAL_DEST.`order_id` = DBT_INTERNAL_SOURCE.`order_id`;

INSERT INTO `dbt_incremental_lab`.`restaurant_order_detail`
    (`order_id`, `correction_seq`, `restaurant_id`,
     `dish_count`, `payable_amount`, `corrected_at`)
SELECT
    `order_id`, `correction_seq`, `restaurant_id`,
    `dish_count`, `payable_amount`, `corrected_at`
FROM `dbt_incremental_lab`.`restaurant_order_detail__dbt_tmp`;
```

正确结果：

| order_id | correction_seq | restaurant_id | dish_count | payable_amount |
| ---: | ---: | ---: | ---: | ---: |
| 201 | 4 | 81 | 3 | 63.00 |
| 202 | 2 | 81 | 1 | 28.00 |
| 203 | 3 | 93 | 4 | 96.00 |
| 204 | 5 | 93 | 2 | 52.00 |

201 的旧行被完整替换，204 被新增，202 和 203 没有出现在本批中，因此保留。

### 7.5 实现要求

- 必须配置一个或多个 `unique_key`；
- Delete 和 Insert 必须使用同一份临时表结果；
- 组合 Key 的每个字段都必须加入 Delete 匹配条件；
- Delete 成功而 Insert 失败时不能留下永久缺失数据；
- 本批没有出现的 Key 不参与删除，继续保留；
- 临时表必须在成功或失败后正确清理。

## 8. Merge：会员等级更新

### 8.1 场景

会员系统每天发送等级变化：已经存在的会员更新积分和等级，新会员直接插入。业务
希望用一条 Merge 明确表达“匹配则更新、不匹配则插入”。

### 8.2 准备数据和 Model

```sql
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_member_changes;
DROP TABLE IF EXISTS member_level_current;

CREATE TABLE raw_member_changes (
    member_id BIGINT,
    change_seq BIGINT,
    member_level VARCHAR(16),
    points BIGINT,
    updated_at DATETIME
)
DUPLICATE KEY(member_id)
DISTRIBUTED BY HASH(member_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_member_changes VALUES
    (301, 1, 'silver', 1200, '2026-07-28 13:00:00'),
    (302, 2, 'gold',   5600, '2026-07-28 13:01:00'),
    (303, 3, 'bronze',  300, '2026-07-28 13:02:00');
```

`models/member/sources.yml`：

```yaml
version: 2

sources:
  - name: member
    schema: dbt_incremental_lab
    tables:
      - name: changes
        identifier: raw_member_changes
```

`models/member/member_level_current.sql`：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['member_id'],
        distributed_by=['member_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    member_id,
    change_seq,
    member_level,
    points,
    updated_at
from {{ source('member', 'changes') }}

{% if is_incremental() %}
where change_seq > (
    select coalesce(max(change_seq), 0)
    from {{ this }}
)
{% endif %}
```

### 8.3 第一次执行

```bash
dbt run --profiles-dir profiles --select member_level_current
```

第一次运行读取全部源数据并创建 Unique Key 目标表：

```sql
CREATE TABLE `dbt_incremental_lab`.`member_level_current`
UNIQUE KEY (`member_id`)
COMMENT ''
DISTRIBUTED BY HASH (`member_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
SELECT
    member_id,
    change_seq,
    member_level,
    points,
    updated_at
FROM `dbt_incremental_lab`.`raw_member_changes`;
```

第一次结果：

| member_id | change_seq | member_level | points |
| ---: | ---: | --- | ---: |
| 301 | 1 | silver | 1200 |
| 302 | 2 | gold | 5600 |
| 303 | 3 | bronze | 300 |

### 8.4 第二次执行

第二批把会员 301 升级为 Gold，同时新增会员 304：

```sql
INSERT INTO dbt_incremental_lab.raw_member_changes VALUES
    (301, 4, 'gold',  5200, '2026-07-28 14:00:00'),
    (304, 5, 'bronze', 100, '2026-07-28 14:01:00');
```

执行：

```bash
dbt run --profiles-dir profiles --select member_level_current
```

策略把本批结果写入临时表，再生成：

```sql
MERGE INTO `dbt_incremental_lab`.`member_level_current` AS DBT_INTERNAL_DEST
USING `dbt_incremental_lab`.`member_level_current__dbt_tmp` AS DBT_INTERNAL_SOURCE
ON DBT_INTERNAL_DEST.`member_id` <=> DBT_INTERNAL_SOURCE.`member_id`
WHEN MATCHED THEN UPDATE SET
    `change_seq` = DBT_INTERNAL_SOURCE.`change_seq`,
    `member_level` = DBT_INTERNAL_SOURCE.`member_level`,
    `points` = DBT_INTERNAL_SOURCE.`points`,
    `updated_at` = DBT_INTERNAL_SOURCE.`updated_at`
WHEN NOT MATCHED THEN INSERT
    (`member_id`, `change_seq`, `member_level`, `points`, `updated_at`)
VALUES
    (DBT_INTERNAL_SOURCE.`member_id`,
     DBT_INTERNAL_SOURCE.`change_seq`,
     DBT_INTERNAL_SOURCE.`member_level`,
     DBT_INTERNAL_SOURCE.`points`,
     DBT_INTERNAL_SOURCE.`updated_at`);
```

最终结果：

| member_id | change_seq | member_level | points |
| ---: | ---: | --- | ---: |
| 301 | 4 | gold | 5200 |
| 302 | 2 | gold | 5600 |
| 303 | 3 | bronze | 300 |
| 304 | 5 | bronze | 100 |

301 匹配已有 Key，因此被更新；304 没有匹配，因此被插入；302 和 303 没有出现
在本批中，因此保持不变。

### 8.5 实现要求

- 必须正确处理单列和组合 `unique_key`；
- Key 比较需要明确 NULL 安全语义；
- `merge_update_columns` 只更新用户指定的列；
- `merge_exclude_columns` 保留用户排除列的旧值；
- Incremental Predicates 只能限制目标扫描，不能改变匹配正确性；
- 同一个 Key 在本批出现多次时必须有确定的冲突处理规则。

## 9. Microbatch：按小时处理 IoT 数据

### 9.1 场景

IoT 平台一天产生大量温度读数。用户希望按小时处理，每个小时是一个独立批次：

- 10:00 到 11:00 是一个查询；
- 11:00 到 12:00 是另一个查询；
- 某个小时失败时可以只重试该批次；
- 晚到数据可以通过 `lookback` 重算前一个小时。

Microbatch 从 dbt Core 1.9 起可用。它不要求用户自己写
`is_incremental()` 过滤，而是根据 `event_time`、`begin` 和 `batch_size`
为每个时间窗口编译独立查询。

### 9.2 准备数据和 Model

```sql
USE dbt_incremental_lab;

DROP TABLE IF EXISTS raw_iot_temperature;
DROP TABLE IF EXISTS hourly_device_temperature;

CREATE TABLE raw_iot_temperature (
    reading_id BIGINT,
    device_id BIGINT,
    event_time DATETIME,
    temperature DECIMAL(5, 2)
)
DUPLICATE KEY(reading_id)
DISTRIBUTED BY HASH(device_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO raw_iot_temperature VALUES
    (401, 51, '2026-07-28 10:05:00', 21.50),
    (402, 51, '2026-07-28 10:35:00', 22.10),
    (403, 52, '2026-07-28 11:10:00', 29.30);
```

`models/iot/sources.yml`：

```yaml
version: 2

sources:
  - name: iot
    schema: dbt_incremental_lab
    tables:
      - name: temperature
        identifier: raw_iot_temperature
        config:
          event_time: event_time
```

`models/iot/hourly_device_temperature.sql`：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='microbatch',
        event_time='hour_start',
        begin='2026-07-28 10:00:00',
        batch_size='hour',
        lookback=1,
        duplicate_key=['hour_start', 'device_id'],
        partition_by=['hour_start'],
        partition_type='RANGE',
        partition_by_init=[
            "PARTITION p2026072810 VALUES LESS THAN ('2026-07-28 11:00:00')",
            "PARTITION p2026072811 VALUES LESS THAN ('2026-07-28 12:00:00')",
            "PARTITION pmax VALUES LESS THAN (MAXVALUE)"
        ],
        distributed_by=['device_id'],
        buckets=1,
        properties={'replication_num': '1'}
    )
}}

select
    date_trunc(event_time, 'hour') as hour_start,
    device_id,
    count(*) as reading_count,
    avg(temperature) as avg_temperature
from {{ source('iot', 'temperature') }}
group by
    date_trunc(event_time, 'hour'),
    device_id
```

### 9.3 按时间窗口执行

```bash
dbt run \
  --profiles-dir profiles \
  --select hourly_device_temperature \
  --event-time-start "2026-07-28 10:00:00" \
  --event-time-end "2026-07-28 12:00:00"
```

dbt 根据 `event_time` 和 `batch_size='hour'` 把这次运行拆成两个独立批次：

```text
批次 1：[2026-07-28 10:00:00, 2026-07-28 11:00:00)
批次 2：[2026-07-28 11:00:00, 2026-07-28 12:00:00)
```

### 9.4 执行每个批次

10:00 批次的 Model 查询：

```sql
select
    date_trunc(event_time, 'hour') as hour_start,
    device_id,
    count(*) as reading_count,
    avg(temperature) as avg_temperature
from (
    select *
    from `dbt_incremental_lab`.`raw_iot_temperature`
    where event_time >= '2026-07-28 10:00:00'
      and event_time <  '2026-07-28 11:00:00'
) AS DBT_MICROBATCH_INPUT
group by
    date_trunc(event_time, 'hour'),
    device_id;
```

11:00 批次使用：

```sql
where event_time >= '2026-07-28 11:00:00'
  and event_time <  '2026-07-28 12:00:00'
```

每个批次都是该小时的完整结果，因此分别覆盖对应 Doris 分区：

```sql
INSERT OVERWRITE TABLE `dbt_incremental_lab`.`hourly_device_temperature`
PARTITION (p2026072810)
SELECT * FROM `hourly_device_temperature__dbt_tmp_2026072810`;

INSERT OVERWRITE TABLE `dbt_incremental_lab`.`hourly_device_temperature`
PARTITION (p2026072811)
SELECT * FROM `hourly_device_temperature__dbt_tmp_2026072811`;
```

执行结果：

| hour_start | device_id | reading_count | avg_temperature |
| --- | ---: | ---: | ---: |
| 2026-07-28 10:00:00 | 51 | 2 | 21.80 |
| 2026-07-28 11:00:00 | 52 | 1 | 29.30 |

### 9.5 重新处理晚到数据

如果后来收到一条 10:20 的晚到数据：

```sql
INSERT INTO dbt_incremental_lab.raw_iot_temperature VALUES
    (404, 51, '2026-07-28 10:20:00', 22.70);
```

重新执行 10:00 批次：

```bash
dbt run \
  --profiles-dir profiles \
  --select hourly_device_temperature \
  --event-time-start "2026-07-28 10:00:00" \
  --event-time-end "2026-07-28 11:00:00"
```

同一个小时被完整重算并覆盖，不会追加一条重复聚合结果：

| hour_start | device_id | reading_count | avg_temperature |
| --- | ---: | ---: | ---: |
| 2026-07-28 10:00:00 | 51 | 3 | 22.10 |

### 9.6 实现要求

- `event_time`、`begin` 和 `batch_size` 必须完整配置；
- dbt 必须自动向设置了 `event_time` 的上游 Relation 下推时间范围；
- 每个批次必须独立、幂等，可以单独重试；
- `lookback` 用于定期重算最近的若干批次，接收晚到数据；
- 同一个批次重复执行必须得到相同目标结果；
- 并行批次不能同时覆盖同一个 Doris 分区；
- 批次失败时只重试失败批次，不重复执行已经成功的批次。

## 10. 相关但不能混为一谈的能力

### 10.1 自定义 Incremental Strategy

dbt Core 支持用户定义：

```jinja
{% macro get_incremental_my_strategy_sql(arg_dict) %}
    ...
{% endmacro %}
```

并配置：

```python
incremental_strategy='my_strategy'
```

它是扩展机制，不是第六种内置策略。当前 dbt-doris 的策略白名单会先拒绝
`my_strategy`，所以这条通用扩展机制也尚未接入。

### 10.2 Doris 自定义 Partition Materialization

dbt-doris 仓库另有
[`partition`](../dbt/include/doris/macros/materializations/partition/partition.sql)
Materialization。它能识别本批分区、创建临时分区并执行
`ALTER TABLE ... REPLACE PARTITION`。

用户配置方式是：

```python
materialized='partition'
```

而不是：

```python
materialized='incremental',
incremental_strategy='insert_overwrite'
```

它与动态分区替换的目标相近，但不是 dbt 标准 Incremental Strategy，也没有让
当前 `insert_overwrite` 获得正确语义。

## 11. 当前代码状态总结

### 11.1 已经实现

- `append` 首次创建 Duplicate Key 表；
- `append` 第二次通过临时表和 `INSERT INTO` 追加；
- 当前名为 `insert_overwrite` 的 Unique Key Upsert；
- `is_incremental()` 条件编译；
- `--full-refresh` 重建和 Doris 表交换；
- 策略名、缺失 `unique_key` 和目标表模型的前置检查；
- 临时 Relation 清理和 Pre/Post Hook；
- Append、Upsert、Full Refresh 的真实 Doris Functional Test。

### 11.2 还没有实现

- 与名称一致的 Doris 原生 `insert_overwrite`；
- 标准 `delete+insert` 策略；
- 标准 `merge` 策略及列级更新配置；
- `microbatch` 的时间切片、批次执行、重试和并行；
- dbt 标准“策略名到策略宏”的 Dispatch 接口；
- 用户自定义 Incremental Strategy；
- 完整 `on_schema_change`；
- Incremental `persist_docs`；
- 准确的 Full Refresh Adapter Response；
- 与 `dbt-tests-adapter` Incremental 测试套件的完整兼容。

### 11.3 最容易记住的一句话

当前 dbt-doris 的 Incremental 不是“已经支持五种策略”，而是：

```text
Append 已经可用；
Unique Key Upsert 已经可用，但名字暂时叫 insert_overwrite；
真正的 Insert Overwrite、Delete+Insert、Merge 和 Microbatch 仍待实现。
```

## 12. 代码和语法依据

- dbt 官方：
  [Incremental models](https://docs.getdbt.com/docs/build/incremental-models)、
  [Incremental strategies](https://docs.getdbt.com/docs/build/incremental-strategy)、
  [Microbatch](https://docs.getdbt.com/docs/build/incremental-microbatch)；
- dbt-doris 当前策略实现：
  [`incremental.sql`](../dbt/include/doris/macros/materializations/incremental/incremental.sql)；
- dbt-doris 辅助宏：
  [`help.sql`](../dbt/include/doris/macros/materializations/incremental/help.sql)；
- dbt-doris Incremental Functional Test：
  [`test_doris_incremental.py`](../test/functional/adapter/test_doris_incremental.py)；
- Doris 原生 Insert Overwrite 用例：
  [`insert_overwrite_table_range.groovy`](../../../regression-test/suites/insert_overwrite_p0/insert_overwrite_table_range.groovy)；
- Doris `DELETE ... USING` 用例：
  [`test_delete_using.groovy`](../../../regression-test/suites/delete_p0/test_delete_using.groovy)；
- Doris `MERGE INTO` 用例：
  [`test_merge_into.groovy`](../../../regression-test/suites/load_p0/merge_into/test_merge_into.groovy)。
