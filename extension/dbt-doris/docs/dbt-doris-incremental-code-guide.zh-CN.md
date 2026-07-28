# dbt-doris Incremental 策略：代码实现与完整实战

> 本文同时面向 dbt 使用者和 dbt-doris 开发者。
> 当前状态以仓库中的 dbt-doris 1.0.0 实现为准；[`setup.py`](../setup.py) 声明的
> dbt Core 最低版本为 1.10.4。文中标为“未来设计示意”的 SQL
> **不是当前 dbt-doris 会生成的 SQL**。

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

### 2.1 `config`、Model SQL 和 Doris SQL 各负责什么

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

### 2.2 `target/compiled` 不等于最终执行的全部 SQL

`target/compiled/...sql` 主要展示经过 Jinja 渲染后的 Model 查询。对于普通
Incremental Model，它能帮助用户确认 `source()`、`ref()` 和过滤条件怎样展开，
但不会简单地等于 Doris 最终收到的一条 SQL。

运行时 Materialization 还会通过多个 statement 发送：

```text
DROP 临时表
CREATE 临时表 AS <Model 查询>
SHOW CREATE TABLE
INSERT INTO 目标表 SELECT ... FROM 临时表
DROP 临时表
```

因此排查生成 SQL 时需要同时看：

```bash
dbt compile --select <model>
dbt run --debug --select <model>
```

前者方便看编译后的 Model 查询，后者的 `logs/dbt.log` 才能看到
Materialization 在运行时发送的 DDL/DML。本文后面分别展示“运行时展开的
Model 查询”和“Adapter 发送的 Doris SQL”。

### 2.3 策略校验

`dbt_doris_validate_get_incremental_strategy()` 当前等价于：

```jinja
{% set strategy = config.get('incremental_strategy') or 'insert_overwrite' %}

{% if strategy not in ['append', 'insert_overwrite'] %}
    {{ exceptions.raise_compiler_error(...) }}
{% endif %}

{% if strategy == 'insert_overwrite' and not unique_key %}
    {{ exceptions.raise_compiler_error(...) }}
{% endif %}
```

这带来三个重要结果：

1. `delete+insert`、`merge`、`microbatch` 和任意自定义策略都会在这里被拒绝；
2. 当前默认策略是 `insert_overwrite`；
3. 默认策略要求 `unique_key`，所以只写
   `config(materialized='incremental')` 会报错。

dbt Core 1.2 以后允许项目通过 `get_incremental_<策略名>_sql` 扩展自定义策略，
但当前 dbt-doris 会先执行上述白名单校验，因此也没有接通这条扩展路径。

### 2.4 `is_incremental()` 什么时候为真

当前 Doris 宏检查：

```text
execute 为真
目标 Relation 已存在
目标类型是 Table
Model materialized 是 incremental 或 partition
当前没有 --full-refresh
```

条件全部满足才返回 `True`。因此：

- 第一次运行：目标表不存在，执行 Model 中的全量分支；
- 第二次普通运行：目标表存在，执行增量过滤分支；
- `dbt run --full-refresh`：即使表存在，也执行全量分支。

需要注意：`is_incremental()` 只决定 Model 查询返回哪些行，并不会自动判断
“哪些数据是新的”。时间戳、序号或回看窗口都需要用户在 Model 中写清楚。

### 2.5 首次运行

目标表不存在时：

```text
append
  -> doris__create_table_as
  -> CREATE TABLE ... DUPLICATE KEY ... AS <Model SQL>

当前 insert_overwrite
  -> doris__create_unique_table_as
  -> CREATE TABLE ... UNIQUE KEY ... AS <Model SQL>
```

首次构建没有临时表，也没有先执行 `INSERT INTO`。

### 2.6 第二次普通运行

目标表已经存在时，两条策略的主要流程实际上非常接近：

```text
DROP 临时 Relation
  -> CREATE TABLE 临时 Relation AS <本轮 Model SQL>
  -> 当前 Upsert 路径额外执行 SHOW CREATE TABLE 检查目标是否为 Unique Key
  -> tmp_insert()
  -> INSERT INTO 目标表 (...) SELECT ... FROM 临时表
  -> COMMIT dbt 连接状态
  -> DROP 临时 Relation
```

两者结果不同，不是因为 `tmp_insert()` 生成了不同 SQL，而是目标表模型不同：

- Duplicate Key 表接收相同 Key 时保留多行，所以表现为 Append；
- Unique Key 表接收相同 Key 时以新行覆盖旧行，所以表现为 Upsert。

### 2.7 Full Refresh

目标是 View 或用户传入 `--full-refresh` 时，当前代码：

1. 创建 `<目标表>__dbt_backup`；
2. 使用全量 Model SQL 填充 Backup；
3. 执行 Doris `ALTER TABLE ... REPLACE WITH TABLE`；
4. 使用 `select 'hello doris'` 作为主 statement 的占位 SQL。

因此 Full Refresh 能重建结果，但这个占位查询会让 Adapter Response 的影响行数
不准确。这也是现有 Incremental 仍需要完善的边界。

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

## 4. 已实现：Append 游戏事件日志

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

当前 Adapter 发送的核心 Doris SQL形态是：

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

## 5. 已实现但命名有问题：外卖订单 Unique Key Upsert

### 5.1 场景

外卖订单会从“已接单”变成“配送中”或“已送达”。本批可能既有旧订单的完整新状态，
也有第一次出现的新订单。目标表只需要保留每个 `order_id` 的最新完整状态。

这正是当前 dbt-doris 已实现的 Unique Key Upsert，但用户必须暂时写成：

```python
incremental_strategy='insert_overwrite'
```

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
        incremental_strategy='insert_overwrite',
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

这里的 Model 必须返回完整的新行。当前实现没有提供“只更新部分列”的独立语义。

### 5.4 第一次执行

```bash
dbt run --profiles-dir profiles --select delivery_order_current
```

首次运行的 Model 查询读取三条源数据。当前 Adapter 生成的是 Unique Key CTAS：

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

当前 Materialization 会先确认目标是 Unique Key 表：

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

请注意，SQL 是 `INSERT INTO`，没有 `OVERWRITE`。更新行为来自目标表的
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

现有 Functional Test 验证的也是同一语义：一条旧 Key 更新、一条新 Key 插入、
一条不在本批的历史记录保留。

### 5.6 为什么说名字有问题

Unique Key Upsert 的语义是：

```text
本批出现的 Key       -> 更新或新增
本批没有出现的旧 Key -> 保留
```

Insert Overwrite 的语义是：

```text
被覆盖分区中的旧数据 -> 整体移除
本批分区数据         -> 成为该分区的完整新内容
其他分区             -> 保留
```

两个策略在“本批没有出现的旧 Key”上会得到相反结果，不能只把当前实现理解成
Insert Overwrite 的另一种写法。

### 5.7 缺少 `unique_key` 会发生什么

如果 Model 写成：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite'
    )
}}

select * from {{ source('delivery', 'order_changes') }}
```

执行：

```bash
dbt run --profiles-dir profiles --select delivery_order_current
```

会在策略校验阶段失败，错误信息核心内容是：

```text
Incremental strategy 'insert_overwrite' requires a 'unique_key' config on model
model.doris_incremental_lab.delivery_order_current.

Either add the key columns:
    {{ config(materialized='incremental', unique_key=['<your_key>']) }}
or, if appending every row is what you want, say so explicitly:
    {{ config(materialized='incremental', incremental_strategy='append') }}
```

此时不会为这个 Model 创建目标表或执行 Incremental DML。

如果连 `incremental_strategy` 也不写：

```sql
{{ config(materialized='incremental') }}
select ...
```

结果相同，因为当前默认值就是 `insert_overwrite`。

## 6. 官方 Insert Overwrite：当前未实现

### 6.1 场景

广告归因表按日期分区。7 月 27 日第一次计算出了 Campaign A 和 B；后来归因规则
修正，整天需要重算，新结果只有 A 和 D。正确的分区覆盖完成后，旧的 B 必须从
7 月 27 日分区消失，7 月 26 日的数据不受影响。

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

### 6.3 按官方语义编写 Model

用户自然会写成：

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by=['stat_date'],
        partition_type='RANGE',
        partition_by_init=[
            "PARTITION p20260726 VALUES LESS THAN ('2026-07-27')",
            "PARTITION p20260727 VALUES LESS THAN ('2026-07-28')",
            "PARTITION pmax VALUES LESS THAN (MAXVALUE)"
        ],
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

第一次执行：

```bash
dbt run --profiles-dir profiles --select daily_ad_attribution
```

当前真实结果不是创建分区表，而是在策略校验阶段报错：

```text
Incremental strategy 'insert_overwrite' requires a 'unique_key' config ...
```

没有该 Model 的 CTAS、`INSERT INTO` 或 `INSERT OVERWRITE` 发往 Doris。

### 6.4 加上 `unique_key` 也不等于支持了覆盖

为了让当前代码接受配置，假设用户加上：

```python
unique_key=['stat_date', 'campaign_id']
```

第一次运行会创建：

```sql
CREATE TABLE `dbt_incremental_lab`.`daily_ad_attribution`
UNIQUE KEY (`stat_date`, `campaign_id`)
COMMENT ''
PARTITION BY RANGE (`stat_date`) (
    PARTITION p20260726 VALUES LESS THAN ('2026-07-27'),
    PARTITION p20260727 VALUES LESS THAN ('2026-07-28'),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
)
DISTRIBUTED BY HASH (`campaign_id`) BUCKETS 1
PROPERTIES ("replication_num" = "1") AS
select
    stat_date,
    campaign_id,
    batch_id,
    attributed_orders,
    attributed_revenue
from `dbt_incremental_lab`.`raw_ad_attribution_batches`;
```

此时第一次结果为：

| stat_date | campaign_id | batch_id | attributed_orders |
| --- | ---: | ---: | ---: |
| 2026-07-26 | 900 | 1 | 5 |
| 2026-07-27 | 101 | 1 | 10 |
| 2026-07-27 | 102 | 1 | 20 |

写入第二批重算结果：

```sql
INSERT INTO dbt_incremental_lab.raw_ad_attribution_batches VALUES
    ('2026-07-27', 101, 2, 12, 1200.00),
    ('2026-07-27', 104, 2,  8,  880.00);
```

再次执行：

```bash
dbt run --profiles-dir profiles --select daily_ad_attribution
```

当前真实生成的写入仍然是：

```sql
INSERT INTO `dbt_incremental_lab`.`daily_ad_attribution`
    (`stat_date`, `campaign_id`, `batch_id`,
     `attributed_orders`, `attributed_revenue`)
(
    SELECT
        `stat_date`, `campaign_id`, `batch_id`,
        `attributed_orders`, `attributed_revenue`
    FROM `dbt_incremental_lab`.`daily_ad_attribution__dbt_tmp`
);
```

结果：

| stat_date | campaign_id | batch_id | attributed_orders |
| --- | ---: | ---: | ---: |
| 2026-07-26 | 900 | 1 | 5 |
| 2026-07-27 | 101 | 2 | 12 |
| 2026-07-27 | 102 | 1 | 20 |
| 2026-07-27 | 104 | 2 | 8 |

Campaign B（`campaign_id=102`）仍然存在。这证明当前路径是按 Key Upsert，
没有覆盖 7 月 27 日整个分区。

### 6.5 正确实现后应该是什么

> **未来设计示意，当前 dbt-doris 不会生成下面的 SQL。**

对 7 月 27 日进行真正覆盖时，目标 SQL 应使用 Doris 原生分区覆盖：

```sql
INSERT OVERWRITE TABLE `dbt_incremental_lab`.`daily_ad_attribution`
PARTITION (p20260727)
    (`stat_date`, `campaign_id`, `batch_id`,
     `attributed_orders`, `attributed_revenue`)
SELECT
    `stat_date`, `campaign_id`, `batch_id`,
    `attributed_orders`, `attributed_revenue`
FROM `dbt_incremental_lab`.`daily_ad_attribution__dbt_tmp`
WHERE stat_date = '2026-07-27';
```

正确结果应该是：

| stat_date | campaign_id | batch_id | attributed_orders |
| --- | ---: | ---: | ---: |
| 2026-07-26 | 900 | 1 | 5 |
| 2026-07-27 | 101 | 2 | 12 |
| 2026-07-27 | 104 | 2 | 8 |

`campaign_id=102` 被移除，而 7 月 26 日分区保持不变。

Doris 本身已经支持：

```sql
INSERT OVERWRITE TABLE target SELECT ...;
INSERT OVERWRITE TABLE target PARTITION (p1, p2) SELECT ...;
INSERT OVERWRITE TABLE target PARTITION (*) SELECT ...;
```

当前缺的是 dbt-doris 对策略 Config、分区选择、运行生命周期和兼容迁移的接入，
不是 Doris 数据库缺少 `INSERT OVERWRITE`。

## 7. Delete+Insert：当前未实现

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

### 7.3 当前两轮执行会发生什么

第一次执行：

```bash
dbt run --profiles-dir profiles --select restaurant_order_detail
```

当前真实错误：

```text
Invalid incremental strategy provided: delete+insert
Expected one of: 'append', 'insert_overwrite'
```

第二次执行仍会得到同一个错误，因为第一次没有创建目标表：

```bash
dbt run --profiles-dir profiles --select restaurant_order_detail
```

策略校验发生在首次建表、临时表和 Hook 之前。该 Model 没有 CTAS、Delete 或
Insert SQL 发往 Doris。

虽然 [`help.sql`](../dbt/include/doris/macros/materializations/incremental/help.sql)
里存在 `tmp_delete()`，但仓库中没有调用点，而且它也没有接入
`get_incremental_delete_insert_sql`。一个未被调用的辅助宏不能算功能支持。

### 7.4 正确实现后的第二批过程

> **未来设计示意，当前 dbt-doris 不会生成下面的 SQL，也不会得到下面的结果。**

假设未来首次运行已经得到：

| order_id | correction_seq | restaurant_id | dish_count | payable_amount |
| ---: | ---: | ---: | ---: | ---: |
| 201 | 1 | 81 | 2 | 45.00 |
| 202 | 2 | 81 | 1 | 28.00 |
| 203 | 3 | 93 | 4 | 96.00 |

第二批把 201 的菜品数量和金额改正，同时新增 204：

```sql
INSERT INTO dbt_incremental_lab.raw_order_corrections VALUES
    (201, 4, 81, 3, 63.00, '2026-07-28 12:30:00'),
    (204, 5, 93, 2, 52.00, '2026-07-28 12:31:00');
```

用户仍然执行：

```bash
dbt run --profiles-dir profiles --select restaurant_order_detail
```

未来策略应先把本批 Model 结果放入临时表，然后按 `unique_key` 删除并插入：

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
Doris 仓库的 `delete_p0/test_delete_using.groovy` 已有 `DELETE FROM ... USING`
语法测试；dbt-doris 仍需补表模型约束、事务边界、组合 Key 和失败恢复。

## 8. Merge：当前未实现

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

### 8.3 当前真实执行结果

第一次和第二次运行都会在同一个校验点失败：

```bash
dbt run --profiles-dir profiles --select member_level_current
```

```text
Invalid incremental strategy provided: merge
Expected one of: 'append', 'insert_overwrite'
```

没有该 Model 的建表或 Merge DML 发往 Doris。当前 Unique Key Upsert 能实现这个
简单案例的最终数据效果，但它不等于 dbt 的标准 Merge 接口，也没有接入
`merge_update_columns`、`merge_exclude_columns` 或 Incremental Predicates。

### 8.4 正确实现后的第二批过程

> **未来设计示意，当前 dbt-doris 不会生成下面的 SQL，也不会得到下面的结果。**

假设未来第一次运行已创建 Doris Unique Key 目标，并写入 301、302、303。第二批：

```sql
INSERT INTO dbt_incremental_lab.raw_member_changes VALUES
    (301, 4, 'gold',  5200, '2026-07-28 14:00:00'),
    (304, 5, 'bronze', 100, '2026-07-28 14:01:00');
```

执行：

```bash
dbt run --profiles-dir profiles --select member_level_current
```

未来的核心 Doris SQL 可以是：

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

正确结果：

| member_id | change_seq | member_level | points |
| ---: | ---: | --- | ---: |
| 301 | 4 | gold | 5200 |
| 302 | 2 | gold | 5600 |
| 303 | 3 | bronze | 300 |
| 304 | 5 | bronze | 100 |

Doris 仓库的 `load_p0/merge_into/test_merge_into.groovy` 已有
`MERGE INTO ... WHEN MATCHED ... WHEN NOT MATCHED` 测试。Adapter 仍需决定支持的
Doris 版本和表模型，并把 dbt 的列更新配置、组合 Key、NULL 安全比较和
Predicates 正确翻译进去。

## 9. Microbatch：当前未实现

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

### 9.3 当前真实执行结果

```bash
dbt run \
  --profiles-dir profiles \
  --select hourly_device_temperature \
  --event-time-start "2026-07-28 10:00:00" \
  --event-time-end "2026-07-28 12:00:00"
```

当前 dbt-doris 在自己的策略校验中拒绝 `microbatch`：

```text
Invalid incremental strategy provided: microbatch
Expected one of: 'append', 'insert_overwrite'
```

不会为 10:00 和 11:00 生成两个 Doris 批次 DML，也不会创建目标表。

### 9.4 正确实现后应怎样拆批

> **未来设计示意，当前 dbt-doris 不会生成下面的 SQL，也不会得到下面的结果。**

10:00 批次的 Model 查询应被限制为：

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

11:00 批次应使用：

```sql
where event_time >= '2026-07-28 11:00:00'
  and event_time <  '2026-07-28 12:00:00'
```

每个批次还需要一种幂等的“完整批次替换”机制。对于按小时分区的 Doris 目标，
可以设计为：

```sql
INSERT OVERWRITE TABLE `dbt_incremental_lab`.`hourly_device_temperature`
PARTITION (p2026072810)
SELECT * FROM `hourly_device_temperature__dbt_tmp_2026072810`;

INSERT OVERWRITE TABLE `dbt_incremental_lab`.`hourly_device_temperature`
PARTITION (p2026072811)
SELECT * FROM `hourly_device_temperature__dbt_tmp_2026072811`;
```

预期结果：

| hour_start | device_id | reading_count | avg_temperature |
| --- | ---: | ---: | ---: |
| 2026-07-28 10:00:00 | 51 | 2 | 21.80 |
| 2026-07-28 11:00:00 | 52 | 1 | 29.30 |

如果后来补到一条 10:20 的晚到数据：

```sql
INSERT INTO dbt_incremental_lab.raw_iot_temperature VALUES
    (404, 51, '2026-07-28 10:20:00', 22.70);
```

`lookback=1` 应让下一次运行重新处理前一个小时，覆盖后的 10:00 结果变成：

| hour_start | device_id | reading_count | avg_temperature |
| --- | ---: | ---: | ---: |
| 2026-07-28 10:00:00 | 51 | 3 | 22.10 |

真正接入还需要声明 dbt Microbatch 能力，处理自动时间过滤、目标书签、批次重试、
并行执行和每批幂等写入，不能只增加一个策略名称。

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
