# dbt-doris Aggregate Key 实现设计

> 状态：设计方案，尚未实现
>
> 范围：先支持 `materialized='table'`，Incremental 留到后续阶段
>
> 依据：当前 dbt-doris 代码、Apache Doris 官方文档和公开 Adapter 设计

## 1. 先说结论

Aggregate Key 在 dbt-doris 中不应该只是增加一段
`AGGREGATE KEY (...)` SQL。

原因是 Doris Aggregate Key 表同时要求：

1. 指定哪些列是 Key；
2. Key 列必须排在所有 Value 列之前；
3. 每个 Value 列都要声明 `SUM`、`MAX`、`REPLACE` 等聚合函数；
4. 建表时必须知道每个列的准确类型；
5. 分区列和 HASH 分桶列必须是 Key 列。

当前 dbt-doris 的 Table Materialization 主要使用
`CREATE TABLE ... AS SELECT`，依靠 Doris 从查询结果推断列定义。
这个流程没有位置给每个 Value 列添加聚合函数，因此不能直接生成正确的
Aggregate Key DDL。

建议的实现方式是：

```text
Model SQL + Config + Contract
              |
              v
dbt Core 解析 Model、依赖和列声明
              |
              v
dbt-doris 校验 Aggregate Key 配置
              |
              +-- 单独执行 CREATE TABLE（显式列定义）
              |
              +-- 单独执行 INSERT INTO ... SELECT ...
              |
              v
Doris 按 Key 和逐列聚合函数存储数据
```

对 dbt Core 来说，它仍然是一张普通的 Table Model；Aggregate Key 是
dbt-doris 负责解释的 Doris 物理表设计。

## 2. Aggregate Key 到底解决什么问题

假设模型查询产出下面三行：

| order_date | shop_id | order_count | sales_amount | max_order_amount |
| --- | ---: | ---: | ---: | ---: |
| 2026-07-01 | 1001 | 1 | 30.00 | 30.00 |
| 2026-07-01 | 1001 | 1 | 70.00 | 70.00 |
| 2026-07-01 | 1002 | 1 | 20.00 | 20.00 |

如果表定义为：

```sql
AGGREGATE KEY(order_date, shop_id)
```

并为三个 Value 列分别配置：

```text
order_count      -> SUM
sales_amount     -> SUM
max_order_amount -> MAX
```

Doris 查询时得到：

| order_date | shop_id | order_count | sales_amount | max_order_amount |
| --- | ---: | ---: | ---: | ---: |
| 2026-07-01 | 1001 | 2 | 100.00 | 70.00 |
| 2026-07-01 | 1002 | 1 | 20.00 | 20.00 |

这里的聚合不是 dbt 自动给 Model SQL 增加 `GROUP BY`，而是 Doris 在写入、
Compaction 和查询阶段按照表定义完成的。

常用聚合函数如下：

| 聚合函数 | 作用 | 第一阶段建议 |
| --- | --- | --- |
| `SUM` | 累加相同 Key 的数值 | 支持 |
| `MIN` | 保留最小值 | 支持 |
| `MAX` | 保留最大值 | 支持 |
| `REPLACE` | 使用后写入的值替换原值 | 支持 |
| `REPLACE_IF_NOT_NULL` | 只用非 NULL 新值替换原值 | 支持 |
| `HLL_UNION` | 合并 HLL 中间状态 | 后续支持 |
| `BITMAP_UNION` | 合并 Bitmap 中间状态 | 后续支持 |
| `QUANTILE_UNION` | 合并 Quantile State | 后续支持 |
| `AGG_STATE` | 保存和合并自定义聚合状态 | 后续单独设计 |

第一阶段先支持五种普通聚合函数，是因为 HLL、Bitmap、Quantile 和
`AGG_STATE` 不只涉及一个函数名，还涉及特殊列类型和 Model SQL 中的状态构造
函数，需要单独验证类型和写入语义。

## 3. 用户最终怎样使用

### 3.1 Model Config

建议使用统一的表模型配置，而不是继续增加第三个独立的
`aggregate_key`：

```sql
-- models/fct_shop_sales_daily.sql

{{ config(
    materialized='table',
    table_type='aggregate',
    keys=['order_date', 'shop_id'],
    aggregate_functions={
        'order_count': 'SUM',
        'sales_amount': 'SUM',
        'max_order_amount': 'MAX',
        'last_order_status': 'REPLACE_IF_NOT_NULL'
    },
    partition_by=['order_date'],
    partition_type='RANGE',
    partition_by_init=[
        "PARTITION p202607 VALUES LESS THAN ('2026-08-01')",
        "PARTITION pmax VALUES LESS THAN ('9999-12-31')"
    ],
    distributed_by=['shop_id'],
    buckets=10,
    properties={'replication_num': '1'}
) }}

select
    cast(order_time as date) as order_date,
    shop_id,
    1 as order_count,
    order_amount as sales_amount,
    order_amount as max_order_amount,
    order_status as last_order_status
from {{ ref('stg_orders') }}
```

三个核心配置的职责是：

| Config | 含义 |
| --- | --- |
| `table_type='aggregate'` | 选择 Doris Aggregate Key 表模型 |
| `keys` | 指定维度列，同时生成 `AGGREGATE KEY(...)` |
| `aggregate_functions` | 指定每个 Value 列的存储聚合函数 |

### 3.2 Schema Contract

Aggregate Key 建表必须知道准确的列类型，因此第一阶段要求开启
Model Contract：

```yaml
# models/schema.yml

version: 2

models:
  - name: fct_shop_sales_daily
    description: 按天、店铺预聚合的销售指标

    config:
      contract:
        enforced: true

    columns:
      # Key 列必须写在 Value 列之前，顺序与 keys 一致。
      - name: order_date
        data_type: date

      - name: shop_id
        data_type: bigint

      - name: order_count
        data_type: bigint

      - name: sales_amount
        data_type: decimal(18, 2)

      - name: max_order_amount
        data_type: decimal(18, 2)

      - name: last_order_status
        data_type: string
```

Contract 在这里不只是文档校验，还承担两个实现职责：

1. 为显式 `CREATE TABLE` 提供列名和列类型；
2. 校验 Model SQL 实际产出的列与声明一致。

如果没有 Contract，Adapter 只能再次从查询推断类型，容易出现
`BIGINT`、`DECIMAL`、字符串宽度或特殊聚合类型不一致。因此第一阶段遇到
Aggregate Key 但没有 `contract.enforced: true` 时，应在编译期直接报错。

这不是说 Aggregate Key 永远必须依赖 YAML。dbt Core 已提供
`get_column_schema_from_query()`，后续可以用空结果查询自动取得列类型。
但当前 Doris Connection 的类型映射主要根据 MySQL Type Code 返回
`DECIMAL`、`VARCHAR` 等通用名称，没有完整保留 Precision、Scale 和长度；
在补齐这部分类型推断之前，Contract 是更可靠的第一阶段边界。

### 3.3 预期生成的 SQL

dbt-doris 应分两次调用 Connector，不能把两条 SQL 拼进一个
`execute()`。

第一条 SQL 显式创建中间表：

```sql
create table `analytics`.`fct_shop_sales_daily__dbt_tmp`
(
    `order_date` DATE,
    `shop_id` BIGINT,
    `order_count` BIGINT SUM,
    `sales_amount` DECIMAL(18, 2) SUM,
    `max_order_amount` DECIMAL(18, 2) MAX,
    `last_order_status` STRING REPLACE_IF_NOT_NULL
)
ENGINE = OLAP
AGGREGATE KEY (`order_date`, `shop_id`)
PARTITION BY RANGE (`order_date`)
(
    PARTITION p202607 VALUES LESS THAN ('2026-08-01'),
    PARTITION pmax VALUES LESS THAN ('9999-12-31')
)
DISTRIBUTED BY HASH (`shop_id`) BUCKETS 10
PROPERTIES (
    "replication_num" = "1"
);
```

第二条 SQL 写入 Model 查询结果：

```sql
insert into `analytics`.`fct_shop_sales_daily__dbt_tmp`
(
    `order_date`,
    `shop_id`,
    `order_count`,
    `sales_amount`,
    `max_order_amount`,
    `last_order_status`
)
select
    cast(`order_date` as DATE),
    cast(`shop_id` as BIGINT),
    cast(`order_count` as BIGINT),
    cast(`sales_amount` as DECIMAL(18, 2)),
    cast(`max_order_amount` as DECIMAL(18, 2)),
    cast(`last_order_status` as STRING)
from (
    -- 编译后的 Model SQL
) `_dbt_aggregate_source`;
```

写入成功后，继续复用当前 Table Materialization 的 Exchange/Rename 流程，
把中间表替换成目标表。

## 4. dbt 怎么“理解”这张表

需要区分 dbt Core、dbt-doris 和 Doris 三层。

| 层 | 它理解什么 |
| --- | --- |
| dbt Core | 这是一个 `materialized='table'` 的 Model；知道 SQL、`ref()` 依赖、Contract、Test、Docs 和目标 Relation |
| dbt-doris | 读取 `table_type`、`keys` 和 `aggregate_functions`，校验配置并生成 Doris DDL、INSERT 和替换流程 |
| Doris | 真正理解 `AGGREGATE KEY`，在写入、Compaction 和查询时执行逐列聚合 |

因此不需要给 dbt Core 新增一种 Resource Type，也不需要创建新的
`RelationType`。构建完成后，它仍然是：

```text
RelationType.Table
```

dbt 的 Lineage 仍来自 Model 中的 `ref()`；Data Test、Docs 和下游 Model
也继续把它当作普通 Relation 使用。

所谓“dbt 支持 Aggregate Key”，准确地说是：

> dbt Core 保存用户声明的 Doris Config，dbt-doris 把这些 Config
> 变成正确的 Aggregate Key DDL 和写入行为。

dbt 不会自己模拟 Doris 的存储聚合，也不需要在编译阶段计算最终聚合结果。

## 5. 为什么当前实现不能直接支持

当前 Table 构建主路径是：

```text
macros/materializations/table/table.sql
        |
        v
doris__create_table_as(...)
        |
        v
CREATE TABLE ... AS SELECT ...
```

相关实现文件：

- `dbt/include/doris/macros/materializations/table/table.sql`
- `dbt/include/doris/macros/materializations/table/create_table_as.sql`
- `dbt/include/doris/macros/adapters/relation.sql`
- `dbt/adapters/doris/impl.py`

当前 `doris__create_table_as` 可以插入：

- Duplicate Key；
- 表注释；
- Partition；
- Distribution；
- Properties。

Incremental 的另一条路径还可以生成 Unique Key。

但是当前列处理只是在开启 Contract 时，把查询结果写成：

```sql
select
    cast(column_a as type_a),
    cast(column_b as type_b)
from (...)
```

它不是下面这种物理列定义：

```sql
column_a BIGINT SUM
```

所以只增加：

```jinja
{% macro doris__aggregate_key() %}
  AGGREGATE KEY (...)
{% endmacro %}
```

仍然不够，Doris 会发现 Value 列没有声明聚合函数。

## 6. 对外 Config 设计

### 6.1 推荐的新入口

建议统一为：

```yaml
table_type: duplicate | unique | aggregate
keys: [k1, k2]
aggregate_functions:
  value_1: SUM
  value_2: MAX
```

原因是 `duplicate_key` 和 `unique_key` 已经形成了两个不同入口。
如果再增加 `aggregate_key`，用户需要记住三组互斥 Config，Adapter 也要在
每个 Materialization 中重复判断冲突。

统一入口更容易表达：

```text
先选择 Table Model
        |
再声明 Key 列
        |
最后声明该模型独有的选项
```

### 6.2 兼容现有配置

现有项目不能被强制迁移，建议使用下面的兼容规则：

| 用户配置 | Adapter 解析结果 |
| --- | --- |
| 只有 `duplicate_key` | 继续按现有 Duplicate Key 行为执行 |
| Incremental 中只有 `unique_key` | 继续按现有 Unique Key Upsert 行为执行 |
| `table_type='duplicate'` + `keys` | 使用新的统一入口 |
| `table_type='unique'` + `keys` | 使用新的统一入口，后续逐步接入完整 Unique Key 选项 |
| `table_type='aggregate'` + `keys` + `aggregate_functions` | 使用新的 Aggregate Key 流程 |
| 新旧入口同时出现 | 编译失败，要求用户只保留一种写法 |

第一阶段不删除 `duplicate_key` 或改变 Incremental 的 `unique_key` 语义。
统一入口先服务于 Aggregate Key，并为后续整理另外两种模型提供迁移路径。

### 6.3 必须在编译期校验的规则

Adapter 应在执行 DDL 前完成以下校验：

1. `table_type` 只能是 `duplicate`、`unique` 或 `aggregate`；
2. Aggregate Key 只允许 `ENGINE=OLAP`；
3. `keys` 不能为空；
4. 必须开启 `contract.enforced`；
5. Contract 中的前 K 个列必须与 `keys` 顺序完全一致；
6. `keys` 中的每个列必须存在于 Contract；
7. 每个非 Key 列都必须出现在 `aggregate_functions`；
8. Key 列不能出现在 `aggregate_functions`；
9. `aggregate_functions` 不能引用 Model 中不存在的列；
10. 聚合函数名不区分大小写，但生成 SQL 前统一转成大写；
11. 第一阶段只接受 `SUM`、`MIN`、`MAX`、`REPLACE`、
    `REPLACE_IF_NOT_NULL`；
12. `partition_by` 中的列必须属于 `keys`；
13. `distributed_by` 中的列必须属于 `keys`；
14. Aggregate Key 暂不允许 `materialized='incremental'` 或自定义
    Partition Materialization。

这些属于确定的 Doris DDL 前置条件，应该由 Adapter 给出具体错误，而不是等
Doris 返回一条难以定位的建表失败。

类型与聚合函数的完整兼容矩阵会随 Doris 版本和特殊类型变化。第一阶段可以：

- 对明显错误做 Adapter 校验，例如字符串列使用 `SUM`；
- 对复杂类型和版本边界交给 Doris 做最终校验；
- 在错误信息中同时显示列名、数据类型和聚合函数。

## 7. 具体需要修改哪些代码

### 7.1 `dbt/adapters/doris/impl.py`

在 `DorisConfig` 中增加统一配置的类型声明：

```python
class TableType(str, Enum):
    duplicate = "duplicate"
    unique = "unique"
    aggregate = "aggregate"


class DorisConfig(AdapterConfig):
    # 现有配置保留
    table_type: TableType
    keys: Tuple[str]
    aggregate_functions: Dict[str, str]
```

这一层的职责是让 dbt 能识别这些 Adapter Config，并为后续校验提供稳定类型。

不建议在这里执行数据库操作。配置互斥、列顺序和函数覆盖检查更适合放在一个
统一的校验宏或纯 Python 校验函数中，并为它单独写 Unit Test。

### 7.2 `dbt/include/doris/macros/adapters/relation.sql`

建议新增四类宏：

```text
doris__resolve_table_model()
doris__validate_aggregate_table()
doris__key_clause()
doris__aggregate_column_definitions()
```

各自职责如下：

| 宏 | 职责 |
| --- | --- |
| `resolve_table_model` | 统一解析新旧 Config，发现互斥配置立即失败 |
| `validate_aggregate_table` | 校验 Key、Value、Contract、Partition 和 Distribution |
| `key_clause` | 根据表模型生成 Duplicate/Unique/Aggregate Key 子句 |
| `aggregate_column_definitions` | 根据 Contract 和聚合函数生成显式列定义 |

不要让 `doris__duplicate_key`、`doris__unique_key` 和新的 Aggregate 宏分别
重复读取 Config。先解析成统一结构，再由各个 SQL 宏消费。

解析后的内部结构可以理解为：

```text
{
  "type": "aggregate",
  "keys": ["order_date", "shop_id"],
  "aggregate_functions": {
    "order_count": "SUM",
    "sales_amount": "SUM"
  }
}
```

### 7.3 `dbt/include/doris/macros/materializations/table/create_table_as.sql`

保留现有 CTAS 宏供 Duplicate/Unique 等路径使用，另外增加两个宏：

```text
doris__create_aggregate_table(relation)
doris__insert_into_aggregate_table(relation, sql)
```

`create_aggregate_table` 只生成一条显式 DDL：

```sql
CREATE TABLE (...) AGGREGATE KEY (...) ...
```

`insert_into_aggregate_table` 只生成一条 DML：

```sql
INSERT INTO (...) SELECT ...
```

必须保持“一个宏返回一条语句”。当前 Connector 曾经因为把多条语句交给一次
`execute()` 而出现 `Commands out of sync`，所以不能为了方便把
`CREATE; INSERT;` 拼成一个字符串。

现有 `doris__table_colume_type(sql)` 中的 Contract 校验和 CAST 投影可以抽成
一个复用宏，让 CTAS 和 Aggregate INSERT 使用同一套列对齐逻辑。

### 7.4 `dbt/include/doris/macros/materializations/table/table.sql`

在构建中间表的位置增加表模型分支：

```jinja
{% set table_model = doris__resolve_table_model() %}

{% if table_model.type == 'aggregate' %}
  {{ doris__validate_aggregate_table(table_model) }}

  {% call statement('create_aggregate_table') %}
    {{ doris__create_aggregate_table(intermediate_relation, table_model) }}
  {% endcall %}

  {% call statement('main') %}
    {{ doris__insert_into_aggregate_table(
        intermediate_relation,
        sql
    ) }}
  {% endcall %}
{% else %}
  {% call statement('main') %}
    {{ doris__create_table_as(False, intermediate_relation, sql) }}
  {% endcall %}
{% endif %}
```

后面的流程不需要重写：

1. 创建 Intermediate Relation；
2. 写入数据；
3. 如果目标表已存在则 Exchange；
4. 否则 Rename；
5. Persist Docs；
6. 清理中间 Relation。

这样 Aggregate Key 能继续复用 Table Materialization 已有的替换语义。

### 7.5 `dbt/include/doris/macros/adapters/columns.sql`

新增显式物理列定义的渲染逻辑。它与当前 Contract CAST 的用途不同：

| 渲染 | 用途 |
| --- | --- |
| `column BIGINT SUM` | 用在 `CREATE TABLE (...)` |
| `cast(column as BIGINT)` | 用在 `INSERT INTO ... SELECT ...` |

不能把两者混成一个宏，否则会把 Doris DDL 语法带入查询投影。

列定义至少应包含：

```text
列名 + 数据类型 + 可选聚合函数 + 可选 COMMENT
```

默认值、NULL 属性和其他 Constraint 可以在已有 Contract 能力完善后统一接入，
不应为 Aggregate Key 再创建一套相互独立的 Constraint 语义。

### 7.6 `dbt/adapters/doris/doris_column_item.py`

如果继续复用当前 `DorisColumnItem`，应把“查询 CAST”和“DDL 列定义”拆成两个
明确的方法，例如：

```python
get_select_projection()
get_aggregate_column_definition(aggregate_function)
```

不要继续使用 `get_table_column_constraint()` 同时表达两种不同 SQL 位置，
否则后续接入 Default、Nullable 和 Column Constraint 时容易混淆。

## 8. 为什么第一阶段不直接支持 Incremental

Aggregate Key 可以接收持续写入，但这不代表它天然满足 dbt Incremental
的可重复执行语义。

例如 Value 列使用 `SUM`：

```text
第一次写入某批订单：sales_amount +100
同一批任务重跑：     sales_amount 再 +100
最终结果：            +200
```

Doris 按表定义正确执行了 `SUM`，但 dbt 任务因为重复处理同一批数据产生了
业务错误。

普通 Table Materialization 每次先创建新表，再整体替换旧表，因此重跑仍然是
幂等的。第一阶段只支持 Table，可以先把表模型、DDL 和 dbt 行为做正确。

后续支持 Incremental 时应按策略分别设计：

| 策略 | Aggregate Key 风险和方向 |
| --- | --- |
| Append | 只有在输入批次严格只处理一次时安全；默认不建议 |
| Dynamic/Partition Overwrite | 重新计算并替换受影响分区，更适合时间汇总表 |
| Microbatch | 需要为每个 Event Time 批次提供可重跑的分区替换 |
| Unique Key Upsert | 与 Aggregate Key 语义不同，不能复用当前 Unique Key 路径 |

如果 `table_type`、`keys` 或 `aggregate_functions` 发生变化，Incremental
目标表也不能继续沿用，应要求 Full Refresh 或执行明确的重建流程。

## 9. 可以参考其他 Adapter 的什么

### 9.1 StarRocks：参考统一表模型入口

dbt-starrocks 公开使用：

```yaml
table_type: PRIMARY | DUPLICATE | UNIQUE
keys: [...]
```

这个设计值得参考，因为表模型和 Key 使用统一入口，而不是每种表模型定义一个
独立 Config。

但其当前公开功能没有把 Aggregate 表模型列为已支持能力，公开的
`table_type` 说明也只列出 Primary、Duplicate 和 Unique。因此可以借鉴它的
Config 结构，不能直接复制一个现成的 Aggregate Key 实现。

### 9.2 ClickHouse：参考“数据库物理设计属于 Model Config”

dbt-clickhouse 允许 Model 配置：

```yaml
engine: AggregatingMergeTree
order_by: [...]
partition_by: [...]
```

当使用 `AggregatingMergeTree` 时，Model SQL 还需要产生
`AggregateFunction` 或 `SimpleAggregateFunction` 类型及对应 State。

它说明了两个可借鉴原则：

1. 数据库原生表引擎或表模型应该作为 Model Config 暴露；
2. 当物理表要求精确列类型时，不能只靠一个通用 Materialization 名称，
   Model SQL、Contract 和 Adapter DDL 必须配合。

ClickHouse 的 Aggregate State 与 Doris 的 Value 列聚合函数不是同一种语义，
因此只能参考分层方式，不能直接复用其 SQL。

### 9.3 对比结论

| 产品 | 公开做法 | 对 dbt-doris 的启发 |
| --- | --- | --- |
| StarRocks | `table_type + keys` | 统一三种 Doris 表模型的 Config 入口 |
| ClickHouse | `engine + order_by + partition_by`，复杂聚合类型由 Model SQL 明确产生 | 原生物理设计进入 Config，精确 Schema 由 Model/Contract 配合 |
| Doris 目标方案 | `table_type + keys + aggregate_functions` | 在统一入口上增加 Doris 独有的逐列聚合语义 |

## 10. 测试应该怎样补

### 10.1 Unit Test

在 `test/unit/test_macro_behavior.py` 增加以下用例：

1. Aggregate DDL 包含 `AGGREGATE KEY`；
2. 每个 Value 列带正确聚合函数；
3. CREATE 和 INSERT 分别只有一条 SQL；
4. Aggregate Key 缺少 Contract 时编译失败；
5. 缺少 `keys` 时编译失败；
6. Value 列漏配聚合函数时编译失败；
7. Key 列错误配置聚合函数时编译失败；
8. 聚合函数引用不存在的列时编译失败；
9. Partition/Distribution 使用 Value 列时编译失败；
10. 新旧 Config 同时出现时编译失败；
11. Aggregate Key 配置到 Incremental 时编译失败；
12. 现有 Duplicate/Unique SQL 保持不变。

### 10.2 Functional Test

建议新增：

```text
test/functional/adapter/test_doris_aggregate_table.py
```

端到端验证：

1. 用两行相同 Key 数据创建 Aggregate Key 表；
2. `SHOW CREATE TABLE` 中存在 `AGGREGATE KEY`、`SUM`、`MIN`、`MAX`；
3. 查询结果已按 Key 聚合；
4. 第二次执行 `dbt run` 后结果没有重复累计；
5. 修改 Model SQL 后 Table 能通过 Intermediate + Exchange 正确替换；
6. `dbt test` 可以正常测试最终 Relation；
7. `dbt docs generate` 可以读取列和表信息；
8. Persist Docs 不会破坏聚合列定义。

测试数据优先使用 `SUM`、`MIN` 和 `MAX`。`REPLACE` 在没有 Sequence
控制时不适合依赖同一批数据的输入顺序做精确断言，可重点检查 DDL，或者为测试
设计不同批次的确定性写入。

### 10.3 回归检查

实现完成后至少执行：

```bash
/tmp/dbtci-verify/bin/pytest test/unit -q

/tmp/dbtci-verify/bin/pytest \
  test/functional/adapter/test_doris_aggregate_table.py -q
```

同时回归现有 Table、Incremental、Contract、Partition 和 Persist Docs
测试，确认统一配置解析没有改变旧项目行为。

## 11. 建议的实现顺序

### 第一阶段：Table 主路径

1. 增加 `table_type`、`keys`、`aggregate_functions`；
2. 增加统一配置解析和编译期校验；
3. 要求 Contract；
4. 实现显式 CREATE 和单独 INSERT；
5. 支持 `SUM`、`MIN`、`MAX`、`REPLACE`、`REPLACE_IF_NOT_NULL`；
6. 补 Unit 和 Functional Test；
7. 保持旧 Duplicate/Unique Config 兼容。

完成这一阶段后，用户可以稳定构建和重建 Aggregate Key Table。

### 第二阶段：复杂聚合类型

1. HLL；
2. Bitmap；
3. Quantile State；
4. `AGG_STATE`；
5. 聚合函数与数据类型兼容矩阵；
6. 对应 Package 和 Docs 示例。

### 第三阶段：增量与配置变化

1. Dynamic/Partition Overwrite；
2. Microbatch；
3. 失败重跑和幂等性；
4. 表模型或聚合函数变化时的 Full Refresh 提示；
5. Schema Change 边界。

## 12. 验收标准

第一阶段满足下面条件，才可以认为 dbt-doris 支持 Aggregate Key：

- 用户能在 Model Config 中声明 Aggregate Table、Key 和逐列聚合函数；
- dbt 在编译期拒绝不完整或互相冲突的配置；
- Adapter 生成显式列定义的 `CREATE TABLE ... AGGREGATE KEY`；
- CREATE 和 INSERT 使用两次独立 Connector 调用；
- Doris 中的表确实按照 SUM/MIN/MAX 等定义聚合；
- 普通 `dbt run` 重跑不会重复累计；
- `ref()`、Lineage、Test、Docs、Contract 和 Persist Docs 继续工作；
- 原有 Duplicate Key、Unique Key、Table 和 Incremental 行为不变；
- 文档明确第一阶段不支持 Aggregate Incremental。

## 13. 参考资料

- [Apache Doris Aggregate Model](https://doris.apache.org/docs/dev/table-design/data-model/aggregate/)
- [Apache Doris CREATE TABLE](https://doris.apache.org/docs/4.x/sql-manual/sql-statements/table-and-view/table/CREATE-TABLE/)
- [Apache Doris Manual Partitioning](https://doris.apache.org/docs/4.x/table-design/data-partitioning/manual-partitioning/)
- [Apache Doris Data Bucketing](https://doris.apache.org/docs/4.x/table-design/data-partitioning/data-bucketing/)
- [dbt-starrocks](https://github.com/StarRocks/dbt-starrocks)
- [dbt-clickhouse Materializations](https://clickhouse.com/docs/integrations/connectors/data-ingestion/etl-tools/dbt/materializations)
