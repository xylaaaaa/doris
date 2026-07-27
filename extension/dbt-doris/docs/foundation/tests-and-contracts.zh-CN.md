# dbt-doris 基础功能实施方案：Test 与 Contract

> 状态：实施方案。本文的“目标”和“建议”不代表当前已经支持，当前能力以第 2 节为准。

## 1. 目标

这项工作包含三个不同层次：

| 层次 | 作用 |
| --- | --- |
| Data Test | 构建后查询数据，发现违反业务规则的记录 |
| Unit Test | 在给定模拟输入下验证 Model SQL 的输出 |
| Model Contract | 在构建前校验 Model 的字段集合、顺序、类型和约束声明 |

目标是让三者在 Doris 上具有与 dbt 一致的失败语义，并明确哪些 Constraint
由 Doris 强制、哪些只用于文档、哪些不支持。

## 2. 当前实现

### 2.1 Data Test

当前已经可以运行：

- Generic Test；
- Singular Test；
- Ephemeral 上的 Singular Test；
- `not_null`、`unique`、`relationships`、`accepted_values` 等常见规则；
- 用户自定义 Test Macro。

基础官方测试已经通过，入口见
[`test_basic.py`](../../test/functional/adapter/test_basic.py)。

Data Test 的实际流程是：

```text
dbt 编译测试 SQL
        |
dbt-doris 提交给 Doris
        |
Doris 返回违规记录
        |
失败数超过阈值 -> warn 或 error
```

它是构建后的检查，不是数据库写入约束。

### 2.2 Unit Test

当前工作区的 `test/functional/adapter/test_doris_unit_test.py`
已经包含一个用户编写的 SQL Unit Test 主路径验证。

已经证明简单的 `ref()` 输入、行级 Mock 和期望结果可以在 Doris 上执行，但尚未
覆盖类型、大小写、Source、Ephemeral、版本化 Model 和无效输入。

### 2.3 Model Contract

当前已经做到：

- `contract.enforced: true` 时比较 SQL 输出与 YAML 声明列；
- 列集合匹配时按声明类型投影；
- Model 多出未声明列时失败；
- 未启用 Contract 时，`columns:` 只用于文档，不能删除 Model 输出字段。

实现位于
[`create_table_as.sql`](../../dbt/include/doris/macros/materializations/table/create_table_as.sql)，
当前工作区的验证位于
`test/functional/adapter/test_doris_contract.py`。

当前实现还没有完整处理物理 Constraint、View/Incremental 的全部 Contract 场景。

## 3. Data Test 具体补什么

### 3.1 建立内置测试矩阵

至少覆盖：

| 测试 | Doris 场景 |
| --- | --- |
| `not_null` | NULL 与非 NULL |
| `unique` | 单列、字符串、时间和复合业务 Key |
| `relationships` | 有效引用、孤儿记录、NULL 外键 |
| `accepted_values` | 字符串、数字、Quote 行为 |
| Singular Test | 直接返回异常记录 |
| Custom Generic Test | 参数、Column 和 Model 级调用 |
| Severity | `warn`、`error`、`error_if`、`warn_if` |
| Limit | 失败记录数量限制 |

复合 Key 不应假装由单列 `unique` 支持，应通过自定义 Test 或经过验证的 Package
Macro 实现。

### 3.2 支持 Store Failures

用户接口：

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        data_tests:
          - unique:
              config:
                store_failures: true
```

或者：

```bash
dbt test --store-failures
```

需要完成：

1. 创建 Audit Schema；
2. 使用 Doris Table Materialization 保存违规记录；
3. 重复运行时稳定替换同名失败表；
4. 通过 `store_failures_as` 明确 Table/View 支持范围；
5. 失败为零时定义保留还是删除旧失败表；
6. 让测试 Relation 支持 `properties.replication_num` 等 Doris 配置；
7. 返回准确的失败数和 Relation 名。

禁止把测试 SQL 和建表 SQL拼在同一个 `cursor.execute()` 中。

### 3.3 错误与 Artifact

`run_results.json` 中应准确记录：

- Pass、Warn、Fail、Error；
- `failures` 数量；
- 执行时间；
- 保存失败记录的 Relation；
- Doris 查询失败与业务规则失败的区别。

## 4. Unit Test 具体补什么

接入 dbt 官方 Unit Testing 套件，并覆盖：

- INT、BIGINT、DECIMAL、STRING、DATE、DATETIME、BOOLEAN；
- NULL；
- 字段名大小写；
- Quote Column；
- `ref()`、`source()`、Ephemeral；
- 增量 Model 的 `is_incremental` 分支；
- 多个输入；
- 重复行和无序结果；
- 缺字段、多字段、类型不兼容等无效 Mock；
- `--select test_type:unit`。

比较结果时需要规范 Doris 返回类型：

- MySQL Connector 的 Decimal、Datetime 和 Boolean；
- CHAR/VARCHAR/STRING；
- 浮点误差；
- 列名大小写。

不能为了通过 Unit Test 而改变生产 Model SQL。

## 5. Contract 目标语义

### 5.1 字段集合与顺序

启用 Contract 后：

- SQL 输出字段与 YAML 声明必须一一对应；
- 缺字段和多字段都失败；
- 重复字段失败；
- Quote 后的名称按 Doris 标识符规则比较；
- 物理表字段顺序以 YAML Contract 为准；
- Key、Partition 和 Distribution 引用的字段必须存在。

### 5.2 字段类型

建立 Doris 类型规范化：

```text
integer -> INT
decimal(10, 2) -> DECIMAL(10,2)
varchar(20) -> VARCHAR(20)
datetime(6) -> DATETIME(6)
```

比较时区分：

- 完全相同；
- 可安全扩大，例如部分 VARCHAR 长度扩大；
- 需要显式 Cast；
- 不兼容并失败。

类型别名只在语义确实相同时归一化，不能把所有整数宽度或 Decimal 精度都视为相同。

### 5.3 Constraint 支持矩阵

第一版建议明确：

| dbt Constraint | dbt-doris 行为 |
| --- | --- |
| `not_null` | 通过显式 Doris 列 DDL 生成 `NOT NULL`，由数据库强制 |
| `unique` | 不映射为普通 Constraint；如需 Unique Key 表，使用 Doris 表模型配置 |
| `primary_key` | 不映射为普通 Constraint；Doris Key Model 不是关系型 Primary Key |
| `foreign_key` | 标记 Unsupported，不生成虚假的强制约束 |
| `check` | 在确认 Doris 版本和 DDL 支持并完成测试前标记 Unsupported |
| 自定义表达式 | 默认 Unsupported |

“不支持”必须在编译期给出清晰 Warning 或 Error，不能静默丢弃后仍声称 Contract
已经完全执行。

### 5.4 Contract 建表方式

当前通过外层 `CAST` 影响 CTAS 推导类型，但这不足以表达 `NOT NULL` 等物理约束。
完整实现应改为两步：

```text
1. 根据 Contract 生成显式字段 DDL，并创建空目标或中间表
2. INSERT INTO ... SELECT ... 写入 Model 结果
```

显式 DDL 需要同时组合：

- Column Name、Type、Nullability 和 Comment；
- Duplicate/Unique/Aggregate Key；
- Partition；
- Distribution 和 Bucket；
- Table Properties。

写入失败时删除中间表，不能替换现有目标。

## 6. 代码改造

### 6.1 类型与 Constraint 层

在 Python Adapter 中增加：

- Doris 类型规范化与等价比较；
- Contract Column 渲染；
- Constraint 支持等级；
- 清晰的 Unsupported/Not Enforced 诊断。

不再让 `DorisColumnItem` 只生成 Cast 表达式；它应区分：

```text
SELECT 投影
物理列 DDL
View 字段列表
```

### 6.2 Materialization 层

Table、Incremental、Seed 和 Snapshot 分别接入：

- Contract 校验；
- 统一显式列 DDL；
- 失败回滚或中间 Relation 清理；
- Persist Docs；
- Grants。

View 只校验列集合和类型兼容，不声明 Doris 无法强制的物理约束。

### 6.3 Test 存储层

验证 dbt Core 默认 Test Materialization 在 Doris 上生成的 Audit Relation；
仅在存在 Doris 差异时增加 `doris__` Dispatch，避免复制 dbt Core 全部逻辑。

## 7. 测试计划

### 7.1 无集群单元测试

- 类型别名和参数规范化；
- 列名、顺序、缺失和多余字段；
- Constraint 支持矩阵；
- Unsupported Constraint 的错误信息；
- 未启用 Contract 时 Model SQL 原样保留；
- Store Failures 建表 SQL 一条 Statement 一条 SQL。

### 7.2 真实 Doris Functional

- Generic/Singular Test 的 Pass、Warn、Fail；
- Store Failures 首次、重复和失败为零；
- Unit Test 类型、大小写和无效输入；
- Table/View/Incremental Contract；
- `NOT NULL` 写入 NULL 时由 Doris 拒绝；
- Contract 失败后旧 Relation 不变；
- Quote Column、复杂类型和 Decimal 精度；
- Contract 与 Key/Partition/Distribution 组合。

### 7.3 官方 Adapter 套件

接入：

- `constraints`；
- `store_test_failures_tests`；
- `unit_testing`；
- Basic Generic/Singular Tests。

每个跳过项必须写明 Doris 平台不支持、Adapter 尚未实现或测试前置条件不满足，
不能使用无说明的整类 Skip。

## 8. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| T1 | 扩大 Data Test 正反例 | 常用测试、Severity 和 Artifact 有断言 |
| T2 | 接入 Store Failures | Audit Relation 可重复构建并保存正确异常行 |
| T3 | 接入官方 Unit Test 套件 | 类型、大小写和无效输入通过 |
| T4 | 重构 Contract 类型比较 | 字段和类型错误在建表前失败 |
| T5 | 使用显式列 DDL | `NOT NULL` 可由 Doris 强制 |
| T6 | 发布 Constraint 支持矩阵 | Unsupported 不再静默忽略 |
| T7 | 接入官方 Constraints 套件 | 支持项全部通过 |

## 9. 完成定义

- Data Test 的状态、失败数和失败 Relation 准确；
- Unit Test 覆盖 Doris 主要数据类型和引用方式；
- Contract 能稳定校验列名、顺序和类型；
- 支持的 Constraint 确实由 Doris 执行；
- 不支持的 Constraint 有明确诊断；
- 所有失败路径不会留下错误目标或覆盖原 Relation。
