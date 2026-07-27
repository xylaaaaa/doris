# dbt-doris 基础功能实施方案：Grants 与治理

> 状态：实施方案。本文的“目标”和“建议”不代表当前已经支持，当前能力以第 2 节为准。

## 1. 目标与安全边界

Grants 的目标是让 dbt 在成功构建 Relation 后，把声明的访问权限同步到 Doris。
它不是让 dbt 接管 Doris 的账号系统。

第一原则：

- dbt-doris 只给已经存在的 User 或 Role 授权；
- 不创建、修改或删除 User/Role；
- 不管理密码、认证方式和登录锁定；
- 只管理当前 Model 明确声明的 Relation 级权限；
- 所有 Principal、Privilege 和 Relation 都必须结构化渲染；
- 不允许把任意 SQL 片段塞进 `grants`；
- 执行 dbt 的账号必须由管理员显式授予相应授权能力。

Apache Doris 的威胁模型把受限 SQL 用户视为 RBAC 边界内的不可信主体。
虽然 `extension/dbt-doris` 本身不在 Doris 核心安全组件范围内，它生成的 DCL 会改变
真实 Doris 权限，因此实现和测试必须防止越权授权、错误撤权和标识符注入。

## 2. 当前实现

Table 和 View Materialization 已经调用 dbt Core 的 `apply_grants`，Seed 和
Snapshot 也会经过 dbt Core 的 Grants 流程。

但是 dbt-doris 没有 Doris 专用 Grants 宏，会回落到默认 SQL：

```sql
show grants on <relation>;
grant <privilege> on <relation> to a, b, c;
```

这与 Doris 不兼容：

- Doris 的授权查询接口不同；
- Doris User 和 Role 的语法不同；
- 一条 DCL 不应混合多个未知类型的 Principal；
- dbt 默认会把多条 DCL 拼进一个 Statement，再次触发多结果集问题。

因此目前“不配置 Grants 可以运行”，但“配置 Grants”不能算受支持。

## 3. 目标用户接口

保留 dbt 标准 `grants` 结构，并用 Doris 专用前缀明确 Principal 类型：

```yaml
models:
  - name: fct_orders
    config:
      grants:
        select:
          - "role:analytics_reader"
          - "user:report_user@%"
```

规则：

| 写法 | 含义 |
| --- | --- |
| `role:<name>` | 已存在的 Doris Role |
| `user:<name>@<host>` | 已存在的 Doris User 和 Host |
| 无前缀 | 编译失败，不猜测 User 还是 Role |

必须显式写 Host。`user:report_user` 不能自动补成 `@'%'`，避免把本来只允许本机
登录的账号扩大成任意来源。

渲染后的示意 SQL：

```sql
GRANT SELECT_PRIV
ON internal.analytics.fct_orders
TO ROLE 'analytics_reader';

GRANT SELECT_PRIV
ON internal.analytics.fct_orders
TO 'report_user'@'%';
```

Principal 名和 Host 分别校验、分别 Quote，不能把整段字符串原样拼入 SQL。

## 4. Privilege 映射

第一版只公开经过 Doris 权限检查和端到端测试的映射：

| dbt 配置名 | Doris Privilege | 范围 |
| --- | --- | --- |
| `select` | `SELECT_PRIV` | Table / View |
| `insert` | `LOAD_PRIV` | 经过验证的可写 Internal Table |

以下内容不能直接按英文单词机械映射：

- `create` 通常属于 Catalog/Database 范围，不是已存在 Relation 的权限；
- `drop`、`alter` 会扩大变更能力；
- `all` 可能包含超出 Model 消费所需的权限；
- External Catalog 的权限层级和 Internal Catalog 不一定相同。

第一版遇到未列入支持矩阵的 Privilege 应编译失败。后续每增加一项，都要补：

- Doris SQL 语义；
- Internal/External Catalog 边界；
- User 和 Role 测试；
- Grant、Revoke 和重复执行。

## 5. 当前权限读取

dbt 需要知道 Relation 当前有哪些权限，才能计算：

```text
需要新增的 Grant
需要撤销的 Grant
保持不变的 Grant
```

建议从 `information_schema.table_privileges` 精确读取当前 Relation：

```sql
select
    privilege_type as privilege,
    grantee
from information_schema.table_privileges
where table_catalog = ?
  and table_schema = ?
  and table_name = ?;
```

实现前必须用真实 Doris 确认：

- User 和 Role 在 `GRANTEE` 中的表示；
- Host 是否保留；
- Privilege 名称；
- 当前执行者是否有权限看到完整结果；
- Internal 与 External Catalog 行为；
- View 是否出现在相同接口。

如果 Doris 不能通过该表稳定区分 User 与 Role，就不能用猜测结果自动 Revoke。
此时第一版应采用“只增量 Grant、不自动 Revoke”，直到有可靠的当前状态接口。

## 6. Grant 与 Revoke 语义

### 6.1 普通配置

```yaml
grants:
  select: ["role:analytics_reader"]
```

目标是让 Relation 的 dbt 管理权限与配置一致。只有在“当前权限读取可靠且能区分
Principal 类型”后，才允许自动撤销配置中已删除的授权。

### 6.2 Additive Grants

dbt 支持只增加而不撤销的配置方式时，dbt-doris 应保留其语义。该方式适用于：

- Relation 还有非 dbt 管理的手工授权；
- 多个系统共同管理权限；
- 当前执行账号看不到完整授权集合。

文档必须明确普通覆盖模式与 Additive 模式的差异。

### 6.3 Relation 重建

Table Full Refresh、Incremental Full Refresh 和 Snapshot 原子替换可能创建新物理对象。
每次交换完成后都要重新确认 Grants：

```text
新对象构建完成
      |
原子交换
      |
读取当前权限
      |
应用 Grant/Revoke 差异
```

如果授权失败：

- dbt 任务必须失败；
- 错误信息包含 Relation、Privilege 和 Principal 类型；
- 不记录密码或其他凭据；
- 不自动把权限扩大到 Database/Catalog 范围。

## 7. 具体代码改造

新增 Doris Dispatch：

```text
doris__get_show_grant_sql
doris__get_grant_sql
doris__get_revoke_sql
doris__support_multiple_grantees_per_dcl_statement
doris__call_dcl_statements
```

行为：

- `get_show_grant_sql`：只查当前 Relation；
- `get_grant_sql`：解析一个 Principal 并生成一条 Grant；
- `get_revoke_sql`：解析一个 Principal 并生成一条 Revoke；
- `support_multiple_grantees...`：返回 `false`；
- `call_dcl_statements`：每条 DCL 使用独立 dbt Statement 执行。

再增加共用校验：

```text
doris__parse_grantee
doris__map_dbt_privilege
doris__quote_user_and_host
doris__quote_role
```

严禁用分号连接多条 Grant/Revoke。

## 8. 治理范围

### 8.1 本阶段纳入

- Model、Seed、Snapshot 和 Incremental 的 Relation Grants；
- User/Role 显式映射；
- Relation 级 Select 和经过验证的写入权限；
- Manifest 中的 Grants 配置；
- Audit Log 中可追踪的 DCL；
- 权限变更的成功、失败和重复执行。

### 8.2 本阶段不纳入

- 创建和删除 User/Role；
- 密码和认证后端；
- Database、Catalog、Workload Group、Compute Group 和 Storage Vault 授权；
- Row Policy、Column Mask；
- External Catalog 跨系统权限同步；
- 自动推断“谁应该有权限”。

这些是平台治理能力，不应伪装成基础 Relation Grants。

## 9. 测试计划

### 9.1 单元测试

- User/Role 前缀解析；
- 缺 Host、空名称和非法字符；
- Privilege 白名单；
- Identifier 和 Literal Quote；
- 每个 Principal 一条 DCL；
- Grant 与 Revoke SQL；
- Unsupported Privilege；
- 不包含密码和连接信息的错误输出。

必须包含恶意输入：

```text
role:r'; DROP TABLE important; --
user:u@%'; GRANT ADMIN_PRIV ON *.*.* TO ...
```

验收结果是编译失败且不执行任何 DCL。

### 9.2 真实 Doris Functional

使用独立测试 User 和 Role：

- Table、View、Seed、Snapshot、Incremental；
- 首次 Grant；
- 重复运行不重复变更；
- 新增 Principal；
- 删除 Principal 并 Revoke；
- Additive 模式不撤销手工权限；
- Full Refresh 后权限仍正确；
- 不存在的 User/Role；
- 执行账号没有 Grant 权限；
- Select 成功、未授权访问失败；
- 一个 DCL 失败后后续状态可检查、可重跑。

### 9.3 官方 Adapter 测试

接入 `dbt-tests-adapter`：

- Model Grants；
- Seed Grants；
- Snapshot Grants；
- Incremental Grants；
- Invalid Grants。

测试夹具创建的 User/Role 必须只存在于隔离测试 Database，结束后显式清理。

## 10. 分阶段任务

| 阶段 | 任务 | 完成标准 |
| --- | --- | --- |
| G1 | 定义 Principal 和 Privilege 公开格式 | 无歧义、可安全解析 |
| G2 | 实现 Relation 当前权限读取 | User/Role 和 Host 可准确还原 |
| G3 | 实现逐条 Grant | Table/View Select 端到端通过 |
| G4 | 实现安全 Revoke 和 Additive 模式 | 不撤销非 dbt 管理权限 |
| G5 | 覆盖五类资源生命周期 | 重建和增量后权限稳定 |
| G6 | 接入官方 Grants 套件 | 支持项全部通过 |

## 11. 完成定义

- 所有 Principal 类型必须显式；
- 只支持白名单 Privilege；
- Grant/Revoke 一条 Statement 一条 DCL；
- 普通与 Additive 模式有清晰语义；
- Relation 重建后权限仍正确；
- 非法配置在执行 DCL 前失败；
- Adapter 不创建账号、不扩大作用域、不泄露凭据。
