# Apache Doris 对 Databricks Iceberg 的支持完善调研

> 状态：调研结论，不是实现方案或最终接口承诺
>
> 调研更新：2026-08-14
>
> Doris 代码复核基线：`2e8fd03e8c0b19a65fab7fd94f7c0318afc28995`
>
> 范围：只讨论 Databricks Unity Catalog 中的 Iceberg 外部访问及 Doris 现有 Iceberg 能力；native Delta Lake Catalog 见[独立调研](databricks-native-delta-lake-catalog-research.md)。

## 本文回答的三个问题

| 问题 | 直接回答 | 展开位置 |
| --- | --- | --- |
| 为什么 Doris 教程需要 Databricks External Location？ | 教程用 External Location 承载 customer-managed managed storage，避开当前不支持外部 FileIO/vending 的 default storage；它不是把表变成 External Table。 | 第 3.1 节 |
| 已有 Databricks 内部/managed 表能否不复制数据而直接访问？ | 可以，但必须按表名经过 Unity Catalog Iceberg REST；目标表要具备 external-engine capability，底层 storage 要能 vending，Doris 还要支持返回的云凭证。不能绕过 UC 猜测对象存储 URI。 | 第 3.2、3.3、4 节 |
| Doris 已有 Iceberg 支持还要完善什么？ | Doris 已采用标准 Iceberg REST 路线；与成熟支持相比，主要差距是三云数据访问、短期凭证生命周期、表类型/治理边界和真实环境认证。 | 第 5、6 节 |

此外，第 5 节先比较 Snowflake、Spark、Starburst Enterprise/Trino 和 ClickHouse OSS/Cloud，建立商业版能力、开源基线和版本边界；第 6 节再据此评估 Doris 当前能力与差距。

## 1. 结论摘要

1. **已有 Databricks catalog 中符合外部访问条件的 managed Iceberg 表，可以由 Doris 按原表名直接访问，不要求为 Doris 再创建一份 catalog、external table 或复制数据。** “直接”是 Doris 通过 Unity Catalog 的 Iceberg REST API 发现表并取得临时凭证，然后直读对象存储；不是绕过 UC 读取 `__unitystorage` URI。

2. **Doris 教程中的 External Location 不等于 External Table。** 教程把 External Location 选作新 catalog 的 managed storage root，未指定 `LOCATION` 创建的 Iceberg 表仍是 managed table。这样做的关键目的是把 managed storage 放到支持外部 FileIO 和 credential vending 的 customer-managed storage，而不是要求 Doris 只能访问 external table。

3. **是否需要新建 External Location，取决于已有表的存储条件，不取决于它是不是 managed table。** 已有 catalog/schema 若继承了可 vending 的 customer-managed storage，通常不需要新建；如果表位于 Databricks default storage，当前不支持外部 Iceberg/Delta FileIO 和 credential vending，则不能靠给 Doris 配一个长期存储密钥绕过限制。

4. **Doris 已有正确的 Iceberg REST 主干，但尚不能把“Databricks 三云 managed Iceberg”整体标为 production-ready。** 当前具备 REST、OAuth、access delegation 和 native Iceberg scan/write 基础；AWS、Azure、GCP、长查询凭证和 Databricks 特有能力边界尚未完成系统认证。

5. **欧洲 Azure 现场不是“未新建 External Location”导致失败的优先解释。** 现场已成功列库/列表，`loadTable` 又返回 `abfss://.../__unitystorage/...` metadata 和 table-scoped ADLS SAS，证明 UC 控制面、表解析和 credential vending 已经走通。现有证据更指向 Doris 消费 Azure SAS 的数据访问链路；由于缺少原始 `SELECT` 错误和 FE/BE 日志，尚不能写成最终根因。

6. **竞品的成熟方案都把 Unity Catalog 当控制面，而不是要求用户复制 managed table。** Snowflake 的商业实现最完整，已经通过 catalog-linked database、vended credentials 和 GA write 支持形成双向集成；Starburst Enterprise 481-e 在开源 Trino 的 REST/vending 基础上增加 external write 和 Databricks server-side scan planning；Spark 是 Databricks 官方开源参考客户端；ClickHouse 的 UC 集成仍处于 Beta/Experimental，当前 UC 指南的可靠范围主要是 external-storage tables。

## 2. 先把五个概念分开

| 概念 | 是什么 | 不是什么 |
| --- | --- | --- |
| Unity Catalog | Databricks 的表目录、权限、治理和临时凭证控制面 | 表格式或对象存储 |
| Managed table | UC 管理表的位置和生命周期 | “数据一定存放在 Databricks 计算节点” |
| External table | UC 登记用户管理的数据路径 | 使用了 External Location 的所有表 |
| External Location | UC 中“云存储路径 + Storage Credential + 权限”的治理对象 | External Table 的同义词 |
| Iceberg REST Catalog | 外部 Iceberg client 按表名获取 metadata、capability 和临时凭证的开放协议 | 原始对象存储路径访问 |

在 customer-managed storage 场景中，managed table 的数据仍可位于客户自己的 S3、ADLS 或 GCS，只是位置和生命周期由 UC 管理。Databricks 新的 default storage 是 fully managed storage，外部 FileIO 能力不同，不能把两种 managed storage 混为一谈。

## 3. 直接回答：为什么需要 External Location，Databricks 内部表能否直接访问

**问题一：为什么需要一个 Databricks External Location？**

External Location 不是 Doris 协议层的强制要求，也不是为了把表创建成 external table。Doris 教程需要它，是因为教程要主动准备一块**确定支持外部 FileIO 和 credential vending 的 customer-managed storage**，再把它设为 Databricks catalog 的 managed storage。这样创建出来的表仍是 managed Iceberg，但 UC 可以在 Doris 按表名访问时，为其签发限定表路径和有效期的临时云凭证。

**问题二：Databricks 中已有的大量内部/managed 表，有方案让 Doris 不复制数据而直接访问吗？**

有。只要已有 managed Iceberg 表具备 external-engine capability，并且实际位于支持外部 FileIO/vending 的存储中，Doris 就可以直接连接**已有 Unity Catalog 和原表名**，通过 Iceberg REST 取得 metadata 和临时凭证，再读取原来的 S3、ADLS 或 GCS 文件。此时不需要新建 External Location、不需要把表改成 external table，也不需要复制数据。

但是，“managed table”只表示 UC 管理表的位置和生命周期，不能单独证明它能被外部客户端访问。Databricks managed table 还要按底层存储分成两类：

| 已有 managed table 的实际存储 | Doris 能否原地直接访问 | 是否需要另建 External Location |
| --- | --- | --- |
| 支持外部 FileIO/vending 的 customer-managed storage | 可以；使用现有 UC catalog、原表名和 UC 签发的短期凭证 | 不需要 |
| 当前不支持外部 FileIO/vending 的 Databricks default storage | 不能通过 Iceberg REST + 外部 FileIO 原地读取 | 单独新建 External Location 不会改变已有表的位置；如必须由 Doris 直读，需要把兼容数据迁移/复制到 customer-managed storage，或改用 JDBC/ODBC、受限 OpenSharing 等非直读方案 |
| 存储类型或 capability 不明确 | 不能只看 Catalog Explorer 中的 `MANAGED` 判断 | 先检查 table capability，并用一次真实 `loadTable` 验证是否返回可用的临时凭证 |

因此最短结论是：

> External Location 是官方教程用来构造“可向外部引擎签发凭证的 managed storage”的手段，不是 Doris 访问所有 managed table 的前置条件。已有表能否直接访问，取决于 external-engine capability、底层存储是否支持 FileIO/vending，以及 Doris 能否消费返回的云凭证。

### 3.1 Doris 教程为什么创建 External Location

Doris 当前的 [Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/) 做了三件关键操作：

1. 在 Databricks 创建 External Location；
2. 创建 catalog 时取消 `Use default storage`，把该 External Location 选为 managed storage；
3. 创建没有显式 `LOCATION` 的 `USING iceberg` 表。

按照 Databricks 的 [managed storage location 规则](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)，catalog/schema 的 managed storage root 位于一个 External Location 内，实际表数据进入 UC 生成的 `__unitystorage/...` 隔离目录。没有指定 `LOCATION`，所以表仍然是 managed Iceberg：

```text
External Location（管理员侧路径与凭证治理）
    └── catalog/schema managed storage root
        └── __unitystorage/.../managed Iceberg table
            └── Iceberg REST 按表名返回 metadata + 短期凭证
```

它不是：

```text
External Location -> External Iceberg Table -> Doris 长期持有 AK/SK 读路径
```

教程选择 customer-managed storage 的现实原因是：[default storage](https://docs.databricks.com/aws/en/storage/default-storage) 当前不支持外部 Iceberg/Delta client 访问底层 metadata、manifest 和 data files，也不支持 Unity REST/Iceberg REST credential vending。

### 3.2 已有 catalog 能否不新建 External Location

| 目标表场景 | 是否要为 Doris 新建 External Location | 正确访问方式 |
| --- | --- | --- |
| Managed Iceberg 已位于支持 vending 的 customer-managed catalog/schema storage | **不需要** | 使用现有 UC catalog 名，Iceberg REST + OAuth/PAT + vended credentials |
| 表位于当前不支持外部 FileIO/vending 的 default storage | 不能原地靠 Doris 凭证解决；要外部 FileIO，需把兼容数据放到符合条件的 customer-managed storage | 迁移后仍按表名走 Iceberg REST；不应让 Doris 猜 managed 路径 |
| External/unmanaged table，且用户明确选择 path 模式 | 可能需要 External Location/path credential 或 Doris 自有存储权限 | 显式 path 模式，不冒充 UC managed-table 访问 |

第一种场景最终应通过目标表 capability 和一次真实 `loadTable` 验证。只看 catalog UI 无法证明底层存储可 vending。

读取已有 managed table 通常需要 metastore 开启 external data access，并授予 `EXTERNAL USE SCHEMA`、`USE CATALOG`、`USE SCHEMA` 和 `SELECT`。`EXTERNAL USE LOCATION` 主要用于 external/path 操作，不是读取 managed table 的普通前置权限。

### 3.3 “按表名直接访问”究竟是什么

正确链路是：

```text
                         控制面
Doris ── catalog.schema.table ──> Unity Catalog Iceberg REST
  ▲                                      │
  │                 metadata、capability、短期 SAS/STS/OAuth
  └──────────────────────────────────────┘
  │
  │                      数据面
  └──────────────────────────────> ADLS / S3 / GCS
                读取 Iceberg metadata、manifest、Parquet
```

因此“直接”的含义是：不复制数据、不把查询交给 Databricks SQL Warehouse 执行、不重建 external table，并在 UC 授权后由 Doris 直读数据文件。它不意味着拿到 `abfss://.../__unitystorage/...` 或 `s3://...` 后绕开 UC。raw URI 会绕过权限、审计、table capability、行列策略和生命周期管理，也不能作为 managed 模式失败后的静默降级。

## 4. Databricks Iceberg REST 的协议与支持边界

Databricks Iceberg REST endpoint 为：

```text
https://<workspace-host>/api/2.1/unity-catalog/iceberg-rest
```

客户端发送 `X-Iceberg-Access-Delegation: vended-credentials` 后，`loadTable` 可返回 metadata location、表能力以及短期云凭证。官方示例的凭证默认有效期约一小时：

- AWS：STS access key、secret key、session token；
- Azure：ADLS SAS；
- GCP：短期 OAuth token。

短查询成功不能证明实现完整。排队、长查询、retry 或写入跨越 TTL 时，需要携带 expiration 并刷新凭证。

### 4.1 第一步：识别 Unity Catalog 暴露的对象

这一层只判断表以什么形式通过 Iceberg REST 暴露，不讨论它存在哪里。

| UC 中的对象 | 它实际是什么 | 公开支持边界 |
| --- | --- | --- |
| Managed Iceberg | UC 原生管理位置、metadata 和生命周期的 Iceberg 表 | Iceberg REST 支持读写；具体操作以服务端 capability 为准 |
| Foreign Iceberg | UC 从外部 Iceberg catalog 登记的表，生命周期不由 UC 管理 | 只读；credential vending 的官方表述存在差异，需要真实环境确认 |
| Delta table with Iceberg reads | 本质仍是 Delta 表，但 Databricks 为外部 Iceberg client 异步生成兼容 metadata | 只读；不等于 native Delta 支持，也不能通过 Iceberg 路径写 Delta |

### 4.2 第二步：检查这张表能否由 Doris 直接读写

知道对象形式后，还必须分别检查存储、临时凭证和治理策略。下面这些是访问条件或阻断因素，不是新的表类型。

| 检查项 | 观察到的情况 | 对外部客户端的影响 |
| --- | --- | --- |
| 底层存储 | customer-managed storage 支持外部 FileIO/vending | UC 可以签发临时凭证；客户端还必须支持对应的 AWS、Azure 或 GCP 凭证 |
| 底层存储 | Databricks default storage | 当前不能通过 Iceberg REST + FileIO 直读；可选 JDBC/ODBC、受限 OpenSharing 或迁移副本 |
| 服务端 capability | 不支持当前 read/write/DDL 操作 | 该操作不可用；不能把通用 Iceberg 能力直接套用到 Databricks |
| 临时凭证 | UC 未返回凭证，或客户端不认识返回的云凭证 | metadata 可能可见，但底层文件无法读取 |
| 行列策略 | 表带有 UC row filter/column mask | 普通文件扫描无法执行策略，需要 cross-engine ABAC/server-side planning |

因此 Doris 的判断顺序应该是：

```text
识别 managed / foreign / Delta with Iceberg reads
        ↓
检查底层 storage 是否支持外部 FileIO/vending
        ↓
检查服务端 capability 和 UC 治理策略
        ↓
检查客户端能否消费返回的云凭证
        ↓
决定允许读、允许写，还是给出明确拒绝
```

Databricks managed Iceberg 也不是通用 Iceberg 能力的无条件超集。截至本调研日期，官方限制涉及文件格式、delete 表达、branches/tags、partition transform、部分类型和 UC 控制的 table properties。因此不能因为客户端支持通用 Iceberg，就推导出它在 Databricks 上支持全部语法。

## 5. 竞品：只看 Databricks Iceberg

本节只把各产品通过 Iceberg REST 访问 Databricks 的能力计入比较。Delta Sharing、Delta Direct、native Delta connector 等不在本篇的 Iceberg 支持结论内。

### 5.1 支持矩阵与版本边界

| 产品/版本 | Catalog 与身份入口 | 表与存储范围 | 凭证/FileIO | 写入与治理 | 成熟度和商业边界 |
| --- | --- | --- | --- | --- | --- |
| Snowflake 商业 SaaS | Unity Iceberg REST catalog integration；OAuth 或 bearer token；catalog-linked database 自动发现 UC namespace/table | AWS、Azure、GCP 上符合条件的 UC Iceberg；按已有 UC catalog 接入 | `ACCESS_DELEGATION_MODE=VENDED_CREDENTIALS` 时由 UC 下发临时凭证，不需要 Snowflake external volume；也可选择 external volume | catalog-linked database 默认可读写；支持 INSERT/UPDATE/CREATE 等受支持操作 | externally managed Iceberg writes 与 catalog-linked database 于 2025-10-17 GA；本次比较中产品化最完整 |
| Apache Spark + Apache Iceberg | `SparkCatalog` 直连 `/api/2.1/unity-catalog/iceberg-rest`；PAT/OAuth | Managed Iceberg R/W、Foreign Iceberg R、Delta with Iceberg reads R，取决于 UC capability | 必须加载 Iceberg runtime 和 AWS/Azure/GCP 对应 cloud bundle；支持 REST vending | 写操作由 Iceberg client 按 REST capability 执行 | 开源参考实现，不是裸 Spark core 内置 UC；是 Doris 三云正确性基线 |
| Starburst Enterprise 481-e STS | SEP Iceberg connector + UC Iceberg REST + OAuth | 按 SEP 文档分类支持 external R/W、managed R，覆盖 AWS/Azure/GCP | 开启 `iceberg.rest-catalog.vended-credentials-enabled`；worker 读取对象存储 | 新增 external write；可自动采用 Databricks server-side scan planning 执行 row filter/column mask | 481-e 是商业 STS；external write 与 server-side planning 是该版本新增能力 |
| Trino OSS 483 | 通用 Iceberg REST + OAuth | UC 官方配置要求 `iceberg.security=read_only` | 通用 REST 支持 vended credentials；481 起支持 AWS/GCS/Azure 可刷新凭证 | UC 路径只读；没有 SEP 的 Databricks server-side planning 产品能力 | 是 Starburst Enterprise 的开源基线，不能把 SEP 商业能力反推给 Trino/Doris plugin |
| ClickHouse OSS / ClickHouse Cloud | 开源 `DataLakeCatalog`；Cloud 25.8 起将 Glue/Unity 集成作为 Beta | 当前 UC 指南只承诺使用 external storage locations 的 Delta/Iceberg；不承诺 managed storage | 依赖 UC vending 后直读文件；指南认为其 managed-storage 路径拿不到所需凭证 | 支持矩阵把 UC Read/Create/INSERT 标为 Beta，但 UC 指南只完整展示 external read，写支持需 PoC | OSS 提供 catalog/format engine；Cloud 增加 Shared Catalog、distributed cache、parallel execution 等托管能力，但不改变 UC managed-table 契约 |
| Doris 当前 | 标准 Iceberg REST + OAuth/access delegation | 已有通用 Iceberg scan/write；Databricks managed/foreign/Delta-Iceberg-reads 尚未完成认证 | AWS 有基础；Azure 在 UC 返回 SAS 后仍读取失败；GCP 和长查询凭证尚未完成认证 | 通用 Iceberg 有写基础，但没有完整的 Databricks 读写/治理支持矩阵 | 处于部分链路可用、三云尚未系统认证的阶段 |

### 5.2 Snowflake：商业实现的完整形态

Snowflake 使用 [Unity Catalog REST catalog integration](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity) 对接已有 Databricks catalog。核心配置是：

```text
CATALOG_SOURCE = ICEBERG_REST
CATALOG_URI = <workspace>/api/2.1/unity-catalog/iceberg-rest
CATALOG_NAME = <existing-uc-catalog>
ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
```

然后通过 catalog-linked database 自动同步 UC schema/table。它没有要求把已有 managed Iceberg 复制成 Snowflake 表，也没有要求客户为 Snowflake 创建一张 external table。vended-credentials 模式要求 DBX metastore 开启 external data access，并给 Snowflake service principal 授予 `EXTERNAL USE SCHEMA`、`SELECT`、`USE CATALOG`、`USE SCHEMA`；UC 负责发放临时存储凭证。如果不用 vending，Snowflake 也支持 external volume，但那是另一种由消费端管理存储权限的部署模式。

Snowflake 文档明确写出 catalog-linked database 默认支持读写；externally managed Iceberg writes 与 catalog-linked database 已于 [2025-10-17 GA](https://docs.snowflake.com/en/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga)。这说明商业级 Databricks Iceberg 集成的完整形态是：

```text
自动 catalog 同步 + OAuth/PAT + vended credentials
+ 原生 Iceberg read/write + 权限预检 + 私网/运维能力
```

对 Doris 最直接的启示不是照搬 Snowflake 对象模型，而是：标准 Iceberg REST 已足以访问已有 UC managed Iceberg；消费端应把 credential lifecycle、catalog 自动同步、诊断和远端/本地 RBAC 边界做成产品能力。

### 5.3 Spark：Databricks 官方开源参考链路

Databricks 的 [Iceberg client 文档](https://docs.databricks.com/aws/en/external-access/iceberg) 给出了 Spark 的官方配置。实际组合并不是“Spark core 自动懂 Unity Catalog”，而是：

```text
Apache Spark
  + Iceberg Spark runtime
  + iceberg-aws/azure/gcp bundle
  + SparkCatalog
  + Databricks Iceberg REST endpoint
```

cloud-specific bundle 是重要信号：catalog 层能成功 `loadTable`，不代表数据层一定能消费 S3 STS、Azure SAS 或 GCP OAuth。Doris 当前 Azure 案例正好卡在这一层，因此 Spark 可作为判断 catalog、凭证还是文件读取问题的参考客户端。

### 5.4 Starburst Enterprise 与 Trino OSS：商业增量在哪里

[Starburst Enterprise 481-e STS](https://docs.starburst.io/latest/connector/starburst-iceberg-unity.html) 和 [481-e release notes](https://docs.starburst.io/latest/release/release-481-e.html) 明确增加了两项 Databricks Iceberg 能力：

- 使用 Unity Catalog 时写 external tables；
- Databricks server-side scan planning。

server-side planning 由 UC 决定要读取哪些文件，是执行 row filter/column mask 的必要路径。SEP 会在 catalog advertises capability 时自动选择它，并暴露 `serverSideScanCount`/`clientSideScanCount` 指标。代价是更高的 split planning latency，并限制 statistics/join reorder、time travel 和 Iceberg metadata tables。

这不是 upstream Trino 的无条件能力。[Trino 483 metastore 文档](https://trino.io/docs/current/object-storage/metastores.html) 虽然支持标准 REST、OAuth、vended credentials，并且 Trino 481 已增加 AWS/GCS/Azure refreshable vending，但连接 Databricks UC 时仍要求 `iceberg.security=read_only`。因此 Starburst Enterprise 的价值主要是 provider-aware 支持、商业版本认证、server-side planning 和新增的 external write，不只是把开源 connector 换一个名字。

版本也必须写清：当前 latest STS 是 481-e，而 latest LTS 是 480-e.7。不能把 481-e STS 新能力直接当成所有 LTS 部署已具备的契约；选型时要按客户实际 SEP 版本核对。

### 5.5 ClickHouse OSS 与 ClickHouse Cloud：能力宣传与指南边界

ClickHouse 的 `DataLakeCatalog` 是开源 catalog engine；ClickHouse Cloud 在 25.8 将 Glue/Unity integration 作为 Beta，并增加 Shared Catalog、distributed cache、userspace page cache 和 parallel execution 等托管能力。它可以根据 catalog metadata 自动选择 Iceberg/Delta table engine，这一点值得 Doris 借鉴：catalog discovery 与 format execution 应解耦。

但当前 [Unity Catalog 指南](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog) 明确把范围限定为使用 external storage locations 的 Delta/Iceberg 表，并把功能标为 experimental；managed Databricks storage 不在其承诺范围。与此同时，[ClickHouse support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix) 又把 Unity Read/Create/INSERT 标为 Beta。两份官方资料没有按 Iceberg/Delta、external/managed 拆清写能力，不能据此宣传“UC managed Iceberg 已支持完整写入”。保守产品结论应是：external-storage catalog read 已有公开路径；create/insert 和 managed table 必须以具体版本、云和 PoC 结果为准。

这也说明 Cloud 的商业增量主要在托管、弹性和缓存执行层；当前没有证据表明 Cloud 另有一套绕开 UC storage/vending 限制的 managed-Iceberg 协议。

### 5.6 对 Doris 的共同启示

1. **不复制数据。** Snowflake、Spark、Starburst 和 ClickHouse 都先连接已有 catalog；没有成熟方案要求为消费端把 managed table 改成 external table。
2. **catalog control plane 与 data plane 分离。** 表名、权限、capability 和临时凭证来自 UC；文件由各产品自己的 Iceberg/Parquet 执行层读取。
3. **credential vending 是主路径。** 商业产品把短期、表范围凭证做成自动能力，而不是让用户长期配置 AK/SK。
4. **三云 FileIO 是产品能力，不是 REST 成功的自然结果。** Spark 明确要求 cloud bundle；Trino 也按版本补齐 Azure/GCS vending；Doris 必须分别认证。
5. **治理需要 server-side planning。** 仅有文件凭证不能执行 UC row filter/column mask；Starburst 的实现证明这是独立能力，并伴随性能和功能限制。
6. **商业支持必须带版本和支持矩阵。** Snowflake 已 GA；Starburst STS/LTS 不同；ClickHouse 仍 Beta/Experimental。Doris 不能用“能列表”代替 production-ready 定级。

### 5.7 如何理解“完成度”和“产品化程度”

这里的“完成度”和“产品化程度”不是指是否采用了不同协议，也不是简单比较开源与商业软件。Doris 与竞品使用的主链路相同：按表名访问 Unity Catalog，由 UC 返回 metadata、capability 和临时凭证，再由各自的 Iceberg/FileIO 执行层读取数据。差异在于这条链路覆盖了多少场景，以及能否作为稳定的产品承诺交付给客户。

| 概念 | 本文含义 | 主要观察维度 |
| --- | --- | --- |
| 完成度 | 协议和数据访问链路是否在声明的范围内端到端可用 | AWS/Azure/GCP、短期凭证有效期、managed/foreign/Delta Iceberg reads、读写和治理策略 |
| 产品化程度 | 客户能否安全、稳定地配置、诊断和运维，并获得明确的版本承诺 | 配置与报错、凭证安全、可观测性、私网部署、支持矩阵和兼容认证 |

Snowflake 和 Starburst Enterprise 的领先主要体现在这些能力已经形成版本化支持范围、凭证生命周期、诊断或治理能力，而不是使用了另一套访问架构。ClickHouse 当前仍标为 Beta/Experimental，因此不能只根据“已提供 Unity Catalog 接口”判断其成熟度。

## 6. Doris 当前支持状态

有了第 5 节的竞品基线后，本节只总结 Doris 已经具备的能力、尚未验证完整的范围和客户现场证据，不展开实现设计。

### 6.1 已有基础

当前 Doris 已有：

- 标准 Iceberg REST Catalog；
- OAuth/PAT 认证和 vended-credentials 请求；
- native Iceberg metadata、manifest、Parquet 读取和通用写入基础。

这说明 Doris 的总体协议路线与竞品一致，不需要把现有能力描述成另一种 Databricks 专用 catalog。

### 6.2 尚未完成系统认证的范围

| 范围 | 当前调研结论 |
| --- | --- |
| 云存储 | AWS 已有基础；Azure table-scoped SAS 尚未形成已验证闭环；GCP 未完成系统认证 |
| 短期凭证 | 长查询、排队和重试跨越凭证有效期的行为尚未完成认证 |
| 表与操作 | managed、foreign、Delta Iceberg reads 以及 Databricks 特有 DDL/DML 限制尚未形成正式支持矩阵 |
| 治理 | UC row filter/column mask 需要 server-side planning，普通文件扫描不能等价执行 |
| 测试 | 缺少覆盖三云、表类型、读写和凭证过期场景的完整真实环境结果 |

这里需要修正一个容易误读的说法：**Doris 不是完全不支持 Azure。** Doris 已有 Azure account key/OAuth 等静态存储配置；当前未验证完整的是 Databricks Iceberg REST 返回的 table-scoped `adls.sas-token.*` 数据访问链路。

### 6.3 欧洲 Azure 现场的证据链

现场对已有 UC catalog 开启外部访问后：

```text
SHOW DATABASES / SHOW TABLES 成功
→ REST loadTable 成功
→ 返回 abfss://.../__unitystorage/... metadata
→ config 返回 adls.sas-token.<host> 和 expiration
→ SELECT 读取数据失败
```

这已经证明：

1. REST 鉴权和 namespace/table 控制面可用；
2. 目标是 existing managed Iceberg，不需要为了 Doris 重新建表；
3. UC 已签发限定表路径的 Azure SAS，`storage-credentials: []` 不等于没有凭证；
4. “未新建 External Location”不是优先根因。

结合接口返回与 Doris 现状复核，当前高置信候选原因是：UC 已返回 Azure table-scoped SAS，但 Doris 尚未形成这类凭证从 REST 响应到 Iceberg 文件读取的已验证闭环。这不是最终诊断；仍需要 Doris 版本、完整 `SELECT` 错误、FE/BE 日志，以及 Doris 到目标 ADLS endpoint 的网络/防火墙结果才能关闭问题。

### 6.4 中国区输入的证据等级

“2026 年 6 月某中国区 workspace 未部署 Iceberg REST、无 ETA”只能记录为特定现场和时间点的反馈，不能泛化为当前全部中国区能力。Azure 中国官方文档在 2026-07-24 已给出 Iceberg REST endpoint 和中国云专用域名。产品测试应记录 cloud、region、workspace、HTTP 状态和支持工单，按 workspace 探测能力，而不是静态判断整个区域支持或不支持。

## 7. 主要官方资料

### Apache Doris

- [Doris Unity Catalog 最佳实践](https://doris.apache.org/docs/dev/lakehouse/best-practices/doris-unity-catalog/)
- [Doris Iceberg Catalog](https://doris.apache.org/docs/dev/lakehouse/catalogs/iceberg-catalog/)
- [Doris Iceberg REST Catalog](https://doris.apache.org/docs/3.x/lakehouse/metastores/iceberg-rest/)

### Databricks 与 Iceberg

- [External access overview](https://docs.databricks.com/aws/en/external-access)
- [Managed and external tables](https://docs.databricks.com/aws/en/data-governance/unity-catalog/managed-versus-external)
- [Managed storage locations](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/managed-storage)
- [Default storage limitations](https://docs.databricks.com/aws/en/storage/default-storage)
- [Managed Iceberg tables](https://docs.databricks.com/aws/en/iceberg/)
- [External access through Iceberg REST](https://docs.databricks.com/aws/en/external-access/iceberg)
- [Iceberg REST on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/external-access/iceberg)
- [Iceberg REST on Azure China](https://docs.azure.cn/en-us/databricks/external-access/iceberg)
- [Iceberg REST on GCP](https://docs.databricks.com/gcp/en/external-access/iceberg)
- [Enable external access](https://docs.databricks.com/aws/en/external-access/admin)
- [Credential vending](https://docs.databricks.com/aws/en/external-access/credential-vending)
- [Unity Catalog privilege reference](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference)
- [Cross-engine ABAC](https://docs.databricks.com/aws/en/external-access/cross-engine-abac)
- [Delta Iceberg reads](https://docs.databricks.com/aws/en/delta/iceberg-reads)

### 竞品

- [Snowflake bidirectional access to Unity Catalog](https://docs.snowflake.com/en/user-guide/tutorials/tables-iceberg-set-up-bidirectional-access-to-unity-catalog)
- [Snowflake REST catalog integration for Unity Catalog](https://docs.snowflake.com/en/user-guide/tables-iceberg-configure-catalog-integration-rest-unity)
- [Snowflake externally managed Iceberg writes / catalog-linked database GA](https://docs.snowflake.com/en/release-notes/2025/other/2025-10-17-iceberg-external-writes-cld-ga)
- [Apache Iceberg Spark configuration](https://iceberg.apache.org/docs/latest/spark-configuration/)
- [Starburst Enterprise 481-e Iceberg with Unity Catalog](https://docs.starburst.io/latest/connector/starburst-iceberg-unity.html)
- [Starburst Enterprise 481-e release notes](https://docs.starburst.io/latest/release/release-481-e.html)
- [Starburst Enterprise 480-e LTS release notes](https://docs.starburst.io/latest/release/release-480-e.html)
- [Trino 483 metastore and Iceberg REST configuration](https://trino.io/docs/current/object-storage/metastores.html)
- [Trino 481: Azure and refreshable vended credentials](https://trino.io/docs/current/release/release-481.html)
- [ClickHouse Unity Catalog guide](https://clickhouse.com/docs/guides/use-cases/data-warehousing/unity-catalog)
- [ClickHouse open table format and catalog support matrix](https://clickhouse.com/docs/guides/use-cases/data-warehousing/support-matrix)
- [ClickHouse Cloud DataLakeCatalog architecture](https://clickhouse.com/blog/query-your-catalog-clickhouse-cloud)
