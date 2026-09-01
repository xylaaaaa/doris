# 我对 PR #66782 的理解

> 本文先只记录我对自己 Azure PR 的理解。后续 SPI 重构 PR 的内容暂时留空，等前一个 PR 的链路完全理解后再补。

阅读建议：

- 第 1～9 节先建立概念和 PR 修复主线；
- 第 13 节比较修复前后的普通扫描链路；
- 第 14 节是最完整、最权威的代码跟踪，分别覆盖 Databricks REST、FE Iceberg 元数据、BE 普通数据、BE 原生 SharedKey、BE Java 系统表和 CredentialUtils。

## 1. 这次 PR 要解决什么问题

场景是：Doris 通过 Iceberg REST Catalog 访问 Databricks Unity Catalog 中的 Iceberg 表。Databricks 不只返回表的元数据，还会在查询时临时下发一套 Azure SAS 凭证，Doris 需要用这套临时凭证读取 Azure ADLS 中的 parquet、orc 和 Puffin 文件。

大致链路是：

~~~text
Databricks Unity Catalog
  -> Iceberg 表元数据 + 临时 Azure SAS 凭证
  -> Doris FE 解析并生成扫描/写入计划
  -> Doris FE 把路径、reader 类型和凭证交给 BE
  -> Doris BE 通过 Hadoop ABFS 访问 Azure ADLS
~~~

原来的 Doris 能访问 Azure，但主要支持静态 SharedKey；它不能直接处理 Databricks 返回的 Iceberg ADLS 凭证格式。因此一个普通查询可能出现：Catalog 元数据可以读到，但真正读取数据文件失败。

## 2. 先理解三个容易混淆的概念

### 2.1 Azure 是存储厂商

Azure ADLS 是真正保存数据文件的地方，例如：

~~~text
abfss://container@account.dfs.core.windows.net/table/data/file.parquet
~~~

### 2.2 SAS 是凭证类型

SAS 是一份带权限和过期时间的临时访问凭证。它不是文件格式，也不是访问协议。

Databricks 返回的名字属于 Iceberg/Azure 方言，例如：

~~~text
adls.sas-token.account.dfs.core.windows.net = <SAS token>
~~~

### 2.3 Hadoop ABFS 是访问实现

ABFS 是 Hadoop 提供的 Azure 文件系统客户端。Doris BE 通过 Hadoop FileSystem/JNI 调用它。

~~~text
Doris BE
  -> Hadoop FileSystem
  -> Azure ABFS
  -> Azure ADLS
~~~

ABFS 需要的配置是：

~~~text
fs.azure.account.auth.type.account.dfs.core.windows.net = SAS
fs.azure.sas.fixed.token.account.dfs.core.windows.net = <SAS token>
~~~

因此，本 PR 的主要工作是把 Databricks/Iceberg 的凭证格式转换成 Hadoop ABFS 的格式，并把它完整地送到 BE。

## 3. Azure 为什么有两条访问路径

在这个版本的 Doris 中，不同认证方式走不同的 BE 访问链路：

| Azure 认证方式 | Doris 的访问路径 | 含义 |
| --- | --- | --- |
| SharedKey | FILE_S3 | 走 Doris 的对象存储/S3 family 入口，最终可创建 Azure client |
| OAuth2 | FILE_HDFS | 走 Hadoop FileSystem，再由 ABFS 访问 Azure |
| SAS | FILE_HDFS | 本 PR 复用 Hadoop ABFS 访问 Azure |

这里的 FILE_S3 不一定表示真正访问 Amazon S3，而是 Doris BE 的对象存储 reader family。配置中带有 provider=azure 时，BE 可以在这个入口下创建 Azure Blob client。

这里的 FILE_HDFS 也不表示数据放在 HDFS，而是表示：

> 让 BE 走 Hadoop reader；Hadoop 再根据 abfs/abfss scheme 选择 Azure ABFS 实现。

所以这三者要配套：

~~~text
凭证：fs.azure.* SAS 配置
路径：abfss://container@account/...
reader：FILE_HDFS
~~~

## 4. 普通 SELECT 的完整处理过程

### 4.1 Iceberg connector 提取临时凭证

代码入口是 IcebergScanPlanProvider.extractVendedToken()。

它从 Iceberg 表的 FileIO 中读取：

- FileIO 自身的 properties；
- SupportsStorageCredentials 中的 server-vended StorageCredential.config()。

结果是一个普通的 Map，例如：

~~~text
adls.sas-token.account.dfs.core.windows.net = <token>
adls.sas-token-expires-at-ms.account.dfs.core.windows.net = <timestamp>
~~~

这个步骤只负责“从 Iceberg 中拿出凭证”，还没有交给 BE。

### 4.2 CredentialUtils 翻译凭证名称

代码在 fe-core/.../datasource/credentials/CredentialUtils.java。

它把：

~~~text
adls.sas-token.<account-host>
~~~

翻译成：

~~~text
fs.azure.account.auth.type.<account-host> = SAS
fs.azure.sas.fixed.token.<account-host> = <token>
~~~

adls.sas-token-expires-at-ms.* 等 Iceberg 方言属性不会继续作为原始属性传给 Hadoop；真正消费它们的是前面生成的 fs.azure.* 配置。

### 4.3 buildVendedStorageMap() 构造临时存储配置

代码在 DefaultConnectorContext.buildVendedStorageMap()。

可以把它理解成：

> 给这次查询的临时凭证找一个 Doris 内部可以使用的 storage 配置对象。

它的步骤是：

~~~text
原始 token
  -> CredentialUtils 翻译
  -> StorageAdapter.ofAll()
  -> 得到 StorageAdapter 列表
  -> 按 StorageTypeId 放入 Map
~~~

### 4.4 为什么需要把 HDFS key 改成 AZURE key

这是本 PR 中最容易困惑的一段。

翻译后的属性是 fs.azure.*。旧的路由机制看到 Hadoop 形状的 fs.* 配置，会把它绑定成：

~~~text
StorageTypeId.HDFS -> Azure SAS StorageAdapter
~~~

但处理文件路径时，abfss:// 会被识别为：

~~~text
StorageTypeId.AZURE
~~~

于是路径会去 Map 中找 StorageTypeId.AZURE，但实际配置在 HDFS 下面，查找失败。

因此代码做了：

~~~java
result.remove(StorageTypeId.HDFS);
result.put(StorageTypeId.AZURE, adapter);
~~~

这不是把 Hadoop 配置变成了 Azure SDK 配置，也不是说 Azure 是 HDFS。它只是把同一个 adapter 从错误的 Map 索引位置移动到 Azure 位置，让 abfss:// 路径可以找到它。

更直观地说：

~~~text
配置内容：Azure SAS 的 Hadoop 配置
旧路由标签：HDFS
文件路径标签：AZURE
本 PR：修正 Map 中的标签位置
~~~

之所以需要这个绕法，是因为当时 Azure 插件还没有直接认领 adls.sas-token.<动态账户地址> 的能力，动态属性绑定和 vended credential 路由也还不够完善。

## 5. 为什么 abfss:// 不能被转换成 s3://

Doris 原生对象存储 reader 通常使用：

~~~text
s3://bucket/path
~~~

而 Hadoop ABFS 需要：

~~~text
abfss://container@account.dfs.core.windows.net/path
~~~

如果把 Azure SAS 路径转换成 s3://：

- Azure account authority 可能丢失；
- Hadoop ABFS 不知道应该访问哪个账户；
- account-scoped SAS 配置无法正确匹配；
- BE 可能选择错误的 reader。

所以 StorageAdapter.validateAndNormalizeUri() 对 Azure SAS 的 Azure scheme 做了特殊处理，让 abfs:// 和 abfss:// 保持原样。

目标组合是：

~~~text
abfss://...
  + fs.azure.*
  + FILE_HDFS
~~~

## 6. 为什么要把 FILE_HDFS 放进 scan range

一个 ConnectorScanRange 就是一份交给 BE 的文件扫描任务，里面可以包含：

- 文件路径；
- 文件 offset 和 length；
- parquet/orc 格式；
- delete file 信息；
- BE 应该使用的 file type。

原来的默认逻辑主要根据 URI scheme 判断 reader：

~~~text
abfss:// -> 默认可能是 FILE_S3
~~~

但 Azure SAS 必须使用 Hadoop reader，因此本 PR 增加了可选的 backendFileType：

~~~text
IcebergScanPlanProvider
  -> FILE_HDFS
  -> IcebergScanRange
  -> PluginDrivenSplit
  -> BE
~~~

这样 BE 不再只依赖 abfss scheme 猜测，而是直接使用 FE 已经做好的判断。

## 7. 为什么普通 SELECT 之外还要继续修三个地方

普通数据读取跑通后，E2E 又发现其他执行路径没有自动继承同样的 Azure 信息。

### 7.1 UPDATE/MERGE：补 fs.defaultFS

Azure v3 Iceberg 表在执行 DELETE 后，后续 UPDATE 或 MERGE 需要读取旧的 deletion vector。

普通 scan range 自己有文件路径和 file type，但 DML sink 内部的 deletion-vector helper 没有 per-file fs_name，会依赖：

~~~text
fs.defaultFS
~~~

如果没有这个配置，BE 可能退化为：

~~~text
hdfs://
~~~

这个地址没有 Azure account authority，无法打开文件。

因此 IcebergWritePlanProvider.buildHadoopConfig() 从真实数据位置：

~~~text
abfss://container@account.dfs.core.windows.net/warehouse/...
~~~

提取：

~~~text
fs.defaultFS = abfss://container@account.dfs.core.windows.net
~~~

这样 DML sink 读取旧 deletion vector 时也能找到正确的 Azure authority。

### 7.2 $position_deletes：这是另一种 native range

$position_deletes 系统表读取 Puffin deletion vector 时，走的是单独的 range builder。

之前普通数据 range 已经有 FILE_HDFS，但 position-delete range 没有，于是：

~~~text
abfss://.../dv.puffin
  -> 默认 FILE_S3
  -> Invalid S3 URI
~~~

本 PR 给 position-delete range 也补上了 FILE_HDFS。

### 7.3 $files：BE Java metadata scanner 也要有 Azure FileIO

普通数据文件主要由 BE native reader 读取，但 $files 等 Iceberg system table 会经过 BE Java metadata scanner。

FE connector 依赖 iceberg-azure，不代表 BE Java extension 的 classpath 里也有 ADLSFileIO。因此本 PR 给以下模块补了 Azure Iceberg 依赖：

~~~text
fe-connector-iceberg
iceberg-metadata-scanner
preload-extensions
~~~

并增加 Class.forName("org.apache.iceberg.azure.adlsv2.ADLSFileIO") 测试，确认运行时确实能加载 Azure FileIO。

## 8. 五个功能提交之间的关系

这几个提交是沿着真实 E2E 链路逐步暴露问题后追加的：

~~~text
5f54052  基础能力
         提取/翻译 Azure SAS，打包 Iceberg Azure 依赖，避免 BE 日志打印凭证 value

9791c4b  普通扫描
         保留 abfss 路径，并显式传递 FILE_HDFS

1a122e3  行级 DML
         为旧 deletion vector 的读取补 fs.defaultFS

b7ce052  position_deletes
         给 Puffin/native range 补 FILE_HDFS

fef9c42  system table
         让 BE Java metadata scanner 能加载 ADLSFileIO
~~~

所以这个 PR 不是只改了一个配置转换，而是把以下路径都接通：

~~~text
普通 SELECT
普通 Iceberg data file
position delete / deletion vector
UPDATE / MERGE 的旧 DV 读取
Iceberg $files 等 metadata scanner
~~~

## 9. 我目前对自己 PR 的理解

我目前认为，这个 PR 的本质是：

1. Databricks 返回的是 Iceberg/Azure 方言的临时凭证；
2. Doris 当时没有直接支持这种动态 Azure SAS 配置；
3. 因此 FE 先把它翻译成 Hadoop ABFS 配置；
4. 由于旧路由机制按 fs.* 把它归到了 HDFS，需要额外 re-key 到 Azure；
5. 由于 abfss:// 不能丢 authority，路径必须保持原样；
6. 由于 BE 默认可能把 abfss 当成 S3，scan range 必须显式携带 FILE_HDFS；
7. DML 和 system table 是不同执行路径，所以还要分别补 fs.defaultFS、file type 和 Java classpath。

最核心的一致性要求是：

~~~text
Azure SAS 凭证
  + abfss:// 路径
  + Hadoop ABFS / FILE_HDFS reader
~~~

只要其中一项缺失，就可能出现“Catalog 能发现表，但 BE 读不了文件”的问题。

## 10. 后续：SPI 改进 PR

本节暂时留空。

等我完全理解 PR #66782 的实现和问题链路后，再补充 morningman/pr-66782 对 Storage SPI 的改进思路。

## 11. 参考代码位置

以下路径以本地 Azure worktree /mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended 为准：

- fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java
- fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java
- fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanRange.java
- fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/scan/ConnectorScanRange.java
- fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/storage/StorageAdapter.java
- fe/fe-core/src/main/java/org/apache/doris/common/util/LocationPath.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/split/PluginDrivenSplit.java
- be/src/io/hdfs_builder.cpp

## 12. 本文记录范围

- 已记录我们目前讨论过的 Azure、ABFS、SAS、HDFS/S3 reader、vended credentials、re-key、DML 和 position delete 等问题；
- 内容是对自己 PR 的理解笔记，不是对 SPI 改进 PR 的评审结论；
- SPI 改进 PR 部分暂时不展开；
- 本次只整理文档，没有运行新的编译或测试。

## 13. 修复前后的完整链路

这一节专门记录 abfss:// 为什么最终能够使用 Hadoop reader，以及 backendFileType 在其中起什么作用。

### 13.1 修复前

以 Databricks 返回 Azure vended SAS 的场景为例，修复前的链路是：

~~~text
Databricks Unity Catalog
  -> Iceberg 表元数据
  -> adls.sas-token.<account-host>

Iceberg connector
  -> 从 FileIO 提取 token

CredentialUtils
  -> 旧的固定前缀白名单没有正确接收 adls.*
  -> token 被过滤，或没有转换成 Hadoop 配置

DefaultConnectorContext
  -> REST Catalog 的静态 storage map 通常为空
  -> 没有可用的 vended storage 配置

IcebergScanPlanProvider
  -> 生成 abfss:// 文件路径

LocationPath / PluginDrivenSplit
  -> 主要按 scheme 推断 abfss://
  -> 默认得到 FILE_S3

BE
  -> 用 S3 reader 解析 abfss://
  -> 凭证缺失、Invalid S3 URI 或无法读取 Azure 文件
~~~

即使把 token 手工转换成了 fs.azure.*，旧的 storage 路由仍可能把它放到：

~~~text
StorageTypeId.HDFS -> StorageAdapter
~~~

而 abfss:// 路径根据 scheme 会查找：

~~~text
StorageTypeId.AZURE
~~~

于是还会发生“配置在 HDFS，路径找 AZURE”的索引错位。

### 13.2 修复后的普通 SELECT

你的 PR 将普通数据读取链路接成：

~~~text
Databricks Unity Catalog
  -> Iceberg 元数据 + Azure SAS token

IcebergScanPlanProvider
  -> 从 FileIO 提取 token

CredentialUtils
  -> adls.sas-token.<account-host>
  -> fs.azure.account.auth.type.<account-host> = SAS
  -> fs.azure.sas.fixed.token.<account-host> = <token>

DefaultConnectorContext
  -> StorageAdapter.ofAll()
  -> 得到 HDFS adapter
  -> 将同一个 adapter 从 HDFS key re-key 到 AZURE key

StorageAdapter
  -> 识别当前 adapter 承载的是 Azure SAS
  -> 保留 abfss://，不转换成 s3://

LocationPath
  -> 识别 Azure SAS + Azure URI
  -> 返回 FILE_HDFS

IcebergScanRange
  -> 保存 backendFileType = FILE_HDFS

PluginDrivenSplit
  -> 将 FILE_HDFS 写入 split 的 locationType
  -> 覆盖按 abfss scheme 得到的默认 FILE_S3

BE
  -> FILE_HDFS
  -> Hadoop FileSystem / JNI
  -> Hadoop 根据 abfss:// 选择 Azure ABFS
  -> 使用 fs.azure.* SAS 配置访问 Azure ADLS
~~~

这里的最终一致性是：

~~~text
路径：abfss://container@account.dfs.core.windows.net/...
凭证：fs.azure.* = SAS
reader：FILE_HDFS
~~~

### 13.3 backendFileType 是什么

backendFileType 不是文件格式。

~~~text
fileFormat      = parquet/orc
backendFileType = FILE_S3/FILE_HDFS/FILE_BROKER/FILE_LOCAL
~~~

前者回答“文件内容是什么格式”，后者回答“BE 用哪一类文件系统打开它”。

ConnectorScanRange 增加这个可选字段，是因为 URI scheme 不总能决定正确的 reader。Azure SAS 的 abfss:// 路径就是典型例子：scheme 默认可能映射到 FILE_S3，但实际必须使用 Hadoop ABFS，也就是 FILE_HDFS。

### 13.4 PR 里确实使用了特判

当前实现不是全局把所有 abfss:// 都改成 FILE_HDFS，而是针对 Azure SAS 做三层特判：

1. CredentialUtils 特判 adls.sas-token.*，把它转换成 fs.azure.*；
2. StorageAdapter.isAzureSasStorage() 检查 fs.azure.account.auth.type.* = SAS，并让 Azure SAS 的 URI 保持 abfs:// 或 abfss://；
3. LocationPath.getTFileTypeForBE() 在 Azure SAS + Azure URI 时返回 FILE_HDFS，然后通过 ConnectorScanRange.backendFileType 传到 PluginDrivenSplit。

因此不是：

~~~text
所有 abfss:// -> FILE_HDFS
~~~

而是：

~~~text
Azure SAS 场景下的 abfss:// -> FILE_HDFS
~~~

Azure SharedKey 仍然走：

~~~text
AWS_* 风格配置
  -> FILE_S3
  -> Doris 对象存储入口
  -> Azure client
~~~

Azure OAuth2/SAS 则走：

~~~text
fs.azure.* 配置
  -> FILE_HDFS
  -> Hadoop FileSystem
  -> Azure ABFS
~~~

这里的 FILE_S3 不一定意味着 Amazon S3，FILE_HDFS 也不一定意味着真正的 HDFS；它们是 Doris BE 内部两种不同的访问入口。

### 13.5 普通 SELECT 之外的三条补充链路

普通数据读取成功后，E2E 还发现了三条独立路径。

#### UPDATE/MERGE

UPDATE 和 MERGE 需要读取已有 deletion vector。DML sink 没有普通 scan range 的 per-file fs_name，所以需要：

~~~text
dataLocation
  -> abfss://container@account.dfs.core.windows.net/...
  -> fs.defaultFS = abfss://container@account.dfs.core.windows.net
  -> BE DML sink 可以通过 Hadoop ABFS 找到旧 deletion vector
~~~

#### $position_deletes

这是另一种 native range。修复前它没有显式的 FILE_HDFS：

~~~text
abfss://.../dv.puffin
  -> 默认 FILE_S3
  -> Invalid S3 URI
~~~

修复后 position-delete range 也携带 FILE_HDFS，因此可以通过 Hadoop ABFS 读取 Puffin deletion vector。

#### $files 等 system table

这类查询会经过 BE Java metadata scanner。FE connector 有 iceberg-azure 依赖，并不代表 BE Java extension 也能加载 ADLSFileIO。

因此需要同时给以下模块补依赖：

~~~text
fe-connector-iceberg
iceberg-metadata-scanner
preload-extensions
~~~

并用 Class.forName("org.apache.iceberg.azure.adlsv2.ADLSFileIO") 验证运行时 classpath。

## 14. FE、BE 与 Azure 的完整访问链路

本节回答四个具体问题：

1. FE 是谁访问 Databricks 和 Azure，分别使用什么客户端与参数？
2. BE 普通数据扫描使用什么客户端，FE 要发送什么参数？
3. BE Java metadata scanner 是否还会访问 Azure，它如何得到 FileIO 和凭证？
4. CredentialUtils 到底服务 FE、BE 中的哪一部分？

### 14.1 先分清两类凭证

同一次查询会出现两类用途完全不同的凭证。

| 凭证 | 用途 | 使用者 | 参数示例 |
| --- | --- | --- | --- |
| Catalog 控制面凭证 | 登录 Databricks Iceberg REST Catalog | FE 的 Iceberg REST SDK | iceberg.rest.oauth2.credential、iceberg.rest.oauth2.token |
| Azure 数据面凭证 | 读取 Azure ADLS 中的 metadata、manifest、parquet、orc、Puffin | FE ADLSFileIO、BE Hadoop ABFS | adls.sas-token.* 或 fs.azure.* |

Catalog OAuth2 解决的是：

~~~text
FE 能否调用 Databricks REST API？
~~~

Azure SAS 解决的是：

~~~text
FE/BE 能否读取 abfss:// 下的文件？
~~~

这两套凭证不能互相替代。Databricks OAuth token 不能直接拿去读 ADLS；Azure SAS 也不能拿去登录 Catalog。

### 14.2 总览：当前存在五条链路

| 链路 | 发起进程 | 访问目标 | 客户端 | 凭证格式 |
| --- | --- | --- | --- | --- |
| A. Catalog 控制面 | FE | Databricks Unity Catalog REST API | Iceberg RESTSessionCatalog/RESTClient | iceberg.rest.* |
| B. FE 元数据规划 | FE | Azure 上的 metadata/manifest | Iceberg ResolvingFileIO → ADLSFileIO → Azure Java DataLake SDK | adls.* / StorageCredential |
| C. BE 普通 SAS 数据扫描 | BE C++ + JVM Hadoop | Azure 上的 parquet/orc/Puffin | FILE_HDFS → libhdfs/JNI → Hadoop ABFS | fs.azure.* |
| D. BE SharedKey 数据扫描 | BE C++ | Azure Blob | FILE_S3 → Doris object-storage 入口 → Azure C++ SDK | AWS_* + provider=azure |
| E. BE Iceberg 系统表 | BE C++ + Java extension | 序列化任务中的 metadata 行，或 Azure 上的 manifest | IcebergSysTableJniScanner；manifest task 可恢复 ADLSFileIO | 主要随 serialized FileIO/task 携带 |

最容易混淆的是 C 和 D：

~~~text
Azure SharedKey
  -> FILE_S3
  -> BE 原生 C++ Azure client

Azure OAuth2 / 本 PR 的 SAS
  -> FILE_HDFS
  -> Hadoop ABFS
~~~

### 14.3 链路 A：FE 访问 Databricks REST Catalog

#### 14.3.1 入口

FE 的 Iceberg connector 在 createCatalog() 中构造 Catalog：

~~~java
Map<String, String> catalogOptions =
        IcebergCatalogFactory.buildCatalogProperties(catalogProps, chosenS3);
Map<String, String> storageHadoopConfig = buildStorageHadoopConfig();

if (TYPE_REST.equals(flavor)) {
    return buildRestSessionCatalogDefault(catalogName, catalogOptions, conf);
}
~~~

源码：

- [IcebergConnector.java:910](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java:910>)
- [IcebergConnector.java:987](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java:987>)

REST Catalog 的具体实现是 Iceberg SDK 的 RESTSessionCatalog：

~~~java
RESTSessionCatalog sessionCatalog = new RESTSessionCatalog();
CatalogUtil.configureHadoopConf(sessionCatalog, conf);
sessionCatalog.initialize(catalogName, props);
~~~

源码：

- [IcebergConnector.java:1024](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java:1024>)

#### 14.3.2 使用哪些参数

Doris 解析的 REST 参数包括：

~~~text
iceberg.rest.uri
iceberg.rest.prefix
iceberg.rest.connection-timeout-ms
iceberg.rest.socket-timeout-ms

iceberg.rest.security.type=oauth2
iceberg.rest.oauth2.credential
iceberg.rest.oauth2.token
iceberg.rest.oauth2.scope
iceberg.rest.oauth2.server-uri
iceberg.rest.oauth2.token-refresh-enabled
~~~

如果请求 vended credentials，还会配置：

~~~text
iceberg.rest.vended-credentials-enabled=true
~~~

并向 REST Catalog 发送：

~~~text
X-Iceberg-Access-Delegation: vended-credentials
~~~

参数声明源码：

- [IcebergRestMetaStoreProperties.java:64](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-metastore-iceberg/src/main/java/org/apache/doris/connector/metastore/iceberg/rest/IcebergRestMetaStoreProperties.java:64>)

参数转换源码：

- [IcebergCatalogFactory.java:489](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergCatalogFactory.java:489>)
- [IcebergCatalogFactory.java:506](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergCatalogFactory.java:506>)

#### 14.3.3 这条链路返回什么

REST loadTable 返回的内容包括：

~~~text
表 metadata
metadata location
table config
storage credentials
~~~

Iceberg SDK 随后构造 Table 和 Table.io()。这个 Table.io() 是后续 FE 读取 manifest、以及抽取 Azure vended credentials 的来源。

注意：这条链路访问的是 Databricks REST API，本身不直接读取 Azure parquet 文件。

### 14.4 链路 B：FE 使用 Iceberg FileIO 读取 Azure 元数据

#### 14.4.1 FE 先加载 Table

查询规划时，IcebergScanPlanProvider 通过 catalog ops 加载 Table：

~~~java
Table raw = IcebergStatementScope.sharedTable(..., () -> {
    return context.executeAuthenticated(() -> loadRawTable(ops, handle));
});
~~~

源码：

- [IcebergScanPlanProvider.java:3032](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:3032>)

随后执行：

~~~java
TableScan scan = buildScan(table, iceHandle, filter, session);
scan.planFiles();
~~~

源码：

- [IcebergScanPlanProvider.java:707](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:707>)
- [IcebergScanPlanProvider.java:782](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:782>)

planFiles() 会通过 Table.io() 读取 snapshot、manifest list 和 manifest。

#### 14.4.2 abfss 路径选择什么 FileIO

Iceberg 的默认 ResolvingFileIO 根据路径 scheme 选择实现：

~~~text
s3/s3a/s3n       -> S3FileIO
gs               -> GCSFileIO
abfs/abfss       -> ADLSFileIO
wasb/wasbs       -> ADLSFileIO
其他             -> HadoopFileIO
~~~

Doris 的单元测试直接验证了这个映射：

~~~java
ResolvingFileIO fileIO = new ResolvingFileIO();
fileIO.initialize(Collections.emptyMap());

assertEquals(
    "org.apache.iceberg.azure.adlsv2.ADLSFileIO",
    fileIO.ioClass("abfss://container@account.dfs.core.windows.net/table").getName());
~~~

源码：

- [IcebergScanPlanProviderTest.java:3258](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/IcebergScanPlanProviderTest.java:3258>)

Iceberg Azure 依赖由 connector POM 提供：

- [fe-connector-iceberg/pom.xml:153](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/pom.xml:153>)

#### 14.4.3 ADLSFileIO 使用什么 SDK

ADLSFileIO 是 Apache Iceberg 的实现，不是 Doris 的 AzureFileSystem。它内部创建 Microsoft Azure Java SDK 的：

~~~text
DataLakeFileSystemClientBuilder
DataLakeFileSystemClient
DataLakeFileClient
~~~

本地依赖类：

~~~text
org.apache.iceberg.azure.adlsv2.ADLSFileIO
~~~

本地 JAR：

~~~text
/mnt/disk1/chenjunwei/.m2/repository/org/apache/iceberg/
  iceberg-azure/1.10.1/iceberg-azure-1.10.1.jar
~~~

Apache Iceberg 1.10.1 上游源码位置：

- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/adlsv2/ADLSFileIO.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/io/ResolvingFileIO.java

#### 14.4.4 ADLSFileIO 使用哪些参数

Iceberg AzureProperties 识别：

~~~text
adls.sas-token.<account-host>
adls.sas-token-expires-at-ms.<account-host>
adls.connection-string.<account-host>
adls.auth.shared-key.account.name
adls.auth.shared-key.account.key
adls.token
adls.refresh-credentials-enabled
adls.refresh-credentials-endpoint
~~~

对于本 PR 的 Databricks vended SAS，相关信息包括：

~~~text
adls.sas-token.account.dfs.core.windows.net=<token>
adls.sas-token-expires-at-ms.account.dfs.core.windows.net=<expiry>
~~~

Iceberg ADLSFileIO 支持两种取得 SAS 的方式：

~~~text
方式一：properties 中已经有 adls.sas-token.<host>
       -> 根据 abfss 路径中的 account host 直接选择 SAS

方式二：properties 中有 adls.refresh-credentials-endpoint
       -> VendedAdlsCredentialProvider 向 REST credentials endpoint 获取/刷新 SAS
       -> 根据 account host 缓存并应用 SAS
~~~

REST loadTable 返回的 table config 和 credentials 是两个字段。Iceberg SDK 会把 table config 合并进 FileIO properties，并把 credentials 保存到支持 StorageCredential 的 FileIO 上。实际使用直接 SAS 还是 refresh endpoint，取决于 Catalog 返回的配置；Doris 的 CredentialUtils 不参与这两个 Iceberg SDK 分支。

#### 14.4.5 CredentialUtils 是否参与这条 FE FileIO 链路

不参与。

~~~text
FE ADLSFileIO
  -> 直接理解 adls.*
  -> 不需要 CredentialUtils 转成 fs.azure.*
~~~

CredentialUtils 的转换发生在 FE 已经拿到 Table.io() 后，目的是给 Doris storage routing 和 BE Hadoop ABFS 准备参数，不是给 FE ADLSFileIO 准备参数。

#### 14.4.6 Doris 自己的 Azure plugin 是不是同一个东西

不是。

Doris 的 fe-filesystem-azure/AzureObjStorage 使用 Azure Blob Java SDK：

~~~text
BlobServiceClientBuilder
BlobServiceClient
BlobContainerClient
BlobClient
~~~

源码：

- [AzureObjStorage.java:26](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureObjStorage.java:26>)
- [AzureObjStorage.java:100](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureObjStorage.java:100>)

它是 Doris 通用 filesystem 路径。Iceberg 查询规划读取 metadata/manifest 时使用的是 Table.io() 和 Iceberg ADLSFileIO，两者不要混为一谈。

### 14.5 链路 C：FE 把 Azure SAS 参数发送给 BE，BE 通过 Hadoop ABFS 读数据

这是本 PR 修复的主要数据面链路。

#### 14.5.1 FE 从 Table.io() 提取 vended credentials

IcebergScanPlanProvider 从 FileIO 中合并两类属性：

~~~java
FileIO fileIO = table.io();
Map<String, String> ioProps = new HashMap<>(fileIO.properties());

if (fileIO instanceof SupportsStorageCredentials) {
    for (StorageCredential storageCredential :
            ((SupportsStorageCredentials) fileIO).credentials()) {
        ioProps.putAll(storageCredential.config());
    }
}
~~~

源码：

- [IcebergScanPlanProvider.java:1845](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:1845>)

这里得到的仍是 Iceberg 格式：

~~~text
adls.sas-token.<account-host>
adls.sas-token-expires-at-ms.<account-host>
~~~

#### 14.5.2 CredentialUtils 转换为 Hadoop ABFS 格式

CredentialUtils 执行：

~~~text
输入：
adls.sas-token.<account-host>=<token>

输出：
fs.azure.account.auth.type.<account-host>=SAS
fs.azure.sas.fixed.token.<account-host>=<token>
~~~

核心代码：

~~~java
adlsSasTokens.forEach((accountHost, sasToken) -> {
    normalized.put(
        "fs.azure.account.auth.type." + accountHost,
        "SAS");
    normalized.put(
        "fs.azure.sas.fixed.token." + accountHost,
        sasToken);
});
~~~

源码：

- [CredentialUtils.java:87](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java:87>)

#### 14.5.3 FE 构造 vended StorageAdapter

DefaultConnectorContext 调用：

~~~java
Map<String, String> normalized =
        CredentialUtils.normalizeCloudStorageProperties(rawVendedCredentials);
List<StorageAdapter> vended = StorageAdapter.ofAll(normalized);
~~~

由于当时 Azure provider 不能直接识别动态的 adls.sas-token.*，转换后的 fs.azure.* 会先落入 HDFS fallback。你的 PR 再把同一个 adapter 的 Map key 从 HDFS 修正为 AZURE：

~~~java
result.remove(StorageTypeId.HDFS);
result.put(StorageTypeId.AZURE, adapter);
~~~

源码：

- [DefaultConnectorContext.java:247](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:247>)

#### 14.5.4 FE 决定路径与 reader

这份 adapter 内的配置仍然是 Hadoop fs.azure.*。FE 同时做两件事：

1. 保留 abfss:// 路径，不转换为 s3://；
2. 将 TFileType 设为 FILE_HDFS。

路径特判源码：

- [StorageAdapter.java:348](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/storage/StorageAdapter.java:348>)

reader 特判源码：

- [LocationPath.java:369](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/common/util/LocationPath.java:369>)

最终每个 Iceberg range 可以携带：

~~~text
path=abfss://container@account.dfs.core.windows.net/path/file.parquet
backendFileType=FILE_HDFS
fileFormat=parquet
~~~

reader 类型进入 range 的源码：

- [IcebergScanPlanProvider.java:731](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:731>)
- [IcebergScanPlanProvider.java:1609](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:1609>)
- [PluginDrivenSplit.java:49](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/split/PluginDrivenSplit.java:49>)

#### 14.5.5 FE 将凭证放入 location.* 属性

IcebergScanPlanProvider 将转换后的 BE 参数加上 location. 前缀：

~~~java
Map<String, String> vendedBeProps =
        storage().vendStorageCredentials(extractVendedToken(...));

vendedBeProps.forEach((k, v) ->
        props.put("location." + k, v));
~~~

源码：

- [IcebergScanPlanProvider.java:1962](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:1962>)

PluginDrivenScanNode 去掉 location. 前缀：

~~~java
if (entry.getKey().startsWith("location.")) {
    String realKey = entry.getKey().substring("location.".length());
    locationProps.put(realKey, entry.getValue());
}
~~~

源码：

- [PluginDrivenScanNode.java:909](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java:909>)

#### 14.5.6 FE 将 Map 转为 Thrift Hadoop 参数

当 locationType 是 FILE_HDFS 时：

~~~java
THdfsParams tHdfsParams =
        HdfsResource.generateHdfsParam(locationProperties);
params.setHdfsParams(tHdfsParams);
~~~

源码：

- [FileQueryScanNode.java:513](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FileQueryScanNode.java:513>)
- [HdfsResource.java:105](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/catalog/HdfsResource.java:105>)

除 fs.defaultFS、user、Kerberos 等结构化字段以外，其他配置都进入：

~~~text
THdfsParams.hdfs_conf[]
  - key
  - value
~~~

因此两个 SAS 键也会进入 hdfs_conf：

~~~text
fs.azure.account.auth.type.<host>=SAS
fs.azure.sas.fixed.token.<host>=<token>
~~~

#### 14.5.7 BE 选择 HdfsFileSystem

BE FileFactory 根据 TFileType 分支：

~~~cpp
case TFileType::FILE_HDFS: {
    std::string fs_name = _get_fs_name(file_description);
    return io::HdfsFileSystem::create(
        *fs_properties.properties,
        fs_name,
        io::FileSystem::TMP_FS_ID,
        nullptr);
}
~~~

源码：

- [file_factory.cpp:99](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp:99>)
- [file_factory.cpp:240](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp:240>)

从完整路径：

~~~text
abfss://container@account.dfs.core.windows.net/path/file.parquet
~~~

提取出的 fs_name 是：

~~~text
abfss://container@account.dfs.core.windows.net
~~~

这就是前文所说的 Azure storage authority。

#### 14.5.8 BE 将 fs.azure.* 写入 Hadoop builder

BE parse_properties() 把普通键都放入 THdfsConf，然后 create_hdfs_builder() 逐个写入 Hadoop builder：

~~~cpp
for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
    builder->set_hdfs_conf(conf.key, conf.value);
}
builder->set_hdfs_conf_to_hdfs_builder();
~~~

源码：

- [hdfs_builder.cpp:165](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp:165>)
- [hdfs_builder.cpp:197](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp:197>)

最终调用链：

~~~text
FILE_HDFS
  -> Doris HdfsFileSystem/HdfsFileReader
  -> libhdfs/JNI
  -> Hadoop FileSystem
  -> Hadoop 看到 abfss://
  -> AzureBlobFileSystem/ABFS
  -> 使用 fs.azure.* SAS
  -> Azure ADLS
~~~

这条 SAS 链路不使用 BE 原生 C++ Azure client。

### 14.6 链路 D：BE SharedKey 使用原生 C++ Azure client

这是另一条独立链路，仅用于解释为什么 Azure 有时又表现为 FILE_S3。

#### 14.6.1 FE 生成对象存储参数

Azure SharedKey 的 BackendStorageProperties.toMap() 生成：

~~~text
AWS_ENDPOINT
AWS_REGION=dummy_region
AWS_ACCESS_KEY=<account-name>
AWS_SECRET_KEY=<account-key>
AWS_NEED_OVERRIDE_ENDPOINT=true
provider=azure
use_path_style
~~~

源码：

- [AzureFileSystemProperties.java:233](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureFileSystemProperties.java:233>)

#### 14.6.2 BE 从 FILE_S3 入口进入

BE 的 FILE_S3 分支先将参数解析为 S3Conf：

~~~cpp
S3ClientFactory::convert_properties_to_s3_conf(
    properties, s3_uri, &s3_conf);
~~~

源码：

- [file_factory.cpp:114](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp:114>)
- [s3_util.cpp:407](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/util/s3_util.cpp:407>)

当 provider=azure 时：

~~~cpp
s3_conf->client_conf.provider = ObjStorageProvider::AZURE;
~~~

BE 随后创建 Microsoft Azure C++ SDK Blob client，使用：

~~~text
endpoint + bucket/container
account_name = AWS_ACCESS_KEY
account_key  = AWS_SECRET_KEY
~~~

源码：

- [s3_util.cpp:269](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/util/s3_util.cpp:269>)

因此：

~~~text
FILE_S3
  != 一定是 Amazon S3

FILE_S3 + provider=azure
  -> Doris 原生对象存储入口
  -> Azure C++ SDK
~~~

当前这条原生链路主要支持 SharedKey，不是本 PR 的 vended SAS 实现。

### 14.7 链路 E：BE Java metadata scanner 读取 Iceberg 系统表

这条链路适用于 $files、$entries、$manifests 等 Iceberg system table。$position_deletes 是例外，它在本 PR 中走 native reader，前文已经单独说明。

#### 14.7.1 FE 规划 metadata table

FE 先解析系统表，调用 metadata table 的 scan.planFiles()：

~~~java
Table metadataTable = resolveSysTable(session, handle);
TableScan scan = buildScan(metadataTable, handle, filter, session);

try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
    for (FileScanTask task : tasks) {
        ranges.add(new IcebergScanRange.Builder()
            .path(SYS_TABLE_DUMMY_PATH)
            .serializedSplit(SerializationUtil.serializeToBase64(task))
            .build());
    }
}
~~~

源码：

- [IcebergScanPlanProvider.java:906](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:906>)

关键点：

~~~text
FE 不是只发送一个 manifest URL；
FE 将整个 Iceberg FileScanTask 序列化为 serialized_split。
~~~

对于 $files、$entries 等 manifest task，这个任务中会包含：

~~~text
manifest 信息
schema/projection
FileIO
FileIO properties / StorageCredential
~~~

#### 14.7.2 FE 将任务放入 Iceberg range

IcebergScanRange 在 system-table 分支中设置：

~~~java
rangeDesc.setFormatType(FORMAT_JNI);
fileDesc.setSerializedSplit(serializedSplit);
formatDesc.setIcebergParams(fileDesc);
~~~

源码：

- [IcebergScanRange.java:375](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanRange.java:375>)

#### 14.7.3 BE C++ 启动 Java scanner

BE C++ reader 将 serialized_split 传给：

~~~text
org/apache/doris/iceberg/IcebergSysTableJniScanner
~~~

同时，它会把 scan params 中的 properties 加上 hadoop. 前缀后传给 Java：

~~~cpp
params["serialized_split"] = ...;
params["hadoop." + kv.first] = kv.second;
~~~

源码：

- [iceberg_sys_table_jni_reader.cpp:46](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/format/table/iceberg_sys_table_jni_reader.cpp:46>)
- [iceberg_sys_table_reader.cpp:53](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/format_v2/jni/iceberg_sys_table_reader.cpp:53>)

这些 hadoop.* 参数主要用于 PreExecutionAuthenticator，例如 Kerberos 执行上下文。对于 vended ADLS，核心 FileIO 与 storage credentials 主要随 serialized task 一起传递。

#### 14.7.4 BE Java 反序列化并执行 task

Java scanner 构造时：

~~~java
this.scanTask =
    SerializationUtil.deserializeFromBase64(serializedSplitParams);
~~~

打开时：

~~~java
reader = scanTask.asDataTask().rows().iterator();
~~~

源码：

- [IcebergSysTableJniScanner.java:53](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/src/main/java/org/apache/doris/iceberg/IcebergSysTableJniScanner.java:53>)
- [IcebergSysTableJniScanner.java:85](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/src/main/java/org/apache/doris/iceberg/IcebergSysTableJniScanner.java:85>)

#### 14.7.5 BE Java 是否真的访问 Azure

答案取决于 task 类型。

对于已经在 FE 物化好的 StaticDataTask：

~~~text
BE 主要反序列化内存行
不一定再次访问 Azure
~~~

对于 $files、$entries 这类 ManifestReadTask：

~~~text
scanTask.rows()
  -> ManifestFiles.open(manifest, io, specs)
  -> task 中的 FileIO
  -> ResolvingFileIO
  -> ADLSFileIO
  -> Azure Java DataLake SDK
  -> 远程读取 manifest
~~~

因此不能笼统地说“BE system table 一定不访问 Azure”，也不能说“所有 system table 都会重新访问 Azure”。准确说法是：

> BE Java scanner 执行 FE 序列化的 Iceberg task；task 如果需要读取 manifest，就会使用随 task 恢复的 FileIO 访问 Azure。

Apache Iceberg 上游相关源码：

- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/BaseFilesTable.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/BaseEntriesTable.java

#### 14.7.6 为什么 BE extension 也要打包 ADLSFileIO

FE connector 里有 iceberg-azure，不代表 BE Java extension 的 classpath 中也有它。

如果 BE 反序列化的 ResolvingFileIO 要创建 ADLSFileIO，而 classpath 中缺少：

~~~text
org.apache.iceberg.azure.adlsv2.ADLSFileIO
~~~

就会出现 ClassNotFoundException。

因此 PR 给两个 BE Java 扩展增加：

~~~text
iceberg-azure
iceberg-azure-bundle
~~~

源码：

- [iceberg-metadata-scanner/pom.xml:51](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/pom.xml:51>)
- [preload-extensions/pom.xml:178](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/preload-extensions/pom.xml:178>)

并增加类路径测试：

- [IcebergMetadataScannerClasspathTest.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/src/test/java/org/apache/doris/iceberg/IcebergMetadataScannerClasspathTest.java>)
- [PreloadExtensionsClasspathTest.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/preload-extensions/src/test/java/org/apache/doris/iceberg/PreloadExtensionsClasspathTest.java>)

### 14.8 CredentialUtils 的准确职责

CredentialUtils 只在 FE 执行。BE C++ 和 BE Java scanner 都不调用它。

它的输入、输出和使用者如下：

| 阶段 | 输入 | CredentialUtils 输出 | 消费者 |
| --- | --- | --- | --- |
| 过滤 | Table.io() properties + StorageCredential.config() | 只保留 fs./s3./oss./cos./obs./gs./azure./adls. 等 | FE storage binding |
| Azure SAS 翻译 | adls.sas-token.<host> | fs.azure.account.auth.type.<host>=SAS；fs.azure.sas.fixed.token.<host>=token | FE routing + BE Hadoop ABFS |
| BE map 聚合 | StorageAdapter map | 扁平的 AWS_*、fs.*、dfs.*、hadoop.* map | FE 再通过 Thrift 发给 BE |

#### 14.8.1 它如何服务 FE

FE 使用转换结果来：

~~~text
构造 vended StorageAdapter
识别 Azure SAS
为 abfss 路径找到 adapter
保留 abfss URI
选择 FILE_HDFS
~~~

#### 14.8.2 它如何服务 BE

它不在 BE 中运行，而是由 FE 生成 BE 所需参数：

~~~text
CredentialUtils
  -> fs.azure.*
  -> location.fs.azure.*
  -> PluginDrivenScanNode 去掉 location.
  -> THdfsParams.hdfs_conf
  -> BE hdfs_builder
  -> Hadoop ABFS
~~~

所以“CredentialUtils 给 FE 和 BE 使用”应准确理解为：

~~~text
执行位置：FE

FE 内部用途：
  storage routing、URI normalization、reader selection

BE 用途：
  FE 把它的输出通过 RPC/Thrift 发给 BE
~~~

#### 14.8.3 它不服务哪条链路

CredentialUtils 不参与：

~~~text
Databricks REST OAuth 登录
FE ADLSFileIO 读取 metadata/manifest
BE Java scanner 内部恢复 ADLSFileIO
BE SharedKey 原生 C++ Azure client 的 AWS_* 生成
~~~

FE ADLSFileIO 本身理解 adls.*；CredentialUtils 的主要价值，是把同一份 vended SAS 改写为 BE Hadoop ABFS 能理解的 fs.azure.*。

### 14.9 写入和 DML 的补充链路

写入同样需要：

~~~text
Azure SAS 凭证
abfss:// output path
FILE_HDFS
~~~

IcebergWritePlanProvider 会将静态 storage 参数和 vended 参数合并进 Hadoop config。

对于 DELETE 后的 UPDATE/MERGE，BE 的 deletion-vector helper 没有普通 scan range 的 per-file fs_name，因此 PR 从 dataLocation 推导：

~~~text
fs.defaultFS =
abfss://container@account.dfs.core.windows.net
~~~

链路：

~~~text
IcebergWritePlanProvider
  -> 提取 vended token
  -> vendStorageCredentials()
  -> fs.azure.* SAS
  -> 从 dataLocation 推导 fs.defaultFS
  -> TIceberg sink hadoopConfig
  -> BE deletion-vector helper
  -> Hadoop ABFS
~~~

源码：

- [IcebergWritePlanProvider.java:1007](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java:1007>)

### 14.10 一次普通 SELECT 的端到端时序

把上面的链路压缩成一次真实查询：

~~~text
1. FE 使用 iceberg.rest.* 登录 Databricks REST Catalog

2. FE loadTable
   Databricks 返回：
   - Iceberg table metadata
   - per-table config
   - Azure StorageCredential（adls.sas-token.*）

3. Iceberg SDK 构造 Table.io()
   ResolvingFileIO 按 abfss 选择 ADLSFileIO

4. FE planFiles()
   ADLSFileIO + Azure Java SDK 读取 metadata/manifest
   得到数据文件路径和 delete file 信息

5. FE 从 Table.io() 提取 adls.* vended token

6. CredentialUtils 将 adls.sas-token.* 转为 fs.azure.*

7. FE 构造 vended adapter、保留 abfss、选择 FILE_HDFS

8. FE 将：
   - abfss 文件路径
   - FILE_HDFS
   - fs.azure.* SAS
   写入 scan range/scan params

9. BE 收到 TFileType.FILE_HDFS + THdfsParams

10. BE HdfsFileSystem -> libhdfs/JNI -> Hadoop ABFS

11. Hadoop ABFS 用 fs.azure.* SAS 读取 Azure parquet/orc/Puffin

12. BE 再按 fileFormat 使用 Parquet/ORC/Puffin reader 解码内容
~~~

### 14.11 源码索引

#### Doris FE：Catalog 与 FileIO

- [IcebergConnector.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java>)
- [IcebergCatalogFactory.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergCatalogFactory.java>)
- [IcebergRestMetaStoreProperties.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-metastore-iceberg/src/main/java/org/apache/doris/connector/metastore/iceberg/rest/IcebergRestMetaStoreProperties.java>)
- [IcebergScanPlanProvider.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java>)

#### Doris FE：凭证与路由

- [CredentialUtils.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java>)
- [DefaultConnectorContext.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java>)
- [StorageAdapter.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/storage/StorageAdapter.java>)
- [LocationPath.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/common/util/LocationPath.java>)
- [PluginDrivenScanNode.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java>)
- [FileQueryScanNode.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FileQueryScanNode.java>)

#### Doris BE：普通文件访问

- [file_factory.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp>)
- [hdfs_builder.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp>)
- [s3_util.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/util/s3_util.cpp>)

#### Doris BE：Iceberg 系统表

- [iceberg_sys_table_jni_reader.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/format/table/iceberg_sys_table_jni_reader.cpp>)
- [IcebergSysTableJniScanner.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/src/main/java/org/apache/doris/iceberg/IcebergSysTableJniScanner.java>)

#### Apache Iceberg 1.10.1 上游

- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/rest/RESTSessionCatalog.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/io/ResolvingFileIO.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/adlsv2/ADLSFileIO.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/BaseFilesTable.java
- https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/BaseEntriesTable.java

### 14.12 最终结论

这几条链路不是一套客户端、也不是一套参数反复传递，而是有明确的转换边界：

~~~text
Databricks Catalog：
  iceberg.rest.* -> Iceberg REST SDK

FE Azure metadata：
  adls.* -> ADLSFileIO -> Azure Java DataLake SDK

BE Azure SAS data：
  CredentialUtils: adls.* -> fs.azure.*
  FILE_HDFS -> Hadoop ABFS

BE Azure SharedKey data：
  AWS_* + provider=azure
  FILE_S3 -> Azure C++ SDK

BE Iceberg system table：
  serialized FileScanTask/FileIO
  Java scanner 根据 task 类型读取内存行或远程 manifest
~~~

CredentialUtils 是连接 FE Iceberg vended credentials 与 BE Hadoop ABFS 的 FE 侧桥梁；它不是 FE ADLSFileIO 的一部分，也不会在 BE 中运行。
