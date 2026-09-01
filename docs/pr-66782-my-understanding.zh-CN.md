# 我对 PR #66782 的理解

本文只记录对 Azure vended credentials PR 的理解。SPI 重构 PR 的正文暂时留空。

本文回答四个问题：

1. PR 修复前后，Iceberg 表从 Catalog 到 Azure 数据文件的完整链路是什么？
2. `ADLSFileIO`、Doris 的 `AzureFileSystemProvider`、BE reader 分别做什么？
3. 为什么要把 `adls.sas-token.*` 转成 `fs.azure.*`，并把 `backendFileType` 设为 `FILE_HDFS`？
4. DML、`$position_deletes`、`$files` 等其他路径为什么也要修改？

## 1. 先看结论

- Databricks REST Catalog 的 OAuth 凭证只用于 FE 调用 Catalog；Azure SAS 凭证用于读取 ADLS 文件。两者不是一套凭证。
- FE 规划 Iceberg 扫描时，使用 Iceberg 官方的 `ADLSFileIO` 读取 metadata 和 manifest。普通数据扫描时，BE 不直接使用这个 `FileIO`。
- Doris 确实有自己的 Azure plugin：`AzureFileSystemProvider`。它服务 Doris 的通用 FE 文件操作，不会因为 Iceberg 路径是 `abfss://` 就自动替代 `ADLSFileIO`。
- 本 PR 的核心是把 Iceberg 返回的 `adls.sas-token.*` 转成 Hadoop ABFS 能识别的 `fs.azure.*`，再把 `abfss://`、`FILE_HDFS` 和这套配置一起送到 BE。
- `FILE_HDFS` 表示“使用 Doris 的 Hadoop reader”，不表示文件存储在 HDFS；Hadoop 看到 `abfss://` 后会选择 Azure ABFS 实现。
- SPI 重构 PR 的目标是让存储 provider 自己声明凭证前缀、URI 规范化和 backend reader，减少 FE 中的固定特判；本节暂不展开。

## 2. 必要概念

### 2.1 Iceberg 表、metadata 和 manifest

Iceberg 表不是只有 parquet/orc 数据文件。表还包含一组描述文件：

```text
metadata.json
  -> manifest list
       -> manifest files
            -> data file / delete file 路径
```

普通 `SELECT` 的 FE 规划阶段需要读取这些文件，得到要扫描的数据文件和 delete file。

`$files`、`$entries`、`$manifests` 是 Iceberg 的 metadata table。它们展示的也是 manifest 中的信息，但执行路径和普通数据扫描不同，后文单独说明。

### 2.2 两类凭证

| 凭证 | 用途 | 示例 |
| --- | --- | --- |
| Catalog 凭证 | FE 调用 Databricks Iceberg REST Catalog | `iceberg.rest.oauth2.credential`、`iceberg.rest.oauth2.token` |
| Azure 数据凭证 | 读取 `abfss://` 下的 metadata、manifest、parquet、orc、Puffin | `adls.sas-token.*` 或 `fs.azure.*` |

Catalog 凭证解决“FE 能不能调用 Databricks API”；Azure SAS 解决“FE/BE 能不能读取 Azure 文件”。

### 2.3 Azure SAS、ABFS 和 Azure authority

SAS 是带权限和过期时间的临时 token，不是文件格式。

Iceberg/Databricks 返回的格式示例：

```text
adls.sas-token.account.dfs.core.windows.net = <token>
```

Hadoop ABFS 需要的格式：

```text
fs.azure.account.auth.type.account.dfs.core.windows.net = SAS
fs.azure.sas.fixed.token.account.dfs.core.windows.net = <token>
```

ABFS 是 Hadoop 的 Azure 文件系统实现。`abfss://` URI 中的 authority：

```text
abfss://container@account.dfs.core.windows.net/path/file.parquet
        └──────────────────────────────────────┘
```

`container@account.dfs.core.windows.net` 告诉 Hadoop 访问哪个 Azure storage account 和 container。它不能在转换路径时丢失。

### 2.4 `fileFormat`、`TFileType` 和 `backendFileType`

```text
fileFormat      = parquet / orc / puffin
backendFileType = FILE_HDFS / FILE_S3 / FILE_BROKER / FILE_LOCAL
```

`fileFormat` 表示“文件内容怎么解码”；`backendFileType` 表示“BE 用哪一类文件系统打开文件”。

`backendFileType` 进入 `ConnectorScanRange`，最终由 `PluginDrivenSplit` 设置为 BE 使用的 `TFileType`。

## 3. `ADLSFileIO` 和 Doris `AzureFileSystemProvider`

这两个实现都在 FE 进程中，但不属于同一套 API，也不负责同一件事。

| 实现 | 提供者 | 接口 | 典型调用 | 底层 Azure SDK | 主要用途 |
| --- | --- | --- | --- | --- | --- |
| `ADLSFileIO` | Apache Iceberg | Iceberg `FileIO` | `table.io()`、`ManifestFiles.read(...)` | Azure DataLake Java SDK | Iceberg metadata/manifest 和 Iceberg 文件操作 |
| `AzureFileSystemProvider` | Doris | Doris `FileSystemProvider` | `FileSystemFactory.getFileSystem(...)` | Azure Blob Java SDK | Doris 通用 FE 文件操作 |
| `HdfsFileReader` | Doris BE | Doris BE reader | `FILE_HDFS` | Hadoop FileSystem/ABFS | BE 读取 Azure SAS 数据文件 |
| `S3FileReader` | Doris BE | Doris BE reader | `FILE_S3` | Doris C++ 对象存储客户端 | BE 读取 SharedKey 等对象存储路径 |

### 3.1 FE 读取 Iceberg manifest：使用 `ADLSFileIO`

Iceberg connector 加载表后调用：

```java
Table table = catalog.loadTable(identifier);
CloseableIterable<FileScanTask> tasks = table.newScan().planFiles();
```

`planFiles()` 通过 `table.io()` 读取 snapshot、manifest list 和 manifest。对于 `abfs://`、`abfss://`、`wasb://`、`wasbs://`，Iceberg 的 `ResolvingFileIO` 选择 `ADLSFileIO`。

```java
ManifestFiles.read(manifest, table.io());
```

`ADLSFileIO` 是 Iceberg 官方实现，内部使用：

```text
DataLakeFileSystemClientBuilder
DataLakeFileSystemClient
DataLakeFileClient
```

它理解 `adls.*` 配置，例如：

```text
adls.sas-token.<account-host>
adls.sas-token-expires-at-ms.<account-host>
adls.refresh-credentials-endpoint
adls.auth.shared-key.account.name
adls.auth.shared-key.account.key
```

对于本 PR 的 vended SAS，FE 的 `ADLSFileIO` 可以直接使用 FileIO properties 或 `StorageCredential.config()` 中的 `adls.sas-token.*`。这条 FE FileIO 链路不需要 `CredentialUtils`。

源码：

- [IcebergScanPlanProvider.java:3032](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:3032>)：加载 Iceberg `Table`
- [IcebergScanPlanProvider.java:782](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:782>)：规划并遍历文件任务
- [IcebergManifestCache.java:250](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergManifestCache.java:250>)：通过 `table.io()` 读取 manifest
- [IcebergScanPlanProviderTest.java:3258](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/test/java/org/apache/doris/connector/iceberg/IcebergScanPlanProviderTest.java:3258>)：验证 `abfss://` 选择 `ADLSFileIO`

### 3.2 Doris Azure plugin：用于通用 FE 文件操作

Doris 的 plugin 入口是：

```text
AzureFileSystemProvider
  -> AzureFileSystem
      -> AzureObjStorage
          -> BlobServiceClient
```

典型调用方式：

```java
FileSystem fs = FileSystemFactory.getFileSystem(properties);
fs.exists(path);
fs.delete(path, false);
```

实际代码中的例子是 Azure 资源连通性检查：创建测试对象，再执行 `headObject`、列表或删除操作。其他 Doris 的外部存储、storage vault、load/export 等 FE 流程也可以通过 Doris `FileSystem` 抽象访问远程文件。

源码：

- [FileSystemFactory.java:95](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemFactory.java:95>)：根据属性选择 Doris filesystem provider
- [AzureFileSystemProvider.java:83](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureFileSystemProvider.java:83>)：绑定 Azure properties 并创建 filesystem
- [AzureObjStorage.java:100](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureObjStorage.java:100>)：创建 Azure Blob client
- [AzureResource.java:102](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/catalog/AzureResource.java:102>)：通过 Doris plugin 做 Azure 连通性测试

### 3.3 能不能用 Doris Azure plugin 读取 Iceberg manifest？

结论：通用 FE 文件操作可以直接用；Iceberg manifest 目前不能直接替换 `ADLSFileIO`。

原因是接口不同：

```text
Iceberg 需要 FileIO：
  newInputFile()
  newOutputFile()
  deleteFile()
  FileIO properties / credential 能力

Doris plugin 提供 FileSystem：
  open()
  exists()
  listFiles()
  mkdirs()
  delete()
```

如果要让 Iceberg 使用 Doris plugin，需要实现一个 Doris-backed Iceberg `FileIO` 适配器，并把 Iceberg 的 `table.io()` 配置到这个适配器。适配器还要满足 Iceberg 对输入流、文件长度、随机读取、凭证刷新和生命周期的要求。

当前 PR 没有做这个替换，原因包括：

- Iceberg 已经提供 `ADLSFileIO`，可以直接读取 `abfss://` 和 `adls.sas-token.*`；
- Doris Azure plugin 使用的是 Blob Java SDK，`ADLSFileIO` 使用的是 DataLake Java SDK，路径和层级命名空间语义不能假定完全相同；
- vended SAS 是按表/按请求返回、会过期的凭证，不能简单放入一个全局 Azure client；
- 即使 FE manifest 改用 Doris plugin，BE 读取 parquet/orc 的 reader 和凭证传递问题仍然存在。

因此本 PR 保留了两套实现的边界：

```text
FE Iceberg metadata/manifest -> Iceberg ADLSFileIO
FE 通用文件操作            -> Doris AzureFileSystemProvider
BE 普通数据文件             -> Doris HdfsFileReader / S3FileReader
```

## 4. PR 要解决的问题

场景是：Doris 通过 Iceberg REST Catalog 访问 Databricks Unity Catalog 中的 Iceberg 表。Databricks 在 `loadTable` 响应中返回每张表的 Azure vended SAS。

原有实现的缺口：

1. FE 的 Iceberg SDK 能加载表和读取部分元数据，但 vended SAS 的 `adls.*` 形式没有完整转成 BE 能理解的配置；
2. `abfss://` 在 Doris 的静态 scheme 表中可能映射到 `FILE_S3`，而 SAS 需要 Hadoop ABFS；
3. DML、position delete、system table 使用独立执行路径，不能自动继承普通 scan 的配置；
4. BE Java metadata scanner 的运行时 classpath 中缺少 Iceberg Azure 实现。

修复目标是把以下组合完整送到 BE：

```text
path:            abfss://container@account.dfs.core.windows.net/...
credentials:     fs.azure.* = SAS
backend reader:  FILE_HDFS
```

## 5. 修复前后的普通 SELECT 链路

### 5.1 修复前

```text
1. FE 用 iceberg.rest.* 调用 Databricks REST Catalog
2. Catalog 返回 Iceberg table metadata 和 adls.sas-token.*
3. Iceberg SDK 通过 ADLSFileIO 规划 metadata/manifest
4. FE 尝试把 vended 凭证交给 Doris storage routing
5. adls.* 可能被过滤、未转换，或落到 HDFS key 下
6. abfss:// 按静态 scheme 可能得到 FILE_S3
7. BE 用 S3 reader 解析 abfss://，或找不到 Azure 凭证
8. Catalog 能发现表，但 BE 读数据文件失败
```

即使已经生成 `fs.azure.*`，旧路由仍可能有索引错位：

```text
配置形状：fs.azure.*       -> 先绑定到 StorageTypeId.HDFS
路径形状：abfss://...       -> 查找 StorageTypeId.AZURE
结果：配置在 HDFS，路径找 AZURE
```

### 5.2 修复后的普通 SELECT

```text
1. FE 使用 iceberg.rest.* 登录 Databricks REST Catalog
2. loadTable 返回 table metadata、table config、StorageCredential
3. Iceberg ResolvingFileIO 按 abfss:// 选择 ADLSFileIO
4. ADLSFileIO + Azure DataLake SDK 读取 metadata/manifest
5. planFiles() 得到 data file、delete file 和文件位置
6. FE 从 table.io() 提取 FileIO properties 和 StorageCredential.config()
7. CredentialUtils: adls.sas-token.* -> fs.azure.*
8. DefaultConnectorContext 构造 vended StorageAdapter
9. 将同一个 adapter 从 HDFS key re-key 到 AZURE key
10. 保留 abfss://，不改成 s3://
11. Azure SAS 场景选择 backendFileType=FILE_HDFS
12. FE 把路径、FILE_HDFS、fs.azure.* 写入 scan range/Thrift 参数
13. BE FILE_HDFS -> HdfsFileSystem/HdfsFileReader
14. libhdfs/JNI -> Hadoop FileSystem -> Azure ABFS
15. Hadoop 使用 fs.azure.* SAS 读取 parquet/orc/Puffin
16. Doris reader 根据 fileFormat 解码文件内容
```

### 5.3 凭证转换的代码含义

FE 从 Iceberg `FileIO` 提取属性：

```java
FileIO fileIO = table.io();
Map<String, String> ioProps = new HashMap<>(fileIO.properties());

if (fileIO instanceof SupportsStorageCredentials) {
    for (StorageCredential credential : fileIO.credentials()) {
        ioProps.putAll(credential.config());
    }
}
```

源码：[IcebergScanPlanProvider.java:1845](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:1845>)

`CredentialUtils` 把 Azure Iceberg 方言转换成 Hadoop 方言：

```text
输入：
adls.sas-token.<host>=<token>

输出：
fs.azure.account.auth.type.<host>=SAS
fs.azure.sas.fixed.token.<host>=<token>
```

源码：[CredentialUtils.java:87](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java:87>)

`CredentialUtils` 只在 FE 执行。BE 不调用它，而是消费 FE 通过 Thrift 发送的结果。

### 5.4 为什么要把 adapter 从 HDFS key 改到 AZURE key？

`fs.azure.*` 是 Hadoop 配置，旧的 provider 识别流程会先把它绑定到 HDFS fallback。可是路径的 `StorageTypeId` 来自 URI scheme，`abfss://` 对应 AZURE。

因此本 PR 移动的是 Map 中的索引，不是修改配置内容：

```java
result.remove(StorageTypeId.HDFS);
result.put(StorageTypeId.AZURE, adapter);
```

源码：[DefaultConnectorContext.java:247](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:247>)

含义是：

```text
配置仍然是 fs.azure.*
访问实现仍然是 Hadoop ABFS
只是让 abfss:// 路径能找到这份 adapter
```

### 5.5 为什么保留 `abfss://`？

`abfss://container@account.dfs.core.windows.net/...` 包含 container 和 storage account authority。改成 `s3://` 会丢失 Hadoop ABFS 匹配 SAS 所需的信息，也会让 BE 进入错误的 reader family。

所以目标组合必须保持一致：

```text
abfss://...
  + fs.azure.*
  + FILE_HDFS
```

### 5.6 `backendFileType` 如何到达 BE？

Iceberg range 保存可选的 `backendFileType`：

```text
IcebergScanPlanProvider
  -> IcebergScanRange
      -> ConnectorScanRange.backendFileType
          -> PluginDrivenSplit.locationType
              -> BE TFileType.FILE_HDFS
```

源码：

- [ConnectorScanRange.java:84](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/scan/ConnectorScanRange.java:84>)
- [IcebergScanRange.java:200](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanRange.java:200>)
- [DefaultConnectorContext.java:470](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java:470>)：按路径计算 backend file type
- [PluginDrivenSplit.java:49](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/split/PluginDrivenSplit.java:49>)

它不是把所有 `abfss://` 都改成 `FILE_HDFS`，而是只在 Azure SAS 场景覆盖默认 scheme 推断：

```java
if (storageAdapter != null && storageAdapter.isAzureSasStorage()
        && StorageTypeId.AZURE.equals(StorageRegistry.fromScheme(schema))) {
    return TFileType.FILE_HDFS;
}
```

源码：[LocationPath.java:369](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/common/util/LocationPath.java:369>)

## 6. BE 如何访问 Azure

### 6.1 Azure SAS/OAuth2：Hadoop ABFS 路径

```text
TFileType.FILE_HDFS
  -> Doris HdfsFileSystem/HdfsFileReader
  -> hdfs_builder.cpp
  -> libhdfs/JNI
  -> Hadoop FileSystem
  -> Hadoop 根据 abfss:// 选择 Azure ABFS
  -> fs.azure.*
  -> Azure ADLS
```

BE 将 FE 传来的普通属性写入 Hadoop builder：

```cpp
for (const THdfsConf& conf : hdfsParams.hdfs_conf) {
    builder->set_hdfs_conf(conf.key, conf.value);
}
```

FE 侧把 `location.*` 属性提取出来并生成 `THdfsParams.hdfs_conf`：

- [PluginDrivenScanNode.java:909](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java:909>)
- [FileQueryScanNode.java:513](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FileQueryScanNode.java:513>)

源码：

- [file_factory.cpp:99](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp:99>)
- [hdfs_builder.cpp:165](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp:165>)
- [hdfs_builder.cpp:197](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp:197>)

### 6.2 Azure SharedKey：Doris native object-storage 路径

这是另一条链路，不是本 PR vended SAS 的主要路径：

```text
AzureFileSystemProperties.toMap()
  -> AWS_ENDPOINT
  -> AWS_ACCESS_KEY / AWS_SECRET_KEY
  -> provider=azure
  -> FILE_S3
  -> Doris C++ object-storage 入口
  -> Azure C++ Blob client
```

`FILE_S3` 是 Doris 的对象存储 reader family，不一定表示 Amazon S3。`provider=azure` 时，BE 创建 Azure client。

源码：

- [AzureFileSystemProperties.java:233](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureFileSystemProperties.java:233>)
- [file_factory.cpp:114](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp:114>)
- [s3_util.cpp:269](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/util/s3_util.cpp:269>)

当前 vended SAS 的路径不走这个 native `FILE_S3` 入口，而走 Hadoop ABFS。

## 7. 三条额外执行路径

### 7.1 UPDATE/MERGE：补 `fs.defaultFS`

UPDATE/MERGE 读取旧 deletion vector 时，DML sink 内部 helper 没有普通 scan range 的 per-file `fs_name`，会依赖 `fs.defaultFS`。

PR 从数据文件位置提取 authority：

```text
dataLocation:
abfss://container@account.dfs.core.windows.net/warehouse/...

fs.defaultFS:
abfss://container@account.dfs.core.windows.net
```

这样 helper 不会退化到没有 Azure authority 的 `hdfs://`。

源码：[IcebergWritePlanProvider.java:1007](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java:1007>)

### 7.2 `$position_deletes`：单独的 native range

`$position_deletes` 读取 Puffin deletion vector，使用独立的 range builder。修复前它没有普通数据 range 的 `FILE_HDFS`：

```text
abfss://.../dv.puffin
  -> 默认 FILE_S3
  -> Invalid S3 URI
```

PR 给这个 range 也设置 `FILE_HDFS`，使它进入 Hadoop ABFS 链路。

### 7.3 `$files` 等 system table：BE Java scanner

`$files`、`$entries`、`$manifests` 的任务会由 FE 规划后序列化为 `serialized_split`，再交给 BE Java extension。

```text
FE:
  metadataTable.newScan().planFiles()
  -> 序列化 FileScanTask（包含 manifest 信息和 FileIO）

BE C++:
  -> 启动 IcebergSysTableJniScanner

BE Java:
  -> 反序列化 FileScanTask
  -> scanTask.asDataTask().rows()
```

源码：

- [IcebergScanPlanProvider.java:906](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java:906>)：FE 规划 system table task
- [IcebergScanRange.java:375](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanRange.java:375>)：写入 `serialized_split`

如果 task 是已经物化的 `StaticDataTask`，BE 主要读取序列化的内存行；如果 task 是 `ManifestReadTask`，BE Java 会使用 task 中的 FileIO 重新读取 Azure manifest。

因此 BE Java extension 也必须能加载：

```text
org.apache.iceberg.azure.adlsv2.ADLSFileIO
```

PR 给以下模块补了 `iceberg-azure` 和 `iceberg-azure-bundle`：

- `iceberg-metadata-scanner`
- `preload-extensions`

源码：

- [iceberg_sys_table_jni_reader.cpp:46](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/format/table/iceberg_sys_table_jni_reader.cpp:46>)
- [IcebergSysTableJniScanner.java:53](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/src/main/java/org/apache/doris/iceberg/IcebergSysTableJniScanner.java:53>)
- [iceberg-metadata-scanner/pom.xml:51](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/iceberg-metadata-scanner/pom.xml:51>)
- [preload-extensions/pom.xml:178](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/be-java-extensions/preload-extensions/pom.xml:178>)

## 8. `CredentialUtils` 到底做什么

执行位置只有 FE。

```text
Iceberg FileIO properties / StorageCredential.config()
  -> CredentialUtils
      -> fs.azure.*
          -> vended StorageAdapter
              -> scan params / THdfsParams.hdfs_conf
                  -> BE Hadoop ABFS
```

它的职责：

- 过滤不应进入 Doris backend storage 的属性；
- 把 `adls.sas-token.*` 转成 `fs.azure.*`；
- 将 adapter 配置扁平化，供 FE 路由和 BE 参数生成使用。

它不负责：

- Databricks REST OAuth 登录；
- FE `ADLSFileIO` 读取 manifest；
- BE Java 自己恢复 `ADLSFileIO`；
- BE SharedKey native Azure client 的创建。

“CredentialUtils 给 FE 和 BE 使用”准确地说是：它在 FE 运行，BE 只通过 FE 发送的结果间接使用这些配置。

## 9. 本 PR 实际改了什么

| 功能 | 修改内容 |
| --- | --- |
| vended credential 基础能力 | 提取 Iceberg `StorageCredential`，转换 Azure SAS 属性，避免日志打印 token |
| 普通数据扫描 | 保留 `abfss://`，显式设置 `backendFileType=FILE_HDFS` |
| vended storage routing | 将 Azure SAS adapter 从 HDFS key 修正到 AZURE key |
| UPDATE/MERGE | 从数据位置补 `fs.defaultFS` |
| `$position_deletes` | 给 Puffin/native range 补 `FILE_HDFS` |
| system table | 给 BE Java extension 补 `iceberg-azure` 依赖和 classpath 测试 |

核心结果只有一个：

```text
Iceberg 的 Azure 临时凭证
  -> FE 转为 Hadoop ABFS 配置
  -> FE 明确告诉 BE 使用 FILE_HDFS
  -> BE 能通过 Hadoop ABFS 读取 Azure 文件
```

## 10. 后续：SPI 改进 PR

本节暂时留空。

## 11. 源码入口

以下路径以本地 Azure PR worktree `/mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended` 为准。

### Iceberg Catalog、FileIO 和 scan

- [IcebergConnector.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergConnector.java>)
- [IcebergCatalogFactory.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergCatalogFactory.java>)
- [IcebergScanPlanProvider.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanPlanProvider.java>)
- [IcebergWritePlanProvider.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergWritePlanProvider.java>)
- [IcebergScanRange.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergScanRange.java>)
- [IcebergRestMetaStoreProperties.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-connector/fe-connector-metastore-iceberg/src/main/java/org/apache/doris/connector/metastore/iceberg/rest/IcebergRestMetaStoreProperties.java>)

### Doris FE storage、凭证和 Azure plugin

- [CredentialUtils.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/CredentialUtils.java>)
- [DefaultConnectorContext.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java>)
- [StorageAdapter.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/storage/StorageAdapter.java>)
- [LocationPath.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/common/util/LocationPath.java>)
- [FileSystemFactory.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/fs/FileSystemFactory.java>)
- [AzureFileSystemProvider.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureFileSystemProvider.java>)
- [AzureObjStorage.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-filesystem/fe-filesystem-azure/src/main/java/org/apache/doris/filesystem/azure/AzureObjStorage.java>)

### Doris BE reader

- [PluginDrivenSplit.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/split/PluginDrivenSplit.java>)
- [PluginDrivenScanNode.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/PluginDrivenScanNode.java>)
- [FileQueryScanNode.java](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/fe/fe-core/src/main/java/org/apache/doris/datasource/scan/FileQueryScanNode.java>)
- [file_factory.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/file_factory.cpp>)
- [hdfs_builder.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/io/hdfs_builder.cpp>)
- [s3_util.cpp](</mnt/disk1/chenjunwei/doris_build/doris-pr-azure-vended/be/src/util/s3_util.cpp>)

### Iceberg 上游实现

- [ResolvingFileIO](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/io/ResolvingFileIO.java)
- [ADLSFileIO](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/adlsv2/ADLSFileIO.java)
- [AzureProperties](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/azure/src/main/java/org/apache/iceberg/azure/AzureProperties.java)
- [BaseFilesTable](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/BaseFilesTable.java)

## 12. 记录边界

- 本文是对 Azure vended credentials PR 的代码理解，不是 SPI 重构 PR 的评审结论。
- 结论以本地 worktree 代码和 Apache Iceberg 1.10.1 上游实现为依据。
- 本次只整理和 review 文档，没有运行新的编译或测试。
