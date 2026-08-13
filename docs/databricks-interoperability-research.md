# Apache Doris 对 Databricks 的互操作支持调研

原调研已按两个独立问题拆分。本文件仅保留导航，不再承载混合正文。

## 1. Databricks Iceberg 支持完善

阅读 [Apache Doris 对 Databricks Iceberg 的支持完善调研](databricks-iceberg-unity-catalog-research.md)。

这篇文档回答：

- 为什么 Doris 教程创建 Databricks External Location；
- 已有 Unity Catalog 和 managed Iceberg 表能否原样访问；
- “直接访问 managed table”与“绕过 UC 读取对象存储 URI”的区别；
- Iceberg REST、credential vending、default storage 的关系；
- Doris 当前 Iceberg REST、Azure ADLS SAS、三云凭证与测试方面的缺口；
- Iceberg 主线的竞品结论、交付计划与验收标准。

## 2. Native Delta Lake Catalog

阅读 [Apache Doris Native Delta Lake Catalog 调研](databricks-native-delta-lake-catalog-research.md)。

这篇文档回答：

- 为什么现有 Trino Connector 方案不等于 native Delta Catalog；
- Doris 作为外部 Delta client 需要实现哪些能力；
- Unity REST、temporary table credentials 和 catalog commits 的关系；
- 为什么建议使用 Delta Kernel，以及 Java Kernel 与 Rust Kernel 的架构选择；
- native Delta read/write 的范围、竞品结论、交付计划与验收标准。

两项工作可以共用 Databricks compatibility lab 和临时云凭证基础设施，但产品接口、格式语义、实现路线与验收标准分别维护。
