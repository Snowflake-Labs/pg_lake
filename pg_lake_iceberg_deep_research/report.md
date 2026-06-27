# pg_lake + Iceberg 小白入门调研 Wiki

> 目标读者：没有数据库背景，但需要理解 pg_lake、Iceberg、DuckDB、对象存储和 GaussDB 评估方向的人。
>
> 生成时间：2026-06-26。事实依据来自 `results/*.json` 的本地深度调研、pg_lake 仓库源码，以及 PostgreSQL、Apache Iceberg、DuckDB 官方文档。

---

## 0. 先给结论：我们到底要干什么

一句话：**pg_lake 想让用户像用 PostgreSQL 表一样使用数据湖里的大文件，同时用 Iceberg 保证这些文件组成一张“真正的表”，再用 DuckDB 把扫描和写文件做快。**

如果你完全没有数据库背景，可以先把系统想成一个图书馆：

| 图书馆比喻 | 技术里的角色 | 它负责什么 |
|---|---|---|
| 前台服务员 | PostgreSQL + pg_lake 扩展 | 接收 SQL、判断权限、处理事务、决定怎么执行 |
| 搬书工和扫描仪 | pgduck_server + DuckDB | 快速读取/写入 Parquet、CSV、JSON 等文件 |
| 书架上的书 | 对象存储里的数据文件 | 真正的数据，通常是很多 Parquet 文件 |
| 借阅目录和版本记录 | Apache Iceberg 元数据 | 记录哪些文件属于这张表、版本变化、删除记录 |
| 总目录指针 | Iceberg Catalog | 告诉所有人“当前最新目录是哪一份” |

所以我们要理解的不是一个单点技术，而是一条链路：

```text
用户 SQL
  -> PostgreSQL/pg_lake 判断 SQL 语义和事务
  -> pg_lake 找到当前 Iceberg 表状态
  -> pg_lake 把可下推的部分改写成 DuckDB 能执行的 SQL
  -> pgduck_server/DuckDB 读取或写入对象存储文件
  -> pg_lake 在提交时生成新的 Iceberg 元数据
  -> Catalog 更新“当前表版本”的指针
```

---

## 1. 最小知识地图

先不要背源码名。先掌握 9 个基础概念：

| 概念 | 小白解释 | 在 pg_lake 中为什么重要 |
|---|---|---|
| 数据库 | 一个能用 SQL 管数据的系统 | PostgreSQL 是用户看到的入口 |
| SQL | 让数据库查、写、改、删数据的语言 | 用户仍然写普通 SQL |
| 表 | 行和列组成的数据集合 | pg_lake 让数据湖文件看起来像表 |
| 事务 | 一组操作要么都成功，要么都失败 | 写文件、写元数据、更新 catalog 必须协调 |
| 文件格式 | 单个文件怎么存数据，比如 Parquet | DuckDB 负责高效读写这些文件 |
| 表格式 | 多个文件如何组成一张可演进的表 | Iceberg 负责 snapshot、manifest、delete file |
| Catalog | 表名到当前元数据文件的映射 | 没有它，外部引擎不知道哪份元数据是最新 |
| FDW | PostgreSQL 访问外部数据的插件接口 | pg_lake_table 用它把湖表接入 PostgreSQL 执行器 |
| Pushdown | 把计算推给更适合的引擎 | pg_lake 把大扫描推给 DuckDB，而不是让 PostgreSQL 一行行扫 |

---

## 2. 为什么不是直接让 PostgreSQL 读 Parquet

只读几个 Parquet 文件，DuckDB 已经可以做；PostgreSQL 也可以通过扩展接入。但 pg_lake 的目标更大：**让一堆对象存储文件具备数据库表的语义。**

单独的 Parquet 只回答：

- 这个文件有哪些列？
- 列怎么压缩？
- 怎么只读某些列？

Iceberg 表格式额外回答：

- 当前这张表由哪些数据文件组成？
- 每次写入后产生了哪个新版本？
- 哪些旧版本还要保留给时间旅行或并发读？
- 哪些行被逻辑删除了？
- schema 或分区规则变了以后，老文件怎么继续可读？
- Spark、Trino、PyIceberg、DuckDB 等外部引擎怎么读同一张表？

因此：

```text
Parquet = 文件格式，解决“一个文件怎么存”
Iceberg = 表格式，解决“很多文件怎么组成一张可靠的表”
pg_lake = PostgreSQL 接入层，解决“用户怎么用 SQL 操作这张湖表”
DuckDB = 执行引擎，解决“大量文件怎么读写得快”
```

---

## 3. 整体架构：三层分工

pg_lake 的核心不是把所有能力塞进 PostgreSQL，而是拆成三层。

| 层 | 主要组件 | 责任 |
|---|---|---|
| SQL 与事务层 | PostgreSQL、`pg_lake_table`、`pg_lake_iceberg`、`pg_lake_engine` | 接收 SQL、规划查询、维护本地目录、协调事务、提交 Iceberg 元数据 |
| 向量化执行层 | `pgduck_server`、DuckDB、`duckdb_pglake` | 高速扫描文件、写 Parquet、处理对象存储、缓存、类型兼容 |
| 表格式与存储层 | Iceberg metadata、manifest、data file、delete file、对象存储 | 保存真正的数据文件和跨引擎可读的表状态 |

### 3.1 用户看到的是 PostgreSQL

用户连接 PostgreSQL，不直接连接 DuckDB：

```sql
SELECT * FROM lake_table WHERE dt = DATE '2026-06-26';
INSERT INTO lake_table VALUES (...);
DELETE FROM lake_table WHERE id = 100;
```

PostgreSQL 仍然负责：

- SQL 入口；
- 权限；
- 事务边界；
- planner/executor；
- 不适合下推时的本地回退执行。

### 3.2 DuckDB 负责“重活”

大数据分析通常要扫描大量列式文件。DuckDB 擅长：

- `read_parquet(...)`；
- filter/projection pushdown；
- Parquet/CSV/JSON 读写；
- 对象存储访问；
- 向量化执行。

pg_lake 不把 DuckDB 直接嵌进 PostgreSQL 后端进程，而是通过 `pgduck_server` 隔离出来。这样可以降低线程模型、内存模型、崩溃影响等风险，代价是多了一层 libpq/PostgreSQL wire 协议边界。

### 3.3 Iceberg 负责“表的正确性和互操作”

Iceberg 不执行 SQL，它保存表状态：

- metadata JSON；
- snapshot；
- manifest list；
- manifest；
- data file；
- position delete file；
- schema/partition evolution；
- catalog pointer。

如果没有 Iceberg，pg_lake 仍可扫文件，但会失去标准湖表语义、跨引擎互操作、版本管理和删除语义。

---

## 4. SELECT 查询从头到尾怎么走

假设用户执行：

```sql
SELECT name, amount
FROM sales
WHERE dt = DATE '2026-06-26' AND amount > 100;
```

### 第 1 步：PostgreSQL 解析 SQL

PostgreSQL 先把 SQL 解析成内部语法树，然后 planner 开始找执行方案。pg_lake 通过 FDW callback 和 planner hook 参与这个过程。

### 第 2 步：判断能不能下推

pg_lake 会判断：

- 查询是不是只涉及 lake 表；
- 表达式 DuckDB 是否支持；
- 函数、类型、操作符语义是否可安全映射；
- 是否需要 PostgreSQL 本地处理一部分逻辑。

结果有两种：

| 情况 | 执行方式 |
|---|---|
| 整个查询都能下推 | 走 CustomScan，全查询推给 DuckDB |
| 只能部分下推 | 走 ForeignScan，DuckDB 做文件扫描，PostgreSQL 处理剩余逻辑 |

### 第 3 步：在执行开始时创建 scan snapshot

pg_lake 不在规划阶段就固定文件列表，而是在 executor start 时基于当前事务快照创建扫描快照。这样能让查询看到事务内一致的表状态。

### 第 4 步：发现文件并裁剪文件

pg_lake 找出这张表当前 snapshot 下的数据文件，再根据 WHERE 条件做文件裁剪：

- 分区值不可能匹配的文件跳过；
- min/max 统计不可能匹配的文件跳过；
- `_filename` 谓词可裁剪路径；
- position delete 需要参与读时过滤。

这一步非常关键，因为对象存储里可能有成千上万个文件。少读一个文件，就少一次 I/O。

### 第 5 步：生成 DuckDB SQL

pg_lake 把 lake 表访问改写成类似：

```sql
SELECT name, amount
FROM read_parquet([...], filename=true, file_row_number=true)
WHERE dt = DATE '2026-06-26' AND amount > 100;
```

如果有 position delete，还会加 anti-join 逻辑，大意是：

```sql
读取数据文件
  排除 delete file 中记录的 (文件路径, 行号)
```

### 第 6 步：pgduck_server 执行并流式返回

PostgreSQL backend 通过 libpq 连接 `pgduck_server`，发送改写后的 SQL。DuckDB 扫 Parquet 并返回文本行，pg_lake 再把文本转回 PostgreSQL tuple slot。

---

## 5. INSERT / UPDATE / DELETE 怎么走

传统 OLTP 数据库常常原地更新行。湖表通常不这么做，因为对象存储和 Parquet 更适合追加大文件，而不是随机改某一行。

### 5.1 INSERT：写新数据文件

INSERT 的大意是：

```text
接收用户行
  -> 写入临时 staging
  -> DuckDB COPY 成 Parquet 数据文件
  -> pg_lake 记录新增 data file
  -> 提交时写入 Iceberg metadata
```

### 5.2 DELETE：不一定立刻重写大文件

删除一行时，pg_lake 需要知道这行来自哪个文件、文件内第几行。它使用隐藏的行身份：

```text
_pg_lake_filename + file_row_number -> 合成 ctid
```

然后有三种策略：

| 删除情况 | 策略 | 小白解释 |
|---|---|---|
| 整个文件都被删 | 只从元数据移除文件引用 | 不搬书，只改目录 |
| 删除少量行 | MOR，写 position delete | 不改原书，贴一张“这些页作废”的纸 |
| 删除很多行 | COW，重写数据文件 | 旧书太多页作废，直接重印一本干净的 |

### 5.3 UPDATE = DELETE + INSERT

湖表里 UPDATE 通常可以理解为：

```text
把旧行标记删除
  + 写入一行新数据
```

这就是为什么 UPDATE 路径同时涉及 delete file 和 insert data file。

### 5.4 MOR 为什么快，为什么又需要 VACUUM

MOR（Merge-On-Read）快在写入端：删除少量行时只写小 delete file，不重写大 Parquet 文件。

但读的时候要额外应用 delete file，所以 delete file 积累多了会拖慢查询。VACUUM/compaction 的作用就是：

- 合并小文件；
- 把 delete file 吸收到重写后的数据文件里；
- 过期旧 snapshot；
- 清理不再引用的对象存储文件；
- 合并 manifest。

---

## 6. Catalog 到底是什么

Iceberg 每次提交都会生成新的 metadata JSON。问题是：**读者怎么知道哪一个 metadata JSON 是当前最新版本？**

Catalog 就是这个答案。

```text
catalog.namespace.table
  -> 当前 metadata-location
  -> metadata JSON
  -> 当前 snapshot
  -> manifest list
  -> manifests
  -> data/delete files
```

Catalog 至少要提供：

- 表名到 metadata-location 的映射；
- 原子提交能力；
- 表创建、删除、更新；
- 可选的 REST Catalog 协议互操作。

pg_lake 里同时有几类 catalog 相关能力：

| 能力 | 说明 |
|---|---|
| PostgreSQL 本地 catalog | `lake_iceberg.tables_internal`、`tables_external`、`pg_catalog.iceberg_tables` |
| REST Catalog 桥接 | 对接外部 Iceberg REST Catalog |
| Object-store catalog | 把指针导出为对象存储中的 catalog JSON |
| 元数据检查函数 | `metadata`、`files`、`snapshots`、`data_file_stats` |
| 事务 hook | 在 PRE_COMMIT/COMMIT/ABORT 时协调元数据变更 |

---

## 7. pg_lake 和普通 OLTP 数据库有什么不同

| 维度 | pg_lake/Iceberg | 普通 OLTP 数据库 |
|---|---|---|
| 核心目标 | 大规模分析、对象存储、跨引擎互操作 | 高频小事务、点查、在线业务 |
| 存储方式 | 不可变数据文件 + 元数据提交 | 行存储页、索引、WAL、MVCC |
| 更新方式 | delete file 或重写文件 | 原地版本链/行级修改 |
| 查询优势 | 列式扫描、文件裁剪、批量分析 | 索引点查、低延迟事务 |
| 并发写 | 更偏表级/元数据级协调 | 行锁、页级/行级 MVCC |
| 维护任务 | compaction、snapshot expire、manifest merge | vacuum、checkpoint、索引维护 |

所以不要把 pg_lake 当成“OLTP 替代品”。它的价值是把 PostgreSQL SQL 体验带到 lakehouse 分析场景。

---

## 8. Iceberg 和 Lance 怎么比

| 维度 | Iceberg | Lance |
|---|---|---|
| 主要定位 | 通用 lakehouse 表格式 | AI/ML 向量和多模态数据格式 |
| 文件生态 | Parquet/ORC/Avro 等 | Lance 原生格式 |
| 核心能力 | snapshot、manifest、catalog、schema/partition evolution、delete file | 向量检索、fragment、row lineage、嵌入数据管理 |
| 生态互操作 | Spark、Flink、Trino、PyIceberg 等 | LanceDB 和 AI 数据栈 |
| 对 pg_lake/GaussDB 的启发 | 更适合作为通用分析湖表基线 | 适合作为向量/AI 工作负载补充评估 |

---

## 9. 对 GaussDB 评估最关键的问题

如果要把类似 pg_lake 的能力放到 GaussDB 里，优先评估这些问题：

1. GaussDB 有没有等价的 FDW、planner hook、executor hook 或 table access method 接入点？
2. DuckDB 应该外置、内嵌，还是替换成自研/现有向量化执行引擎？
3. 本地 catalog 和 Iceberg metadata 如何保持一致？
4. COMMIT、ABORT、crash recovery、REST catalog 失败时怎么恢复？
5. 写并发采用表级串行化，还是实现 Iceberg 乐观并发重试？
6. 对象存储凭证放在哪里，谁能访问执行服务？
7. 类型语义怎么对齐：时间、decimal、数组、JSON、geometry、collation、NaN/Infinity？
8. 如何做兼容性测试：Spark、Trino、PyIceberg、DuckDB 是否都能读？
9. VACUUM/compaction/snapshot expire 是否是一等能力，而不是后期补丁？
10. 如何观测和定位跨 PostgreSQL/GaussDB、执行引擎、对象存储、catalog 的问题？

---

## 10. 从 0 到源码的学习路线

建议按这个顺序读，避免一上来陷进源码细节。

### 第 1 天：只理解大图

- 数据库、SQL、表、事务；
- Parquet 是文件格式；
- Iceberg 是表格式；
- DuckDB 是执行引擎；
- pg_lake 是 PostgreSQL 到 lakehouse 的桥。

验收问题：

- 为什么 Parquet 不等于 Iceberg？
- 为什么用户连 PostgreSQL，而不是直接连 DuckDB？
- Catalog 为什么需要原子提交？

### 第 2 天：理解 SELECT

- PostgreSQL planner；
- FDW 与 CustomScan；
- 文件发现；
- 分区裁剪和统计裁剪；
- DuckDB `read_parquet`；
- position delete 读时过滤。

验收问题：

- 为什么 pg_lake 要延迟到 executor start 绑定文件？
- 什么情况下不能全查询下推？
- delete file 为什么会影响 SELECT？

### 第 3 天：理解写入和删除

- INSERT 写 data file；
- DELETE 写 position delete 或移除 data file；
- UPDATE = DELETE + INSERT；
- COW/MOR 权衡；
- VACUUM/compaction。

验收问题：

- MOR 写入为什么快？
- MOR 读多了为什么会变慢？
- COW 什么时候更划算？

### 第 4 天：理解提交和 Catalog

- PostgreSQL 事务 hook；
- PRE_COMMIT 生成 metadata；
- COMMIT 发送 REST catalog 请求；
- ABORT 清理；
- object-store catalog；
- 失败恢复风险。

验收问题：

- metadata JSON、manifest、snapshot 各自是什么？
- REST catalog 失败为什么危险？
- 本地 catalog 和 Iceberg metadata 为什么可能不一致？

### 第 5 天：开始读源码

优先读这些文件：

- `D:/pg_lake/pg_lake_iceberg/pg_lake_iceberg--3.0.sql`
- `D:/pg_lake/pg_lake_iceberg/pg_lake_iceberg--3.3--3.4.sql`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/catalog.c`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/metadata_operations.c`
- `D:/pg_lake/pg_lake_iceberg/src/rest_catalog/rest_catalog.c`
- `D:/pg_lake/pg_lake_iceberg/src/object_store_catalog/object_store_catalog.c`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/external_metadata_modification.c`
- `D:/pg_lake/pg_lake_iceberg/src/init.c`
- `D:/pg_lake/pg_lake_engine/include/pg_lake/util/catalog_type.h`
- `D:/pg_lake/pg_lake_engine/src/utils/catalog_type.c`
- `D:/pg_lake/pg_lake_table/src/transaction/transaction_hooks.c`
- `D:/pg_lake/pg_lake_table/src/transaction/track_iceberg_metadata_changes.c`
- `D:/pg_lake/docs/iceberg-tables.md`
- `D:/pg_lake/pg_lake_iceberg/tests/pytests/test_iceberg_functions.py`
- `D:/pg_lake/pg_lake_table/tests/pytests/test_object_store_catalog.py`
- `D:/pg_lake/pg_lake_table/tests/pytests/test_polaris_catalog.py`
- `D:/pg_lake/pg_lake_table/tests/pytests/test_polaris_catalog_writable.py`
- `D:/pg_lake/pg_lake_table/tests/pytests/test_modify_iceberg_rest_table.py`
- `D:/pg_lake/pg_lake_table/tests/pytests/test_iceberg_catalog_server.py`
- `D:/pg_lake/README.md`
- `D:/pg_lake/pg_lake_table/pg_lake_table--3.0.sql`
- `D:/pg_lake/pg_lake_table/src/fdw/writable_table.c`
- `D:/pg_lake/pg_lake_engine/src/pgduck/read_data.c`
- `D:/pg_lake/pg_lake_engine/src/pgduck/write_data.c`
- `D:/pg_lake/pg_lake_iceberg/include/pg_lake/iceberg/api/datafile.h`
- `D:/pg_lake/docs/file-formats-reference.md`
- `D:/pg_lake/docs/dbt.md`
- `D:/pg_lake/pg_lake_iceberg/pg_lake_iceberg--3.0--3.1.sql`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/read_table_metadata.c`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/read_manifest.c`
- `D:/pg_lake/pg_lake_iceberg/src/iceberg/api/table_metadata.c`
- `D:/pg_lake/pg_lake_table/src/fdw/snapshot.c`
- `D:/pg_lake/pg_lake_table/src/fdw/data_file_pruning.c`
- `D:/pg_lake/pg_lake_table/src/fdw/position_delete_dest.c`
- `D:/pg_lake/pg_lake_table/src/ddl/ddl_changes.c`
- `D:/pg_lake/pg_lake_iceberg/tests/pytests/test_iceberg_metadata_via_spark.py`

---

## 11. 关键源码导航

| 想理解什么 | 优先看哪里 |
|---|---|
| 扩展加载和 GUC | `pg_lake_engine/src/init.c`、`pg_lake_table/src/init.c`、`pg_lake_iceberg/src/init.c` |
| FDW 扫描和写入 | `pg_lake_table/src/fdw/pg_lake_table.c` |
| SELECT 快照和文件发现 | `pg_lake_table/src/fdw/snapshot.c` |
| 文件裁剪 | `pg_lake_table/src/fdw/data_file_pruning.c` |
| IUD 与 COW/MOR | `pg_lake_table/src/fdw/writable_table.c` |
| position delete 写入 | `pg_lake_table/src/fdw/position_delete_dest.c` |
| 全查询下推 | `pg_lake_table/src/planner/query_pushdown.c` |
| 事务 hook | `pg_lake_table/src/transaction/transaction_hooks.c` |
| Iceberg 元数据提交 | `pg_lake_iceberg/src/iceberg/metadata_operations.c` |
| REST Catalog | `pg_lake_iceberg/src/rest_catalog/rest_catalog.c` |
| pgduck 客户端 | `pg_lake_engine/src/pgduck/client.c` |
| DuckDB 读写 SQL 生成 | `pg_lake_engine/src/pgduck/read_data.c`、`write_data.c`、`delete_data.c` |
| pgduck_server | `pgduck_server/src/main.c`、`pgduck_server/src/pgsession/pgsession.c`、`pgduck_server/src/duckdb/duckdb.c` |
| DuckDB 扩展与对象存储 | `duckdb_pglake/src/duckdb_pglake_extension.cpp`、`duckdb_pglake/src/fs/functions.cpp` |

---

## 12. 外部官方资料

- <https://iceberg.apache.org/spec/>
- <https://iceberg.apache.org/spec/#optimistic-concurrency>
- <https://iceberg.apache.org/spec/#table-metadata>
- <https://iceberg.apache.org/spec/#metastore-tables>
- <https://iceberg.apache.org/rest-catalog-spec/>
- <https://raw.githubusercontent.com/apache/iceberg/main/open-api/rest-catalog-open-api.yaml>
- <https://parquet.apache.org/docs/file-format/>
- <https://www.postgresql.org/docs/current/transaction-iso.html>
- <https://www.postgresql.org/docs/current/mvcc.html>
- <https://lance.org/format/>
- <https://lance.org/format/file/>
- <https://lance.org/format/table/>
- <https://iceberg.apache.org/docs/latest/>
- <https://iceberg.apache.org/docs/latest/evolution/>
- <https://www.postgresql.org/docs/current/fdw-callbacks.html>
- <https://www.postgresql.org/docs/current/custom-scan.html>
- <https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml>
- <https://duckdb.org/docs/current/data/parquet/overview>
- <https://duckdb.org/docs/current/core_extensions/httpfs/s3api>
- <https://duckdb.org/docs/current/configuration/secrets_manager>

额外建议优先读：

- PostgreSQL FDW callback：<https://www.postgresql.org/docs/current/fdwhandler.html>
- PostgreSQL Custom Scan：<https://www.postgresql.org/docs/current/custom-scan.html>
- Apache Iceberg Spec：<https://iceberg.apache.org/spec/>
- Apache Iceberg REST Catalog：<https://iceberg.apache.org/rest-catalog-spec/>
- DuckDB Parquet：<https://duckdb.org/docs/current/data/parquet/overview.html>

---

## 13. 一页复盘

pg_lake 做的是一件“缝合但有清晰边界”的事：

- PostgreSQL 负责用户入口、SQL 语义、事务和权限；
- pg_lake 扩展负责把 SQL、文件、Iceberg 元数据和事务串起来；
- DuckDB 负责高性能文件扫描和写入；
- Iceberg 负责标准湖表元数据；
- 对象存储负责持久化数据文件和元数据文件；
- Catalog 负责告诉所有引擎当前表版本在哪里。

这套系统最大的收益是：用户可以用 PostgreSQL 体验操作 lakehouse 数据，外部引擎也能通过 Iceberg 读取同一张表。

最大的复杂度是：提交时必须同时考虑本地 catalog、对象存储文件、Iceberg metadata、REST/object-store catalog、事务成功/失败和后续清理。
