import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"


def collect_sources():
    local = []
    external = []
    for path in sorted(RESULTS.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        for item in data.get("source_links", []):
            if item.startswith("http"):
                external.append(item)
            elif item.startswith("D:/") or item.startswith("D:\\"):
                local.append(item.replace("\\", "/"))
    seen_local = []
    for item in local:
        if item not in seen_local:
            seen_local.append(item)
    seen_external = []
    for item in external:
        if item not in seen_external:
            seen_external.append(item)
    return seen_local, seen_external


LOCAL_SOURCES, EXTERNAL_SOURCES = collect_sources()


def md_list(items, limit=None):
    selected = items if limit is None else items[:limit]
    return "\n".join(f"- `{item}`" for item in selected)


def link_list(items):
    return "\n".join(f"- <{item}>" for item in items)


REPORT = f"""# pg_lake + Iceberg 小白入门调研 Wiki

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

{md_list(LOCAL_SOURCES, limit=36)}

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

{link_list(EXTERNAL_SOURCES)}

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
"""


HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>pg_lake + Iceberg 小白入门 Wiki</title>
<style>
:root {
  --bg:#f6f7f9;
  --panel:#ffffff;
  --ink:#17202a;
  --muted:#5f6b7a;
  --line:#d9dee7;
  --blue:#2563eb;
  --teal:#0f766e;
  --green:#16803c;
  --amber:#b45309;
  --red:#b42318;
  --violet:#6d28d9;
  --code:#111827;
}
* { box-sizing:border-box; }
html { scroll-behavior:smooth; }
body {
  margin:0;
  font-family:"Segoe UI", "Microsoft YaHei", system-ui, sans-serif;
  color:var(--ink);
  background:var(--bg);
  line-height:1.65;
  overflow-x:hidden;
}
a { color:var(--blue); text-decoration:none; }
a:hover { text-decoration:underline; }
.layout { display:grid; grid-template-columns:260px minmax(0,1fr); min-height:100vh; max-width:100vw; overflow-x:hidden; }
aside {
  position:sticky;
  top:0;
  height:100vh;
  min-width:0;
  padding:22px 18px;
  border-right:1px solid var(--line);
  background:#fff;
  overflow:auto;
}
.brand { font-size:18px; font-weight:750; line-height:1.25; margin-bottom:8px; }
.version { color:var(--muted); font-size:12px; margin-bottom:20px; }
nav a {
  display:block;
  padding:8px 10px;
  border-radius:6px;
  color:#334155;
  font-size:14px;
}
nav a:hover { background:#eef2ff; text-decoration:none; }
main { width:100%; min-width:0; max-width:1120px; padding:34px 34px 80px; }
section { margin:0 0 44px; scroll-margin-top:20px; }
.hero {
  min-height:64vh;
  display:grid;
  align-content:center;
  border-bottom:1px solid var(--line);
  margin-bottom:36px;
  padding-bottom:28px;
}
h1 {
  max-width:1020px;
  font-size:clamp(32px,4.2vw,50px);
  line-height:1.08;
  margin:0 0 18px;
  letter-spacing:0;
  overflow-wrap:anywhere;
}
h2 { font-size:26px; margin:0 0 16px; letter-spacing:0; }
h3 { font-size:18px; margin:22px 0 8px; letter-spacing:0; }
.lead { max-width:820px; color:#334155; font-size:18px; margin:0 0 26px; }
.pillrow { display:flex; flex-wrap:wrap; gap:10px; }
.pill {
  display:inline-flex;
  align-items:center;
  border:1px solid var(--line);
  border-radius:999px;
  padding:6px 10px;
  background:#fff;
  font-size:13px;
  color:#334155;
}
.grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:14px; }
.two { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }
.card {
  background:var(--panel);
  border:1px solid var(--line);
  border-radius:8px;
  padding:16px;
}
.card h3 { margin-top:0; }
.muted { color:var(--muted); }
.diagram {
  background:#fff;
  border:1px solid var(--line);
  border-radius:8px;
  padding:18px;
  overflow:auto;
}
.flow {
  display:grid;
  grid-template-columns:repeat(6,minmax(120px,1fr));
  gap:10px;
}
.node {
  min-height:92px;
  border:1px solid var(--line);
  border-radius:8px;
  background:#fff;
  padding:12px;
  position:relative;
}
.node strong { display:block; margin-bottom:6px; }
.node:not(:last-child)::after {
  content:"";
  position:absolute;
  right:-10px;
  top:45px;
  width:10px;
  height:2px;
  background:#94a3b8;
}
.blue { border-top:4px solid var(--blue); }
.teal { border-top:4px solid var(--teal); }
.green { border-top:4px solid var(--green); }
.amber { border-top:4px solid var(--amber); }
.red { border-top:4px solid var(--red); }
.violet { border-top:4px solid var(--violet); }
table { width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); border-radius:8px; overflow:hidden; }
th,td { border:1px solid var(--line); padding:10px 12px; vertical-align:top; }
th { background:#f1f5f9; text-align:left; }
code, pre {
  font-family:"Cascadia Code", Consolas, monospace;
}
pre {
  background:var(--code);
  color:#eef2ff;
  border-radius:8px;
  padding:14px;
  overflow:auto;
  font-size:13px;
}
.callout {
  border-left:4px solid var(--blue);
  background:#eff6ff;
  padding:12px 14px;
  border-radius:0 8px 8px 0;
  margin:14px 0;
}
.tabs {
  display:flex;
  flex-wrap:wrap;
  gap:8px;
  margin:12px 0;
}
button.tab {
  border:1px solid var(--line);
  border-radius:6px;
  background:#fff;
  color:#334155;
  padding:8px 12px;
  cursor:pointer;
  font:inherit;
}
button.tab.active {
  border-color:var(--blue);
  color:#fff;
  background:var(--blue);
}
.tabpane { display:none; }
.tabpane.active { display:block; }
.checklist li { margin:8px 0; }
.path-list {
  columns:2;
  column-gap:26px;
  font-size:13px;
}
.path-list li { break-inside:avoid; margin-bottom:6px; }
footer {
  margin-top:60px;
  padding-top:18px;
  border-top:1px solid var(--line);
  color:var(--muted);
  font-size:13px;
}
@media (max-width: 920px) {
  .layout { grid-template-columns:1fr; }
  aside { position:relative; height:auto; border-right:0; border-bottom:1px solid var(--line); }
  main { width:100%; max-width:100%; padding:24px 16px 60px; }
  section, .hero { width:calc(100vw - 32px); max-width:calc(100vw - 32px); }
  h1 { width:calc(100vw - 32px); max-width:calc(100vw - 32px); font-size:30px; line-height:1.12; word-break:break-all; }
  .lead { width:calc(100vw - 32px); max-width:calc(100vw - 32px); font-size:16px; overflow-wrap:anywhere; }
  .grid,.two,.flow { grid-template-columns:1fr; }
  .node:not(:last-child)::after { display:none; }
  .path-list { columns:1; }
}
</style>
</head>
<body>
<div class="layout">
<aside>
  <div class="brand">pg_lake + Iceberg<br>小白入门 Wiki</div>
  <div class="version">从数据库零基础到源码阅读路径</div>
  <nav>
    <a href="#goal">我们要干什么</a>
    <a href="#concepts">基础概念</a>
    <a href="#architecture">三层架构</a>
    <a href="#select">SELECT 流程</a>
    <a href="#write">写入与删除</a>
    <a href="#catalog">Catalog</a>
    <a href="#compare">对比理解</a>
    <a href="#gaussdb">GaussDB 评估</a>
    <a href="#roadmap">学习路线</a>
    <a href="#sources">来源索引</a>
  </nav>
</aside>
<main>
  <section class="hero" id="goal">
    <h1>把数据湖文件变成可查、可写、可互操作的表</h1>
    <p class="lead">pg_lake 的核心目标不是“再造一个数据库”，而是让 PostgreSQL 继续做 SQL、事务和权限入口，让 DuckDB 快速读写对象存储文件，让 Iceberg 保存标准湖表元数据。</p>
    <div class="pillrow">
      <span class="pill">PostgreSQL：入口和事务</span>
      <span class="pill">pg_lake：桥接层</span>
      <span class="pill">DuckDB：文件执行引擎</span>
      <span class="pill">Iceberg：表格式</span>
      <span class="pill">对象存储：数据落地</span>
    </div>
  </section>

  <section id="concepts">
    <h2>1. 基础概念先分清</h2>
    <div class="grid">
      <div class="card blue"><h3>文件格式</h3><p>Parquet/CSV/JSON 解决“一个文件怎么存”。Parquet 是列式格式，适合分析扫描。</p></div>
      <div class="card teal"><h3>表格式</h3><p>Iceberg 解决“很多文件怎么组成一张表”。它管理 snapshot、manifest、schema 演进和删除语义。</p></div>
      <div class="card green"><h3>执行引擎</h3><p>DuckDB 负责快速读写文件，尤其适合列式扫描、filter pushdown 和批量计算。</p></div>
      <div class="card amber"><h3>数据库入口</h3><p>PostgreSQL 接收 SQL，负责事务、权限、规划和不能下推时的本地执行。</p></div>
      <div class="card red"><h3>Catalog</h3><p>Catalog 把表名映射到当前 metadata 文件，并提供原子提交点。</p></div>
      <div class="card violet"><h3>Pushdown</h3><p>把适合 DuckDB 的扫描和计算推下去，减少 PostgreSQL 一行行处理的成本。</p></div>
    </div>
    <div class="callout">最容易混淆的一点：Parquet 不是 Iceberg。Parquet 是“书页怎么印”，Iceberg 是“目录、版本和哪些书页属于这张表”。</div>
  </section>

  <section id="architecture">
    <h2>2. 三层架构</h2>
    <div class="diagram">
      <div class="flow">
        <div class="node blue"><strong>用户 SQL</strong><span>SELECT / INSERT / UPDATE / DELETE</span></div>
        <div class="node amber"><strong>PostgreSQL</strong><span>解析、规划、权限、事务</span></div>
        <div class="node teal"><strong>pg_lake 扩展</strong><span>FDW、planner hook、metadata bridge</span></div>
        <div class="node green"><strong>pgduck_server</strong><span>隔离 DuckDB 执行进程</span></div>
        <div class="node green"><strong>DuckDB</strong><span>read_parquet / COPY / 对象存储 I/O</span></div>
        <div class="node violet"><strong>Iceberg + 存储</strong><span>metadata、manifest、data/delete files</span></div>
      </div>
    </div>
    <table>
      <tr><th>层</th><th>组件</th><th>职责</th></tr>
      <tr><td>SQL 与事务层</td><td>PostgreSQL、pg_lake_table、pg_lake_iceberg</td><td>接收 SQL、规划查询、协调事务、提交 Iceberg 元数据</td></tr>
      <tr><td>执行层</td><td>pgduck_server、DuckDB、duckdb_pglake</td><td>高速文件扫描、写 Parquet、对象存储访问、缓存和类型兼容</td></tr>
      <tr><td>表格式与存储层</td><td>Iceberg metadata、manifest、对象存储</td><td>保存标准湖表状态和真实数据文件</td></tr>
    </table>
  </section>

  <section id="select">
    <h2>3. SELECT 查询怎么跑</h2>
    <div class="tabs" data-tabs="select-tabs">
      <button class="tab active" data-tab="s1">1 规划</button>
      <button class="tab" data-tab="s2">2 快照</button>
      <button class="tab" data-tab="s3">3 裁剪</button>
      <button class="tab" data-tab="s4">4 DuckDB</button>
      <button class="tab" data-tab="s5">5 返回</button>
    </div>
    <div id="s1" class="tabpane active card"><h3>规划阶段</h3><p>pg_lake 用 FDW callback 和 planner hook 判断哪些 SQL 可以下推。如果全查询可下推，走 CustomScan；否则走 ForeignScan 加 PostgreSQL 本地处理。</p></div>
    <div id="s2" class="tabpane card"><h3>创建扫描快照</h3><p>执行开始时根据 PostgreSQL 事务快照确定当前可见的表版本，避免查询看到不一致的文件列表。</p></div>
    <div id="s3" class="tabpane card"><h3>文件发现和裁剪</h3><p>从本地 catalog 或 Iceberg metadata 找到当前 data/delete files，再用分区值、min/max 统计和谓词推理跳过不可能命中的文件。</p></div>
    <div id="s4" class="tabpane card"><h3>改写成 DuckDB SQL</h3><p>lake 表会被改写成 <code>read_parquet(...)</code> 等 DuckDB 函数。如果有 position delete，还会生成基于文件名和行号的 anti-join。</p></div>
    <div id="s5" class="tabpane card"><h3>流式返回 PostgreSQL</h3><p>pgduck_server 执行 DuckDB 查询，通过 PostgreSQL wire 协议把文本行返回，pg_lake 再转换成 PostgreSQL tuple。</p></div>
    <pre>SELECT * FROM lake_table WHERE dt = DATE '2026-06-26'
  -> file pruning
  -> read_parquet(candidate_files)
  -> apply position deletes if needed
  -> stream rows back to PostgreSQL</pre>
  </section>

  <section id="write">
    <h2>4. 写入、更新、删除</h2>
    <div class="two">
      <div class="card green">
        <h3>INSERT</h3>
        <p>接收行数据，暂存，再由 DuckDB COPY 成新的 Parquet data files。提交时把新增文件写入 Iceberg metadata。</p>
      </div>
      <div class="card red">
        <h3>DELETE / UPDATE</h3>
        <p>DELETE 记录源文件和文件内行号。UPDATE 可以理解成“删除旧行 + 插入新行”。</p>
      </div>
    </div>
    <table>
      <tr><th>情况</th><th>策略</th><th>解释</th></tr>
      <tr><td>整个文件都删除</td><td>移除 data file 引用</td><td>只改目录，不重写文件</td></tr>
      <tr><td>删除少量行</td><td>MOR / position delete</td><td>写小删除文件，读时过滤</td></tr>
      <tr><td>删除大量行</td><td>COW / 重写文件</td><td>重写干净文件，降低后续读放大</td></tr>
    </table>
    <div class="callout">MOR 写得快，但 delete file 多了会拖慢读；所以 VACUUM/compaction 是湖表系统的核心能力。</div>
  </section>

  <section id="catalog">
    <h2>5. Catalog 是“当前版本指针”</h2>
    <div class="diagram">
      <div class="flow">
        <div class="node blue"><strong>表名</strong><span>catalog.namespace.table</span></div>
        <div class="node amber"><strong>metadata-location</strong><span>当前元数据文件路径</span></div>
        <div class="node violet"><strong>metadata JSON</strong><span>schema、snapshot log</span></div>
        <div class="node teal"><strong>snapshot</strong><span>某一刻的表状态</span></div>
        <div class="node green"><strong>manifest</strong><span>文件清单和统计</span></div>
        <div class="node red"><strong>data/delete files</strong><span>真实数据和删除记录</span></div>
      </div>
    </div>
    <p>pg_lake 在 PostgreSQL 本地 catalog、REST Catalog、object-store catalog 之间做桥接。最难的是提交协调：本地状态、对象存储文件、Iceberg 元数据和外部 catalog 必须尽量保持一致。</p>
  </section>

  <section id="compare">
    <h2>6. 对比理解</h2>
    <div class="two">
      <div>
        <h3>pg_lake/Iceberg vs OLTP</h3>
        <table>
          <tr><th>维度</th><th>pg_lake/Iceberg</th><th>OLTP</th></tr>
          <tr><td>目标</td><td>分析、湖表、互操作</td><td>高频小事务</td></tr>
          <tr><td>存储</td><td>不可变文件 + 元数据</td><td>行存储、索引、WAL</td></tr>
          <tr><td>更新</td><td>delete file 或重写文件</td><td>行级更新/MVCC</td></tr>
        </table>
      </div>
      <div>
        <h3>Iceberg vs Lance</h3>
        <table>
          <tr><th>维度</th><th>Iceberg</th><th>Lance</th></tr>
          <tr><td>定位</td><td>通用 lakehouse 表格式</td><td>AI/向量/多模态数据</td></tr>
          <tr><td>生态</td><td>Spark、Trino、Flink、PyIceberg</td><td>LanceDB 和 AI 数据栈</td></tr>
          <tr><td>适用</td><td>通用分析湖表</td><td>向量检索和特征数据</td></tr>
        </table>
      </div>
    </div>
  </section>

  <section id="gaussdb">
    <h2>7. GaussDB 评估清单</h2>
    <ul class="checklist">
      <li>是否有等价 FDW、planner hook、executor hook 或 table access method 接入点？</li>
      <li>DuckDB 作为外部进程、内嵌库，还是替换成其他向量化引擎？</li>
      <li>本地 catalog 与 Iceberg metadata 如何保持一致？</li>
      <li>COMMIT、ABORT、crash recovery、REST catalog 失败如何恢复？</li>
      <li>写并发走表级串行化，还是实现 Iceberg 乐观并发重试？</li>
      <li>对象存储凭证、执行服务访问权限、审计和观测怎么设计？</li>
      <li>类型语义、时间、decimal、JSON、geometry、collation 如何兼容？</li>
      <li>compaction、snapshot expire、manifest merge 是否作为一等能力建设？</li>
    </ul>
  </section>

  <section id="roadmap">
    <h2>8. 从 0 到源码的 5 天路线</h2>
    <div class="grid">
      <div class="card blue"><h3>第 1 天</h3><p>理解数据库、SQL、表、事务、文件格式、表格式。</p></div>
      <div class="card teal"><h3>第 2 天</h3><p>理解 SELECT：planner、FDW、CustomScan、文件裁剪、read_parquet。</p></div>
      <div class="card green"><h3>第 3 天</h3><p>理解 INSERT/UPDATE/DELETE、MOR、COW、VACUUM。</p></div>
      <div class="card amber"><h3>第 4 天</h3><p>理解 Iceberg metadata、Catalog、PRE_COMMIT、COMMIT、ABORT。</p></div>
      <div class="card violet"><h3>第 5 天</h3><p>按源码导航读关键模块，不从零散函数开始。</p></div>
      <div class="card red"><h3>验收</h3><p>能讲清 Parquet/Iceberg/pg_lake/DuckDB/Catalog 各自边界。</p></div>
    </div>
  </section>

  <section id="sources">
    <h2>9. 来源索引</h2>
    <h3>本地源码与文档</h3>
    <ul class="path-list">__LOCAL_SOURCE_ITEMS__</ul>
    <h3>官方文档</h3>
    <ul>__EXTERNAL_SOURCE_ITEMS__</ul>
  </section>

  <footer>本页面由 `generate_beginner_wiki.py` 根据 `results/*.json` 的深度调研产物重生成。</footer>
</main>
</div>
<script>
document.querySelectorAll("[data-tabs]").forEach(group => {
  group.addEventListener("click", event => {
    const button = event.target.closest("button[data-tab]");
    if (!button) return;
    const id = button.dataset.tab;
    group.querySelectorAll(".tab").forEach(tab => tab.classList.remove("active"));
    button.classList.add("active");
    document.querySelectorAll(".tabpane").forEach(pane => pane.classList.remove("active"));
    document.getElementById(id).classList.add("active");
  });
});
</script>
</body>
</html>
"""


def html_items(items, limit=None, link=False):
    selected = items if limit is None else items[:limit]
    if link:
        return "\n".join(f'<li><a href="{item}">{item}</a></li>' for item in selected)
    return "\n".join(f"<li><code>{item}</code></li>" for item in selected)


def main():
    (ROOT / "report.md").write_text(REPORT, encoding="utf-8", newline="\n")
    html = (
        HTML.replace("__LOCAL_SOURCE_ITEMS__", html_items(LOCAL_SOURCES, limit=48))
        .replace("__EXTERNAL_SOURCE_ITEMS__", html_items(EXTERNAL_SOURCES, link=True))
    )
    (ROOT / "visualization.html").write_text(html, encoding="utf-8", newline="\n")
    print(f"wrote {ROOT / 'report.md'}")
    print(f"wrote {ROOT / 'visualization.html'}")
    print(f"local sources: {len(LOCAL_SOURCES)}, external sources: {len(EXTERNAL_SOURCES)}")


if __name__ == "__main__":
    main()
