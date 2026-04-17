# RMDB 系统目录层（System Catalog）架构文档

> 本文档梳理 `rmdb/src/system/` 的职责、数据结构、对外接口以及典型 DDL/元数据流程。
>
> 目标：让你在动手补 `open_db / close_db / drop_table / create_index / drop_index` 之前，先对这一层的全貌建立清晰认知。

---

## 一、文件一览

```
rmdb/src/system/
├── sm.h            # 对外统一入口头文件，聚合下面三个
├── sm_defs.h       # 公共定义（仅 include defs.h）
├── sm_meta.h       # 元数据类：DbMeta / TabMeta / ColMeta / IndexMeta
├── sm_manager.h    # SmManager 接口声明
├── sm_manager.cpp  # SmManager 实现（DDL + 元数据生命周期）
└── CMakeLists.txt
```

### 文件职责表

| 文件 | 角色 | 关键内容 |
|---|---|---|
| `sm.h` | 统一入口 | 聚合 `sm_manager.h + sm_meta.h + sm_defs.h` |
| `sm_defs.h` | 公共基础 | include `defs.h`，承载公共类型 |
| `sm_meta.h` | **数据结构层** | `ColMeta / IndexMeta / TabMeta / DbMeta` + 流式序列化/反序列化 |
| `sm_manager.h` | **接口层** | `SmManager` 对外 API：DDL、元数据刷盘、库/表生命周期 |
| `sm_manager.cpp` | **实现层** | 上述 API 的真正实现（当前部分是空壳） |

---

## 二、系统目录层在全系统中的位置

```
┌─────────────────────────────────────────────────────┐
│ parser / analyze / optimizer / executor             │
└───────────────┬─────────────────────────────────────┘
                │  需要知道「表/列/索引是否存在 · 元信息」
                ▼
┌─────────────────────────────────────────────────────┐
│               SmManager（系统目录层）                │
│   ┌───────────────────────────────────────────────┐ │
│   │ DbMeta  · TabMeta · ColMeta · IndexMeta        │ │
│   │ fhs_（表记录文件句柄）· ihs_（索引文件句柄）      │ │
│   └───────────────────────────────────────────────┘ │
└───────┬───────────────────────┬─────────────────────┘
        │ DDL 操作               │ 序列化 / 反序列化
        ▼                       ▼
┌──────────────────┐     ┌──────────────────┐
│ RmManager        │     │ <db>.meta 文件    │
│ IxManager        │     │ （文本流式存储）   │
└──────┬───────────┘     └──────────────────┘
       │
       ▼
┌──────────────────────────────────────────────┐
│ BufferPoolManager → DiskManager              │
└──────────────────────────────────────────────┘
```

关键定位：

- **上层**所有算子要用到的表/列/索引信息都从这里查
- **下层**的 `RmManager / IxManager` 做文件级创建和句柄管理
- **自身持久化**走 `<db>.meta` 文本文件

---

## 三、核心数据结构（`sm_meta.h`）

### 3.1 结构关系

```
DbMeta
  ├── name_                    : string
  └── tabs_  : map<string, TabMeta>
                        │
                        ▼
                     TabMeta
                      ├── name
                      ├── cols    : vector<ColMeta>
                      └── indexes : vector<IndexMeta>
                                           │
                                           ▼
                                      IndexMeta
                                       ├── tab_name
                                       ├── col_tot_len
                                       ├── col_num
                                       └── cols : vector<ColMeta>
```

### 3.2 ColMeta

字段元数据：

- `tab_name`
- `name`
- `type`
- `len`
- `offset`：**字段在记录中的字节偏移**
- `index`：未使用，保留字段

### 3.3 IndexMeta

索引元数据：

- `tab_name`
- `col_num`：索引字段数
- `col_tot_len`：所有索引字段长度之和
- `cols`：索引字段元数据序列（顺序就是索引键拼接顺序）

### 3.4 TabMeta

表元数据，提供便捷查询：

- `is_col(col_name)`：判断列是否存在
- `is_index(col_names)`：判断是否已存在包含同样列序列的索引
- `get_index_meta(col_names)`：获取索引迭代器
- `get_col(col_name)`：获取列迭代器（不存在抛 `ColumnNotFoundError`）

### 3.5 DbMeta

数据库元数据，`SmManager` 的主要持有对象：

- `is_table(tab_name)`
- `get_table(tab_name)`：不存在抛 `TableNotFoundError`
- `SetTabMeta(tab_name, meta)`

### 3.6 序列化格式

所有结构体都**重载了 `<<` 和 `>>`**，采用空白分隔的文本流形式。写入顺序：

```
DbMeta:
  <name>\n
  <tabs.size()>\n
  for each tab:
    <TabMeta>\n

TabMeta:
  <name>\n
  <cols.size()>\n
  for each col: <ColMeta>\n
  <indexes.size()>\n
  for each idx: <IndexMeta>\n

ColMeta:
  <tab_name> <name> <type> <len> <offset> <index>

IndexMeta:
  <tab_name> <col_tot_len> <col_num>\n
  for each col: <ColMeta>\n
```

---

## 四、SmManager 的角色与 API

### 4.1 成员

```cpp
class SmManager {
public:
    DbMeta db_;                                            // 当前打开的数据库元数据
    unordered_map<string, unique_ptr<RmFileHandle>> fhs_;  // 表 -> 记录文件句柄
    unordered_map<string, unique_ptr<IxIndexHandle>> ihs_; // 索引名 -> 索引文件句柄
private:
    DiskManager*         disk_manager_;
    BufferPoolManager*   buffer_pool_manager_;
    RmManager*           rm_manager_;
    IxManager*           ix_manager_;
};
```

### 4.2 对外 API 分类

| 分类 | 方法 | 当前状态 |
|---|---|:---:|
| **数据库生命周期** | `create_db` | ✅ |
|  | `drop_db` | ✅ |
|  | `open_db` | ❌ 空壳 |
|  | `close_db` | ❌ 空壳 |
| **元数据持久化** | `flush_meta` | ✅ |
| **表 DDL** | `create_table` | ✅ |
|  | `drop_table` | ❌ 空壳 |
|  | `show_tables` | ✅ |
|  | `desc_table` | ✅ |
| **索引 DDL** | `create_index` | ❌ 空壳 |
|  | `drop_index`（两个重载） | ❌ 空壳 |
| **辅助** | `is_dir` | ✅ |
|  | `get_bpm / get_rm_manager / get_ix_manager` | ✅ |

---

## 五、已实现 API 的数据流

### 5.1 `create_db(db_name)`

```
check !is_dir(db_name)     // 否则 DatabaseExistsError
├─ system("mkdir <db_name>")
├─ chdir(<db_name>)
├─ 构造空 DbMeta
├─ ofstream(DB_META_NAME)  -> 写入 DbMeta
├─ disk_manager_->create_file(LOG_FILE_NAME)
└─ chdir("..")
```

### 5.2 `drop_db(db_name)`

```
check is_dir(db_name)      // 否则 DatabaseNotFoundError
└─ system("rm -r <db_name>")
```

### 5.3 `create_table(tab_name, col_defs, ctx)`

```
check !db_.is_table(tab_name)        // 否则 TableExistsError
├─ 计算各列 offset，组装 TabMeta
├─ record_size = 累计 offset
├─ rm_manager_->create_file(tab_name, record_size)
├─ db_.tabs_[tab_name] = tab
├─ fhs_.emplace(tab_name, rm_manager_->open_file(tab_name))
└─ flush_meta()
```

要点：

- **offset 是按列顺序累加而来**，这就是 RmRecord 里每列的物理偏移
- 建表会**立即打开对应记录文件并挂在 `fhs_` 中**，这样执行器可直接用
- 调用 `flush_meta()` 把最新 `DbMeta` 落盘

### 5.4 `flush_meta()`

```
ofstream(DB_META_NAME)     // 默认清空
└─ ofs << db_              // 重载 << 的流式写入
```

### 5.5 `show_tables / desc_table`

- 走 `RecordPrinter` 输出到客户端
- 同时 `show_tables` 会把结果追加写入 `output.txt`（评测脚本读取）

---

## 六、典型 DDL 调用流程

### 6.1 `CREATE TABLE t (...)`

```
SQL 文本
  │
  ▼
Parser → ast::CreateTable
  │
  ▼
Analyze::do_analyze (当前对 CreateTable 无专门语义校验)
  │
  ▼
Planner::do_planner
  └→ DDLPlan(T_CreateTable, tab_name, cols)
  │
  ▼
Portal::run → QlManager::run_mutli_query
  │
  ▼
SmManager::create_table(tab_name, col_defs, ctx)
  ├─ 组装 TabMeta + 计算 offset
  ├─ RmManager::create_file(tab_name, record_size)
  ├─ db_.tabs_[tab_name] = tab
  ├─ fhs_.emplace(tab_name, RmManager::open_file(...))
  └─ flush_meta()
```

### 6.2 `SELECT * FROM t`（触发元数据查询）

```
Analyze::get_all_cols(tab_names)
  └→ sm_manager_->db_.get_table(tab_name).cols
                       │
                       ▼
Planner::generate_select_plan
  └→ ScanPlan(tab_name, conds, ...)
                       │
                       ▼
Executor 构造
  └→ sm_manager_->fhs_.at(tab_name).get()
  └→ sm_manager_->db_.get_table(tab_name)
```

执行器从不直接读磁盘元数据文件，一切都走 `SmManager::db_` 与 `fhs_ / ihs_`。

### 6.3 元数据一致性原则

- **每次改变 schema（建表、删表、建/删索引）后必须 `flush_meta()`**
- **每次打开数据库必须从 `<db>.meta` 重建 `db_ / fhs_ / ihs_`**
- `fhs_ / ihs_` 是**运行期句柄缓存**，生命周期严格对应 `open_db / close_db`

---

## 七、当前空壳函数的语义契约（待实现）

下面是为后续补完时的“行为规约”，补完时严格对照这份语义即可。

### 7.1 `open_db(db_name)`

预期行为：

```
check is_dir(db_name) 否则 DatabaseNotFoundError
├─ chdir(<db_name>)
├─ ifstream(DB_META_NAME) -> db_（反序列化）
├─ 遍历 db_.tabs_：
│     fhs_.emplace(tab_name, rm_manager_->open_file(tab_name))
│     遍历 tab.indexes：
│         idx_name = ix_manager_->get_index_name(tab_name, index.cols)
│         ihs_.emplace(idx_name, ix_manager_->open_index(tab_name, index.cols))
└─ （可选）打开 log 文件
```

### 7.2 `close_db()`

预期行为：

```
flush_meta()
├─ 遍历 ihs_: ix_manager_->close_index(handle)  并 清空 ihs_
├─ 遍历 fhs_: rm_manager_->close_file(handle)   并 清空 fhs_
├─ db_ 清空（name_ 与 tabs_）
└─ chdir("..")
```

### 7.3 `drop_table(tab_name, ctx)`

预期行为：

```
check db_.is_table(tab_name) 否则 TableNotFoundError
├─ 先把该表的所有索引 drop（调用 drop_index 重载）
├─ rm_manager_->close_file(fhs_[tab_name])
├─ rm_manager_->destroy_file(tab_name)
├─ fhs_.erase(tab_name)
├─ db_.tabs_.erase(tab_name)
└─ flush_meta()
```

### 7.4 `create_index(tab_name, col_names, ctx)`

预期行为：

```
check db_.is_table(tab_name)
check !tab.is_index(col_names)  否则 IndexExistsError

├─ 组装 IndexMeta（col_tot_len, col_num, cols 复制自 tab.cols）
├─ ix_manager_->create_index(tab_name, cols)
├─ 打开该索引句柄：ihs_.emplace(idx_name, ix_manager_->open_index(...))
├─ 全表扫描 fhs_[tab_name]：
│     逐记录构造索引键（按 col.offset, col.len 拼接）
│     ihs_[idx_name]->insert_entry(key, rid, txn)
├─ tab.indexes.push_back(index_meta)
└─ flush_meta()
```

### 7.5 `drop_index`（两个重载）

预期行为：

```
check db_.is_table(tab_name)
idx_name = ix_manager_->get_index_name(tab_name, cols)

├─ check ihs_.count(idx_name) 否则 IndexNotFoundError
├─ ix_manager_->close_index(ihs_[idx_name].get())
├─ ix_manager_->destroy_index(tab_name, cols)
├─ ihs_.erase(idx_name)
├─ 从 tab.indexes 中移除对应 IndexMeta
└─ flush_meta()
```

两个重载的差别：

- `vector<string>` 版本：在 `tab.cols` 中查找名字 → 构造 `vector<ColMeta>` → 复用另一个重载
- `vector<ColMeta>` 版本：是真正干活的

---

## 八、与其他模块的交互摘要

| 被调用方 | 用途 |
|---|---|
| `DiskManager` | 判断文件存在、创建日志文件、目录切换依赖 |
| `BufferPoolManager` | 间接使用（通过 rm/ix handle） |
| `RmManager` | 表记录文件的 create / destroy / open / close |
| `IxManager` | 索引文件的 create / destroy / open / close / 索引名构造 |
| `Context` | 参数传入但目前大部分未用（未来用于锁与日志） |
| 调用方 | 被调用 |
|---|---|
| `QlManager::run_mutli_query` | DDL → `create_table/drop_table/create_index/drop_index` |
| `QlManager::run_cmd_utility` | `show_tables / desc_table` |
| `Analyze / Planner / Executor` | 读 `db_`、读 `fhs_ / ihs_` |

---

## 九、常见陷阱提醒

- **目录切换副作用**
  - `create_db / open_db / close_db` 都要 `chdir`，任何一处异常都要确保 `chdir("..")` 被执行（最稳妥做法：RAII 包装）
- **flush_meta 时机**
  - **任何 schema 变更后都要调用一次**，否则重启后状态不一致
- **索引句柄命名**
  - 必须使用 `IxManager::get_index_name(tab_name, cols)` 统一生成，不要自己拼
- **fhs_ / ihs_ 的生命周期**
  - `open_db` 打开，`close_db / drop_table / drop_index` 清理
  - **句柄存在意味着文件在缓冲池中被跟踪**，错乱会直接导致脏页写入错误
- **关闭顺序**
  - 关索引前要 flush 对应缓冲池页，否则崩溃会丢修改
  - 现在 `IxManager::close_index` 内部已处理，但 `RmManager::close_file` 也要检查

---

## 十、一句话总结

- **`sm_meta.h` 定义“元数据长什么样 + 如何序列化”**
- **`sm_manager.cpp` 定义“元数据的生命周期和 DDL 执行”**
- **下一阶段要做的事就是补完 5 个空壳：`open_db / close_db / drop_table / create_index / drop_index`**
- **所有补完的实现都要维持两个不变量：**
  - `db_ / fhs_ / ihs_` 三者始终一致
  - `<db>.meta` 随 schema 变更立刻刷新

