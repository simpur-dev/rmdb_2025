# RMDB 执行层架构文档

> 本文档梳理 `rmdb/src/execution/` 目录下各个文件的职责、执行层的总体框架（**火山模型 / Volcano Iterator**）、以及一条 SQL 从解析完成到结果输出的完整调用流程。
>
> 目标读者：需要接手或扩展执行器的开发者。

---

## 一、目录文件一览

```
rmdb/src/execution/
├── execution.h                   # 统一对外入口头文件
├── execution_defs.h              # 公共定义（仅引入 defs.h / errors.h）
├── execution_common.h            # MVCC 相关辅助函数声明（当前未实现）
│
├── execution_manager.h/.cpp      # QlManager：执行器调度入口
│
├── executor_abstract.h           # AbstractExecutor：所有执行器的基类
│
├── executor_seq_scan.h           # 顺序扫描算子
├── executor_index_scan.h         # 索引扫描算子
├── executor_insert.h             # 插入算子
├── executor_delete.h             # 删除算子
├── executor_update.h             # 更新算子
├── executor_nestedloop_join.h    # 嵌套循环连接算子
├── executor_projection.h         # 投影算子
└── execution_sort.h              # 排序算子（注意文件名前缀是 execution_）
```

### 文件分类

| 类别 | 文件 | 作用 |
|---|---|---|
| **入口** | `execution.h` | 对外唯一头文件，转发 `execution_defs.h` + `execution_manager.h` |
| **调度** | `execution_manager.{h,cpp}` | `QlManager`：接收计划树，分派到 DDL/DML/DQL 处理流程 |
| **框架** | `executor_abstract.h` | 火山模型基类，定义迭代接口契约 |
| **扫描** | `executor_seq_scan.h` | 全表扫描，基于 `RmScan` |
| | `executor_index_scan.h` | 基于 B+ 树的索引扫描 |
| **DML** | `executor_insert.h` | 插入记录 + 维护索引 |
| | `executor_delete.h` | 批量删除 + 清理索引 |
| | `executor_update.h` | 批量更新 + 先删旧索引→改→插新索引 |
| **关系算子** | `executor_nestedloop_join.h` | 嵌套循环连接 |
| | `executor_projection.h` | 列裁剪 |
| | `execution_sort.h` | 排序（阻塞算子） |
| **辅助** | `execution_defs.h` | 公共头 |
| | `execution_common.h` | MVCC 占位 |

---

## 二、核心设计范式：火山模型（Volcano Iterator）

### 2.1 基类契约

`executor_abstract.h` 的 `AbstractExecutor` 定义了所有算子的共同接口：

```cpp
class AbstractExecutor {
   public:
    virtual void   beginTuple();                   // 定位到第一条结果
    virtual void   nextTuple();                    // 推进到下一条
    virtual std::unique_ptr<RmRecord> Next() = 0;  // 取出当前记录（纯虚）
    virtual bool   is_end() const;                 // 是否已无数据
    virtual Rid&   rid() = 0;                      // 当前记录在表中的 Rid

    virtual size_t tupleLen() const;               // 记录长度（字节）
    virtual const std::vector<ColMeta>& cols();    // 输出列的元数据
    virtual std::string getType();                 // 算子类型（调试用）
    virtual ColMeta get_col_offset(const TabCol&); // 用于条件中定位列
};
```

### 2.2 标准调用模式

上层调用者（如 `QlManager::select_from`）按以下模式驱动任何执行器：

```cpp
for (exec->beginTuple(); !exec->is_end(); exec->nextTuple()) {
    auto record = exec->Next();      // 取出当前记录
    // ... 处理 record ...
}
```

### 2.3 算子分两类

| 类型 | 特点 | 代表 |
|---|---|---|
| **流水线（pipeline）** | `beginTuple` O(1)；每条记录按需产出 | SeqScan、IndexScan、Projection、Insert/Delete/Update、NestedLoopJoin |
| **阻塞（blocking）** | `beginTuple` 时先物化所有子记录；O(n) 空间 | Sort（未来的 Aggregate、HashJoin 等） |

---

## 三、整体调用流程

### 3.1 从 SQL 到执行的完整链路

```
SQL 文本
   │
   ▼
┌──────────────┐
│   Parser     │  Lex/Yacc → AST
└──────┬───────┘
       ▼
┌──────────────┐
│   Analyze    │  语义校验 → Query
└──────┬───────┘
       ▼
┌──────────────┐
│  Optimizer   │  Query → LogicalPlan → PhysicalPlan
│  (Planner)   │
└──────┬───────┘
       ▼
┌──────────────┐
│   portal.h   │  convert_plan_executor(): Plan 树 → Executor 树
└──────┬───────┘
       ▼
┌──────────────────────────────────────┐
│          QlManager                   │
│  ┌─────────┐  ┌─────────┐  ┌──────┐ │
│  │ run_    │  │ select_ │  │ run_ │ │
│  │ multi_  │  │  from   │  │ dml  │ │
│  │ query   │  │         │  │      │ │
│  │ (DDL)   │  │ (SELECT)│  │(DML) │ │
│  └─────────┘  └─────────┘  └──────┘ │
└───────────────┬──────────────────────┘
                ▼
     ┌────────────────────┐
     │  Executor Tree     │  火山模型迭代
     └────────────────────┘
                │
                ▼
     ┌────────────────────┐
     │  RmFileHandle      │
     │  IxIndexHandle     │
     └────────────────────┘
                │
                ▼
     ┌────────────────────┐
     │ BufferPoolManager  │
     │ DiskManager        │
     └────────────────────┘
```

### 3.2 `QlManager` 三种分派

`execution_manager.cpp` 按计划类型分派：

| 方法 | 场景 | 实现要点 |
|---|---|---|
| `run_mutli_query(DDLPlan)` | `CREATE/DROP TABLE/INDEX` | 直接调 `SmManager` 对应方法 |
| `run_cmd_utility(OtherPlan)` | `help / show / desc / begin / commit / abort` | 调 `SmManager` 或 `TransactionManager` |
| `select_from(ExecTreeRoot, sel_cols)` | `SELECT` | 驱动火山模型，输出到客户端 + `output.txt` |
| `run_dml(exec)` | `INSERT / DELETE / UPDATE` | 直接 `exec->Next()` 一次（DML 算子内部自己循环） |

---

## 四、各算子详解

### 4.1 扫描类

#### `SeqScanExecutor`（顺序扫描）

**职责**：对一张表从头到尾扫描，按条件过滤。

**关键字段**：
- `tab_name_` / `fh_`：目标表与文件句柄
- `conds_` / `fed_conds_`：过滤条件
- `scan_`：底层 `RmScan` 迭代器

**迭代流程**：
```
beginTuple(): 创建 RmScan → 跳到第一条满足条件的记录
nextTuple():  RmScan::next() → 跳到下一条满足条件的记录
Next():       fh_->get_record(rid_)
is_end():     scan_ == nullptr || scan_->is_end()
```

**过滤逻辑**：`check_conditions` → `check_single_condition` → `compare_values`。

#### `IndexScanExecutor`（索引扫描）

**职责**：利用 B+ 树索引进行精确查找或范围查找。

**关键字段**：
- `ih_`：`IxIndexHandle` 指针
- `scan_`：`IxScan` 迭代器
- `index_meta_`：索引元数据（列、长度）
- `lower_key_` / `upper_key_`：范围边界

**关键点**：
- 构造索引键时必须使用**索引内累加偏移**（非记录中的 `col.offset`）
- 支持 `=`、`>`、`>=`、`<`、`<=`，多个条件取交集作为 `[lower, upper]`

### 4.2 DML 类

所有 DML 算子都**不是迭代器**（不通过 `beginTuple/nextTuple` 驱动），而是**一次性执行**：
- `QlManager::run_dml(exec)` 只调用 `exec->Next()` 一次
- 算子内部自己循环处理所有 `rids_`

#### `InsertExecutor`
```
Next():
  for each value group:
    构造 RmRecord → fh_->insert_record → 得到 rid_
    for each index: 构造 key → ih->insert_entry(key, rid_)
  return nullptr
```

#### `DeleteExecutor`
```
Next():
  for each rid in rids_:
    rec = fh_->get_record(rid)
    for each index: 构造 key → ih->delete_entry(key)
    fh_->delete_record(rid)
  return nullptr
```

#### `UpdateExecutor`
```
Next():
  for each rid in rids_:
    rec = fh_->get_record(rid)
    for each index: 删除旧 key   ← 关键：先删旧索引
    apply set_clauses_ to rec    ← 再改记录
    fh_->update_record(rid, rec)
    for each index: 插入新 key   ← 最后插新索引
  return nullptr
```
> **注意顺序**：必须先删旧索引、再改记录、再插新索引。如果先改记录会导致旧键无法重建。

### 4.3 关系算子

#### `NestedLoopJoinExecutor`（嵌套循环连接）

**双层循环结构**：外层左表，内层右表。

**关键字段**：
- `left_` / `right_`：左右子算子
- `fed_conds_`：连接条件
- `left_rec_` / `right_rec_`：**当前左右记录缓存**（避免重复调用子 `Next()`）

**核心辅助函数** `find_next_match()`：统一左右游标推进逻辑。

**迭代流程**：
```
beginTuple():
  left_->beginTuple() + 取 left_rec_
  right_->beginTuple()
  find_next_match()     ← 扫到第一对匹配

nextTuple():
  right_->nextTuple()
  find_next_match()     ← 扫到下一对匹配

find_next_match():
  loop:
    如果 right 未结束: 取 right_rec_；判断条件；不过则 right_->nextTuple()
    如果 right 结束:   left_->nextTuple()；取新 left_rec_；right_->beginTuple()
    直到 left 结束 → isend = true

Next():
  拼接 left_rec_ + right_rec_ → 新 RmRecord
```

**条件判断 `check_join_conds`**：
- `cond.is_rhs_val == true` → 右侧为常量
- 否则右侧为列引用，**先查左表列，再查右表列**（条件的 `rhs_col` 可能来自任一侧）

#### `ProjectionExecutor`（投影）

**流水线透传算子**：

```
beginTuple() → prev_->beginTuple()
nextTuple()  → prev_->nextTuple()
is_end()     → prev_->is_end()

Next():
  child_rec = prev_->Next()
  for 每个投影列: 从 child_rec 按 src_col.offset 拷贝到 proj_rec 的 cols_[i].offset
  return proj_rec
```

**关键**：构造函数里把输出列的 `offset` 重新编排为紧凑排列（不再是原记录的 offset）。

#### `SortExecutor`（排序）

**阻塞物化算子**：

```
beginTuple():
  buffer_.clear()
  // 物化所有子记录
  for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()):
    buffer_.push_back(prev_->Next())
  // 排序下标（而非直接排序记录，省一次移动）
  std::sort(sorted_idx_, lambda: less_than(buffer_[a], buffer_[b]))
  pos_ = 0

nextTuple(): ++pos_
Next():      返回 buffer_[sorted_idx_[pos_]] 的拷贝
is_end():    pos_ >= buffer_.size()
```

**比较函数 `less_than`**：按 `cols_.offset/type/len` 提取排序列数据，按类型比较，根据 `is_desc_` 翻转方向。

---

## 五、典型 SQL 的执行示例

### 5.1 `SELECT name FROM t WHERE id = 5`（有索引）

```
Plan 树（优化器输出）:
  ProjectionPlan(cols=[name])
    └─ IndexScanPlan(tab=t, cond: id=5)

portal.h::convert_plan_executor → Executor 树:
  ProjectionExecutor
    └─ IndexScanExecutor

QlManager::select_from 驱动:

  ProjectionExecutor::beginTuple()
    → IndexScanExecutor::beginTuple()
        → IxIndexHandle::lower_bound(key=5)
        → 定位到第一条 id=5 的记录

  while (!is_end):
    rec = ProjectionExecutor::Next()
      → IndexScanExecutor::Next()
          → fh_->get_record(rid_)
      → 按 sel_idxs_ 抽出 name 列
      → 返回裁剪后的记录

    打印到 output.txt 和客户端

    ProjectionExecutor::nextTuple()
      → IndexScanExecutor::nextTuple()
          → ix_scan_->next()，继续应用过滤
```

### 5.2 `UPDATE t SET score = 100 WHERE id = 5`

```
Plan 树:
  UpdatePlan(rids=[...], set_clauses=[score=100])

portal.h → Executor:
  UpdateExecutor(sm, "t", set_clauses, conds, rids, ctx)

QlManager::run_dml:
  exec->Next()      ← 一次性调用
    for rid in rids_:
      读记录 → 删旧索引 → 改 score → 写回 → 插新索引
```

### 5.3 `SELECT t1.name, t2.age FROM t1, t2 WHERE t1.id = t2.id`

```
Plan 树:
  ProjectionPlan(cols=[t1.name, t2.age])
    └─ NestedLoopJoinPlan(cond: t1.id = t2.id)
         ├─ SeqScanPlan(t1)
         └─ SeqScanPlan(t2)

Executor 树（同构）:
  ProjectionExecutor
    └─ NestedLoopJoinExecutor
         ├─ SeqScanExecutor(t1)
         └─ SeqScanExecutor(t2)

迭代:
  ProjectionExecutor::beginTuple()
    → NestedLoopJoinExecutor::beginTuple()
        → SeqScanExecutor(t1)::beginTuple()
        → 取 left_rec_ (t1 第一行)
        → SeqScanExecutor(t2)::beginTuple()
        → find_next_match(): 扫 t2 直到找到 t1.id = t2.id

  每次 Next():
    NestedLoopJoinExecutor 把 left_rec_ + right_rec_ 拼接 → 投影到 [t1.name, t2.age]
```

---

## 六、开发者须知：扩展新算子的检查清单

1. **继承 `AbstractExecutor`**
2. **必须实现**：
   - `Next()`：纯虚
   - `Rid &rid()`：纯虚
3. **通常需要重写**：
   - `beginTuple() / nextTuple() / is_end()`
   - `tupleLen() / cols() / getType()`
4. **若包含条件过滤**：
   - 重写 `get_col_offset()`（基类默认返回空 `ColMeta`，offset=0 会导致非首列过滤错误）
5. **若维护索引**：
   - 按 `tab_.indexes` 遍历所有索引
   - 使用 `sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)` 获取索引名
   - **构造索引键用索引内累加偏移，不是记录 offset**
6. **若为阻塞算子**（如 Sort、Aggregate）：
   - `beginTuple` 一次性拉取所有子记录
   - 处理好内存管理
7. **集成到 `portal.h`**：
   - 在 `convert_plan_executor` 中添加对应 Plan → Executor 的转换
8. **注册到 `executor_scan_test.cpp` 或新建测试文件**

---

## 七、当前状态与待办

### 7.1 算子完成度

| 算子 | 状态 | 测试 | 备注 |
|---|---|---|---|
| `SeqScanExecutor` | ✅ | 🧪 19 | 条件过滤、大数据、元信息 |
| `IndexScanExecutor` | ✅ | 🧪 19 | 范围/等值/点查、边界 |
| `InsertExecutor` | ✅ | 🧪 5 | 含索引同步、列数/类型校验 |
| `DeleteExecutor` | ✅ | 🧪 5 | 空 rids、选择性删除、索引维护 |
| `UpdateExecutor` | ✅ | 🧪 3 | 修复 init_raw 重复调用 bug |
| `NestedLoopJoinExecutor` | ✅ | 🧪 8 | 双空、笛卡尔积、重复键 |
| `ProjectionExecutor` | ✅ | 🧪 6 | 列重排、大数据、元信息 |
| `SortExecutor` | ✅ | 🧪 8 | 升降序、非首列、重复值 |

### 7.2 已修复的 Bug

| 位置 | Bug | 修复 |
|---|---|---|
| `UpdateExecutor::Next()` | `clause.rhs.init_raw()` 在 `rids_` 循环内重复调用，第二次 `raw != nullptr` 触发 assert | 加 `if (!clause.rhs.raw)` 守卫 |

### 7.3 测试文件

| 文件 | 测试数 | 覆盖范围 |
|---|---|---|
| `executor_scan_test.cpp` | 19 | SeqScan + IndexScan |
| `executor_full_test.cpp` | 40 | 全部 8 个执行器 + 组合流水线 |
| `integration_test.cpp` | 26 | 三层联动（Storage→BufferPool→Record/Index→Execution） |

### 7.4 待办

`execution_common.h` 声明的 MVCC 函数（`ReconstructTuple` / `IsWriteWriteConflict`）是为 MVCC 事务保留的接口，当前未实现，也未被任何算子使用。

---

## 八、参考文件位置

- **基类**：`@rmdb/src/execution/executor_abstract.h`
- **调度**：`@rmdb/src/execution/execution_manager.{h,cpp}`
- **Plan→Executor 转换**：`@rmdb/src/portal.h::convert_plan_executor`
- **底层存取**：`@rmdb/src/record/rm_file_handle.h` / `@rmdb/src/index/ix_index_handle.h`
- **元数据**：`@rmdb/src/system/sm_manager.h` / `@rmdb/src/system/sm_meta.h`
