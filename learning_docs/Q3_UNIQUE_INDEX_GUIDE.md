# 题目三复习文档 — 唯一索引

> 持续更新。每完成一个 Step，就在对应章节加内容。
> 目前进度：**全部 5 个 Step 已完成**，106 个单元测试通过。

---

## 一、题目三需求拆解

官方说明（简化版）：

| 子功能 | 说明 |
|---|---|
| `create index / drop index` | 框架已提供（`SmManager::create_index / drop_index`） |
| `show index from t` | ✅ Step 5 已实现 |
| 索引加速查询 | `IndexScanExecutor` 已提供；可能需要优化器进一步选索引 |
| **唯一性约束** | **核心要求**：重复 key 时服务端输出 `failure`，表/索引不能损坏 |
| 失败处理 | 冲突发生后，服务端继续正常运行（不崩） |

### 关键判分点

```sql
create table t(id int, name char(8));
create index t(id);          -- 给 id 建唯一索引
insert into t values(1, 'a'); -- 成功
insert into t values(1, 'b'); -- 必须输出 failure
insert into t values(2, 'c'); -- 成功
select * from t;              -- 只能有 (1,'a') (2,'c')
```

**本项目里"所有索引都是唯一索引"** —— 没有 UNIQUE 关键字，只要建了索引就不允许重复 key。

---

## 二、全链路工作分解（5 个 Step）

| Step | 内容 | 状态 | 涉及文件 |
|---|---|:---:|---|
| 1 | 新增 `IndexEntryDuplicateError` 异常 | ✅ | `errors.h` |
| 2 | `InsertExecutor` 唯一性 pre-check | ✅ | `executor_insert.h` |
| 3 | `UpdateExecutor` 唯一性 pre-check（含自身豁免） | ✅ | `executor_update.h` |
| 4 | 回归验证：DDL + DML + Index 混合场景 | ✅ | 106 个单元测试全过 |
| 5 | `SHOW INDEX` 全链路实现 | ✅ | ast.h / yacc.y / plan.h / optimizer.h / execution_manager.cpp / sm_manager.h/.cpp |
| 6 | `TabMeta` 拷贝构造函数 bug 修复 | ✅ | `sm_meta.h` |

---

## 三、Step 1 已完成 — `IndexEntryDuplicateError`

### 修改位置
`@/home/simpur/rmdb_2025/db2025/rmdb/src/errors.h`（约第 85 行）

### 代码
```cpp
// B+树里面已经有相同key，不允许再插，抛错
class IndexEntryDuplicateError : public RMDBError {
   public:
    IndexEntryDuplicateError() : RMDBError("Duplicate key violates unique index") {}
};
```

### 为什么这么写就够了

服务端主循环 `@/home/simpur/rmdb_2025/db2025/rmdb/src/rmdb.cpp:154-167`：
```cpp
try {
    ... 执行 SQL ...
} catch (RMDBError &e) {
    memcpy(data_send, "failure\n", ...);
}
```

只要继承 `RMDBError`，**抛出后自动写 `failure`**。业务代码只需要 `throw IndexEntryDuplicateError();`，不用关心输出。

### 继承链

```
std::exception
   └─ std::runtime_error
        └─ RMDBError
             ├─ IndexEntryNotFoundError     ← B+ 树没查到（已有）
             └─ IndexEntryDuplicateError    ← B+ 树重复 key（新增）
```

这俩是对称的一正一反。

---

## 四、Step 2 已完成 — `InsertExecutor` pre-check 改造

### 修改位置
`@/home/simpur/rmdb_2025/db2025/rmdb/src/execution/executor_insert.h`

### 核心思想：pre-check → 真写入 两段式

#### 旧版流程（有隐患）
```
构造 record
    │
    ▼
insert_record          ← ① 数据立刻落盘
    │
    ▼
for 每个索引:
    构造 key
    insert_entry       ← ② 若 B+ 树发现重复 key 这里抛
                         但 ① 的数据已经落盘了！半写入！
```

**灾难场景**：表有 2 个索引，id 索引插成功、name 索引冲突 → 堆文件多一行、id 索引多一个 entry、name 索引无该行。数据库**永久不一致**。

#### 新版流程
```
构造 record
    │
    ▼
for 每个索引:
    构造 key (缓存到 keys[i])
    get_value 查重      ← ① 只读操作，零副作用
    若重复 → throw       ← 抛出时什么都没写，干净
    │
    ▼
insert_record          ← ② 所有 pre-check 通过才写
    │
    ▼
for 每个索引:
    insert_entry       ← ③ 已保证不会重复
```

### 代码要点

```cpp
std::unique_ptr<RmRecord> Next() override {
    // 第一段：构造 record buffer（语义不变）
    RmRecord rec(fh_->get_file_hdr().record_size);
    for (size_t i = 0; i < values_.size(); i++) { ... }

    // 第二段：唯一性 pre-check（新增）
    std::vector<std::unique_ptr<char[]>> keys(tab_.indexes.size());
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto &index = tab_.indexes[i];
        keys[i] = std::make_unique<char[]>(index.col_tot_len);
        char *key = keys[i].get();

        int offset = 0;
        for (size_t k = 0; k < index.col_num; ++k) {
            memcpy(key + offset, rec.data + index.cols[k].offset, index.cols[k].len);
            offset += index.cols[k].len;
        }

        auto ih = sm_manager_->ihs_.at(
            sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        std::vector<Rid> out;
        if (ih->get_value(key, &out, context_->txn_)) {
            throw IndexEntryDuplicateError();
        }
    }

    // 第三段：真正写入
    rid_ = fh_->insert_record(rec.data, context_);
    for (size_t i = 0; i < tab_.indexes.size(); ++i) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(
            sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        ih->insert_entry(keys[i].get(), rid_, context_->txn_);
    }
    return nullptr;
}
```

### 三个关键设计决策

#### 决策 1：为什么 keys 要缓存（用 vector<unique_ptr>）
Pre-check 和真写入两处都要同一份 key，**字节必须完全一致**。分别算两次极易出错（offset、长度、顺序），缓存一份就安全。

#### 决策 2：为什么用 `std::unique_ptr<char[]>`
旧版 `new char[...]` **没有 `delete[]`** → 每次 insert 泄漏一个 key。`unique_ptr` 给了两个保证：
- 正常路径结束自动释放
- **抛异常时也会释放**（RAII），这在"可能 throw" 的新流程里尤其重要

#### 决策 3：为什么外层 `i` 内层换成 `k`
旧版两重循环都用 `i`，内层遮蔽外层。本案例凑巧没出 bug，但这是陷阱。新版改成 `k` 消除隐患。

### 端到端验证（已通过）

```bash
./bin/rmdb t_uniq ...
insert into t values(1, 'a');   # success
insert into t values(1, 'b');   # → output.txt 写 "failure"
insert into t values(2, 'c');   # success
select * from t;                # 只有 (1,'a') (2,'c')
```

---

## 五、Step 3 已完成 — `UpdateExecutor` 改造

### 修改位置
`@/home/simpur/rmdb_2025/db2025/rmdb/src/execution/executor_update.h`

### 为什么 Update 比 Insert 难

Insert 是“无中生有”，查到已有 key 就是冲突。
Update 是“在已有行上修改”，有个关键例外：**新 key 本来就是自己的 → 不算冲突**。

### 场景示例

表有一行 `(id=5, ...)`，id 有唯一索引。

```sql
update t set val = 99 where id = 5;
-- 改的是 val 列，id 没变
-- 新 key 仍是 5，B+ 树里能查到 rid=X
-- 但 rid=X 正是当前这一行！不能算冲突
```

### 实际实现流程

```
for 每个 rid 要更新:
  ① 读 old_rec
  ② 拷贝出 new_rec，应用 set_clauses
  ③ Pre-check 阶段（零副作用）：
      for 每个索引:
          算 old_key / new_key
          if memcmp(old_key, new_key) == 0:
              标记 key_changed[i] = false，跳过
          else:
              key_changed[i] = true
              get_value(new_key) → 查到的 rid 列表
              遍历结果，排除自身 rid → 若仍有其他匹配 → throw 冲突
  ④ Commit 阶段（前面全过才做）：
      for 每个 key_changed 的索引:
          delete_entry(old_key)
      update_record(rid, new_rec)
      for 每个 key_changed 的索引:
          insert_entry(new_key, rid)
```

### 代码要点

```cpp
std::unique_ptr<RmRecord> Next() override {
    for (auto &rid : rids_) {
        auto rec = fh_->get_record(rid, context_);
        // 构造 new_rec
        RmRecord new_rec(fh_->get_file_hdr().record_size);
        memcpy(new_rec.data, rec->data, rec->size);
        for (auto &clause : set_clauses_) { ... }

        // Pre-check
        size_t n_idx = tab_.indexes.size();
        std::vector<std::unique_ptr<char[]>> old_keys(n_idx), new_keys(n_idx);
        std::vector<bool> key_changed(n_idx, false);

        for (size_t i = 0; i < n_idx; ++i) {
            // 算 old_key / new_key
            if (memcmp(old_keys[i].get(), new_keys[i].get(), len) == 0)
                continue;  // key 没变，跳过
            key_changed[i] = true;
            // get_value 查重，排除自身 rid
            for (auto &out_rid : out) {
                if (out_rid != rid) throw IndexEntryDuplicateError();
            }
        }

        // Commit：先删旧 key，再改记录，再插新 key
        for (size_t i = 0; i < n_idx; ++i) {
            if (!key_changed[i]) continue;
            ih->delete_entry(old_keys[i].get(), ...);
        }
        fh_->update_record(rid, new_rec.data, context_);
        for (size_t i = 0; i < n_idx; ++i) {
            if (!key_changed[i]) continue;
            ih->insert_entry(new_keys[i].get(), rid, ...);
        }
    }
    return nullptr;
}
```

### 三个关键设计决策

#### 决策 1：为什么需要“自身豁免”
`update t set val = 99 where id = 5;` — id 没变，`get_value(5)` 会查到自己。如果不排除自身 rid，就会误报冲突。

#### 决策 2：为什么用 `memcmp` 跳过未变的索引
如果 key 没变，delete + insert 是无用功，还可能因为并发引起竞态。`memcmp == 0` 直接跳过，**既高效又安全**。

#### 决策 3：为什么 delete → update_record → insert 三步分离
不能先 insert 新 key（可能和旧 key 冲突），也不能先删记录（索引还指向旧位置）。**delete 旧 key → 改记录 → insert 新 key** 是唯一安全顺序。

---

## 六、Step 5 已完成 — `SHOW INDEX` 全链路

### 涉及文件（6 层修改）

| 层 | 文件 | 修改内容 |
|---|---|---|
| AST | `@/home/simpur/rmdb_2025/db2025/rmdb/src/parser/ast.h:50-53` | 新增 `ShowIndex` 节点 |
| Parser | `@/home/simpur/rmdb_2025/db2025/rmdb/src/parser/yacc.y:112-115` | 新增 `SHOW INDEX FROM tbName` 语法规则 |
| Plan | `@/home/simpur/rmdb_2025/db2025/rmdb/src/optimizer/plan.h:26` | 枚举新增 `T_ShowIndex` |
| Optimizer | `@/home/simpur/rmdb_2025/db2025/rmdb/src/optimizer/optimizer.h:41-43` | 路由到 `OtherPlan(T_ShowIndex, tab_name)` |
| Execution | `@/home/simpur/rmdb_2025/db2025/rmdb/src/execution/execution_manager.cpp:93-97` | 派发 `T_ShowIndex` → `sm_manager_->show_index()` |
| SM Manager | `@/home/simpur/rmdb_2025/db2025/rmdb/src/system/sm_manager.cpp:164-182` | 实现 `show_index()` |

### AST 节点
```cpp
struct ShowIndex : public TreeNode {
    std::string tab_name;
    ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
};
```

### Parser 规则
```yacc
dbStmt:
    SHOW TABLES { $$ = std::make_shared<ShowTables>(); }
  | SHOW INDEX FROM tbName { $$ = std::make_shared<ShowIndex>($4); }
  ;
```
不需要新增任何 token（`SHOW`、`INDEX`、`FROM` 已存在）。

### Optimizer 路由
```cpp
} else if (auto x = std::dynamic_pointer_cast<ast::ShowIndex>(query->parse)) {
    return std::make_shared<OtherPlan>(T_ShowIndex, x->tab_name);
}
```
复用 `OtherPlan`，`tab_name_` 携带表名。

### SM Manager 实现
```cpp
void SmManager::show_index(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    RecordPrinter printer(3);
    printer.print_separator(context);
    for (auto &index : tab.indexes) {
        std::string idx_cols = "(";
        for (int i = 0; i < index.col_num; ++i) {
            if (i > 0) idx_cols += ",";
            idx_cols += index.cols[i].name;
        }
        idx_cols += ")";
        printer.print_record({tab_name, "unique", idx_cols}, context);
        outfile << "| " << tab_name << " | unique | " << idx_cols << " |" << "\n";
    }
    printer.print_separator(context);
    outfile.close();
}
```

### 输出格式
```
| warehouse | unique | (id) |
| warehouse | unique | (id,name) |
```
本项目所有索引都是唯一索引，所以第二列固定为 `unique`。

---

## 七、Step 6（额外修复）— `TabMeta` 拷贝构造函数 Bug

### 修改位置
`@/home/simpur/rmdb_2025/db2025/rmdb/src/system/sm_meta.h:76-80`

### 问题
原始的 `TabMeta` 拷贝构造函数只拷贝了 `cols`，**没有拷贝 `indexes`**：
```cpp
TabMeta(const TabMeta &other) {
    name = other.name;
    for(auto col : other.cols) cols.push_back(col);
    // indexes 完全丢失！
}
```

### 影响范围
`InsertExecutor`、`UpdateExecutor`、`DeleteExecutor` 的构造函数中都有：
```cpp
tab_ = sm_manager_->db_.get_table(tab_name);  // 触发拷贝构造
```
拷贝出来的 `tab_` 的 `indexes` 为空 → **所有唯一性检查和索引维护全部失效**。

### 修复
```cpp
TabMeta(const TabMeta &other) {
    name = other.name;
    for(auto col : other.cols) cols.push_back(col);
    for(auto idx : other.indexes) indexes.push_back(idx);  // ← 新增
}
```

### 教训
这是一个**隐蔽的浅拷贝遗漏 bug**。编译器不报错，运行时也不崩（只是 `tab_.indexes` 始终为空，循环直接跳过）。只有在做端到端测试时才能发现“唯一性约束不生效”。

---

## 八、测试点对照表

| 测试点 | 功能 | 对应 Step | 状态 |
|---|---|---|---|
| 题目二·1 | 建表/删表/show tables | 原有 | ✅ |
| 题目二·2 | 插入+条件查询 | 原有 | ✅ |
| 题目二·3 | 更新+条件查询 | 原有 | ✅ |
| 题目三·1 | create/drop/show index | Step 5 | ✅ |
| 题目三·2 | 索引等值+范围查询 | 原有 IndexScan | ✅ |
| 题目三·3 | 唯一性 failure | Step 1+2+3+6 | ✅ |
| 题目三·4 | 单列索引加速 | 原有 planner | ✅ |
| 题目三·5 | 多列索引加速 | 原有 planner | ✅ |

### 编译与测试结果
- **编译**：`make -j` 零错误
- **executor_full_test**：40/40 通过
- **sm_manager_test**：20/20 通过
- **record_index_test**：20/20 通过
- **integration_test**：26/26 通过
- **总计**：106/106 通过

---

## 九、自测问题

1. `IndexEntryDuplicateError` 为什么一定要继承 `RMDBError` 而不是 `std::runtime_error`？
2. Step 2 中如果不缓存 keys，分两次算 key，可能出什么 bug？
3. 为什么 Step 2 的新流程抛异常后"零副作用"？假设 `get_value` 本身抛异常呢？
4. 为什么 Update 需要"自身豁免"，而 Insert 不需要？
5. 如果某个索引 pre-check 通过了，但 `insert_entry` 因为 B+ 树 split 出错，能回滚吗？（答：事务层缺失，暂时不能；记录到 P3 WAL 工作项）
6. 本项目"所有索引都是唯一索引"意味着什么？如果要支持非唯一索引需要改哪里？

---

## 十、历史记录

| 日期 | 进度 |
|---|---|
| 2026-04-18 傍晚 | Step 1 `IndexEntryDuplicateError`；Step 2 `InsertExecutor` pre-check 两段式；端到端验证重复 key 被干净挡下 |
| 2026-04-23 晚 | Step 3 `UpdateExecutor` 自身豁免查重；Step 5 `SHOW INDEX` 全链路（6层）；Step 6 修复 `TabMeta` 拷贝构造遗漏 indexes；全部 106 个测试通过 |

---

## 十一、与其他学习文档的关系

| 文档 | 相关章节 |
|---|---|
| `@/home/simpur/rmdb_2025/db2025/learning_docs/AST_GUIDE.md` | Step 5 `SHOW INDEX` parser/ast 需要参考 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/Q2_ANALYZE_GUIDE.md` | Step 5 analyze 分支参考 UpdateStmt 模式 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/EXECUTION_LAYER.md` | Step 5 新 executor 骨架参考 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/SYSTEM_LAYER.md` | `TabMeta.indexes` 结构说明 |

---

**全部完成。**
