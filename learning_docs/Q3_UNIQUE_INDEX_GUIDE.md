# 题目三复习文档 — 唯一索引

> 持续更新。每完成一个 Step，就在对应章节加内容。
> 目前进度：**Step 1 + Step 2 已完成**；Step 3/4/5 待做。

---

## 一、题目三需求拆解

官方说明（简化版）：

| 子功能 | 说明 |
|---|---|
| `create index / drop index` | 框架已提供（`SmManager::create_index / drop_index`） |
| `show index from t` | **未实现** → Step 5 |
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
| 3 | `UpdateExecutor` 唯一性 pre-check（含自身豁免） | ⏳ | `executor_update.h` |
| 4 | 回归验证：DDL + DML + Index 混合场景 | ⏳ | 端到端 SQL 脚本 |
| 5 | `SHOW INDEX` 全链路实现 | ⏳ | parser / ast / analyze / executor / plan |

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

## 五、Step 3 待做 — `UpdateExecutor` 改造

### 为什么 Update 比 Insert 难

Insert 是"无中生有"，查到已有 key 就是冲突。
Update 是"在已有行上修改"，有个关键例外：**新 key 本来就是自己的 → 不算冲突**。

### 场景示例

表有一行 `(id=5, ...)`，id 有唯一索引。

```sql
update t set val = 99 where id = 5;
-- 改的是 val 列，id 没变
-- 新 key 仍是 5，B+ 树里能查到 rid=X
-- 但 rid=X 正是当前这一行！不能算冲突
```

### 设计思路（Step 3 实施时再细化）

```
for 每个 rid 要更新:
    读 old_rec
    构造 new_rec (按 set_clauses 覆盖)

    for 每个索引:
        算 old_key (从 old_rec)
        算 new_key (从 new_rec)

        if old_key == new_key:
            skip         ← 值没变，索引不动
        else:
            get_value(new_key):
                若查到的 rid 不是当前这行 → 冲突，throw
                若没查到 → OK
            标记：这个索引需要 delete old_key + insert new_key

    pre-check 全过 → 执行所有标记的 delete + update_record + insert
```

**简化策略**（先写最稳版）：
- 一次 update 只处理一行（按 rids_ 逐行 pre-check → 写入）
- 多行原子性：题目三测试不强制，后续 WAL/恢复阶段再补

---

## 六、Step 5 预告 — `SHOW INDEX`

需要改的 6 层：

1. **Lexer / Parser** `rmdb/src/parser/lex.l`, `yacc.y` — 识别 `SHOW INDEX FROM t`
2. **AST** `parser/ast.h` — 新节点 `ShowIndex`
3. **Analyze** `analyze/analyze.cpp` — `ShowIndex` 分支，校验表存在
4. **Plan** 新 plan 节点或复用 OtherPlan
5. **Executor** 新 `ShowIndexExecutor`，从 `TabMeta.indexes` 取数据格式化输出
6. **Interp** `execution_manager.cpp` 把 plan 派发到 executor

输出格式（参考 MySQL）：
```
| Table | Key_name | Column_name |
| t     | t_id     | id          |
```

具体实现等 Step 3/4 做完再推进。

---

## 七、自测问题

1. `IndexEntryDuplicateError` 为什么一定要继承 `RMDBError` 而不是 `std::runtime_error`？
2. Step 2 中如果不缓存 keys，分两次算 key，可能出什么 bug？
3. 为什么 Step 2 的新流程抛异常后"零副作用"？假设 `get_value` 本身抛异常呢？
4. 为什么 Update 需要"自身豁免"，而 Insert 不需要？
5. 如果某个索引 pre-check 通过了，但 `insert_entry` 因为 B+ 树 split 出错，能回滚吗？（答：事务层缺失，暂时不能；记录到 P3 WAL 工作项）
6. 本项目"所有索引都是唯一索引"意味着什么？如果要支持非唯一索引需要改哪里？

---

## 八、历史记录

| 日期 | 进度 |
|---|---|
| 2026-04-18 傍晚 | Step 1 `IndexEntryDuplicateError`；Step 2 `InsertExecutor` pre-check 两段式；端到端验证重复 key 被干净拦下 |

---

## 九、与其他学习文档的关系

| 文档 | 相关章节 |
|---|---|
| `@/home/simpur/rmdb_2025/db2025/learning_docs/AST_GUIDE.md` | Step 5 `SHOW INDEX` parser/ast 需要参考 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/Q2_ANALYZE_GUIDE.md` | Step 5 analyze 分支参考 UpdateStmt 模式 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/EXECUTION_LAYER.md` | Step 5 新 executor 骨架参考 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/SYSTEM_LAYER.md` | `TabMeta.indexes` 结构说明 |

---

**持续更新中。Step 3 完成后回来补第五章实现细节。**
