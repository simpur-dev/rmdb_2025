# 题目二复习文档 — 查询执行 & Analyze 模块

> 本文档整理"题目二"三个测试点的通关路径，重点记录 analyze 模块是如何从半成品走向完备的。
> 复习时先过第一章建立全景，再按第三~六章对着源码位置查细节。

---

## 一、题目二概览

**题目二目标**：让服务端能稳定执行 DDL（CREATE/DROP/SHOW TABLES）、基本 SELECT 含 WHERE、UPDATE/DELETE。

| 测试点 | 典型 SQL | 关键挑战 |
|---|---|---|
| 1 | `create table`, `drop table`, `show tables` | 服务端能稳定接受多条 SQL（需要 TransactionManager 最小版） |
| 2 | `select ... where score > 90`（FLOAT 列 vs INT 字面量） | **类型隐式提升 INT→FLOAT** |
| 3 | `update grade set score = 90 where name = 'X'` | **UpdateStmt 分支从零实现** + SET 的类型提升 |

### 修改到的五个位置

```
transaction_manager.cpp   Step 0：begin/commit/abort 最小实现（否则服务端第一条 SQL 就段错）
analyze.cpp  SelectStmt 分支   加表存在性校验
analyze.cpp  UpdateStmt 分支   从空 TODO 到完整翻译 + 类型校验
analyze.cpp  DeleteStmt 分支   加表存在性校验
analyze.cpp  InsertStmt 分支   从"只转值"到"表存在+值数+类型+长度+提升"全套
analyze.cpp  check_clause      WHERE 中 INT→FLOAT 提升
analyze.cpp  check_column      else 分支（指定表名时）的列存在校验
```

---

## 二、阻塞点链路图

三个测试点像三米跳板，每层解锁前都被前一层卡住：

```
       题目二开始
           │
           ▼
  ┌────────────────────┐
  │ 服务端第一条 SQL 段错 │ ← TransactionManager::begin 返回 nullptr
  └────────┬───────────┘
           │  最小实现 begin/commit/abort 之后
           ▼
  ┌────────────────────┐
  │ 测试点 1 通过        │
  └────────┬───────────┘
           │
           ▼
  ┌────────────────────────────────┐
  │ WHERE score > 90 报             │
  │ IncompatibleType(FLOAT vs INT) │ ← check_clause 严格相等
  └────────┬───────────────────────┘
           │  check_clause 加 INT→FLOAT 提升
           ▼
  ┌────────────────────┐
  │ 测试点 2 通过        │
  └────────┬───────────┘
           │
           ▼
  ┌────────────────────────────────┐
  │ UPDATE 跑不通                   │ ← UpdateStmt 分支是空 TODO
  └────────┬───────────────────────┘
           │  补 UpdateStmt 分支 + SET 类型提升
           ▼
  ┌────────────────────┐
  │ 测试点 3 通过        │
  └────────────────────┘
```

---

## 三、TransactionManager 最小版（先解锁）

### 问题现象
服务端收到任意第一条 SQL 就**段错**（SIGSEGV）。

### 根因
`@/home/simpur/rmdb_2025/db2025/rmdb/src/transaction/transaction_manager.cpp` 的 `begin` 原本是空函数，直接返回 `nullptr`。上游 `rmdb.cpp` 拿到 `nullptr` 后调用 `txn->get_transaction_id()` → 解引用空指针。

### 最小修复
```cpp
Transaction* TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    if (txn == nullptr) {
        txn_id_t new_id = next_txn_id_.fetch_add(1);
        txn = new Transaction(new_id);
        txn->set_start_ts(next_timestamp_.fetch_add(1));
    }
    {
        std::unique_lock<std::mutex> lock(latch_);
        txn_map[txn->get_transaction_id()] = txn;
    }
    txn->set_state(TransactionState::GROWING);
    return txn;
}
```

`commit` / `abort` 对应做状态切换 + 释放锁占位，不实现 MVCC / WAL / 回滚也能让服务端跑起来。

### 记忆要点
"最小 begin" 的核心是**三个字段**：
1. 分配递增的 `txn_id`（`next_txn_id_.fetch_add`）
2. 注册到 `txn_map`（后续 commit/abort 靠它找回 Transaction）
3. 设状态为 GROWING（`TransactionState::GROWING`）

---

## 四、AST → Query 转换基础知识

### 4.1 为什么 analyze 必须存在

Parser 输出的 AST 离机器执行还差三件事：

| AST 问题 | analyze 做的补救 |
|---|---|
| `IntLit{val=90}` 是树节点，不是运行时值 | `convert_sv_value` 转成 `common.h::Value{type, int_val}` |
| `Col{tab_name="", col_name="name"}` 表名可能为空 | `check_column` 根据候选列表推断表名 |
| `ast::IntLit` 写进 FLOAT 列不合语义 | `check_clause` 做类型校验 + 隐式提升 |

### 4.2 Query 结构（ANALYZE 的产出）

```cpp
class Query {
    std::shared_ptr<ast::TreeNode> parse;   // 原始 AST（optimizer 还会读）
    std::vector<Condition>  conds;          // WHERE 条件（已就绪）
    std::vector<TabCol>     cols;           // 投影列（已补表名）
    std::vector<std::string> tables;        // FROM 表（已校验存在）
    std::vector<SetClause>  set_clauses;    // UPDATE SET（已校验类型）
    std::vector<Value>      values;         // INSERT VALUES（已校验 + 提升）
};
```

### 4.3 AST 双重 SetClause 陷阱

```cpp
// parser/ast.h:154  — AST 版
ast::SetClause { col_name; shared_ptr<Value> val; }

// common.h:85       — 运行时版
SetClause { TabCol lhs; Value rhs; }
```

**两个同名类别**，analyze 的 UpdateStmt 分支要显式翻译。

> 详细 AST 知识见 `@/home/simpur/rmdb_2025/db2025/learning_docs/AST_GUIDE.md`。

---

## 五、关键代码模式（必须记住）

### 5.1 模式 A：表存在性校验（前置 fail-fast）

```cpp
if (!sm_manager_->db_.is_table(x->tab_name)) {
    throw TableNotFoundError(x->tab_name);
}
```

- `SelectStmt / UpdateStmt / DeleteStmt / InsertStmt` 四个分支**顶部都要有**
- 不加也不会崩（后续 `get_table` 会抛），但错误信息不如此处显式

### 5.2 模式 B：列推断 + 类型校验 + 隐式提升

```cpp
// 在 check_clause 里
if (cond.is_rhs_val) {
    // ★ 必须先做类型提升，再做 init_raw
    if (lhs_type == TYPE_FLOAT && cond.rhs_val.type == TYPE_INT) {
        cond.rhs_val.set_float((float)cond.rhs_val.int_val);
    }
    cond.rhs_val.init_raw(lhs_col->len);   // 这里会按 Value.type 写字节
    rhs_type = cond.rhs_val.type;
}
if (lhs_type != rhs_type) {
    throw IncompatibleTypeError(...);
}
```

**关键顺序**：`set_float` → `init_raw` → `type 比较`。

### 5.3 模式 C：UpdateStmt 从 AST 翻译到 Query

```cpp
} else if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
    if (!sm_manager_->db_.is_table(x->tab_name)) {
        throw TableNotFoundError(x->tab_name);
    }
    query->tables = {x->tab_name};
    TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);

    for (auto &sv_sc : x->set_clauses) {
        SetClause sc;
        sc.lhs = {.tab_name = x->tab_name, .col_name = sv_sc->col_name};
        sc.rhs = convert_sv_value(sv_sc->val);

        auto col = tab.get_col(sv_sc->col_name);        // 列存在校验
        if (col->type == TYPE_FLOAT && sc.rhs.type == TYPE_INT) {
            sc.rhs.set_float((float)sc.rhs.int_val);    // 类型提升
        }
        if (col->type != sc.rhs.type) {
            throw IncompatibleTypeError(
                coltype2str(col->type), coltype2str(sc.rhs.type));
        }
        query->set_clauses.push_back(sc);
    }

    get_clause(x->conds, query->conds);           // WHERE 复用
    check_clause({x->tab_name}, query->conds);
}
```

**固定三步**：
1. 表存在校验 + `query->tables` 填充
2. 遍历 AST 的某个 vector 字段，逐个翻译 + 校验
3. `get_clause` + `check_clause` 处理 WHERE

DeleteStmt 是 UpdateStmt 的**子集**（没有 SET）；InsertStmt 则没有 WHERE。

### 5.4 模式 D：InsertStmt 完整校验（四重门）

```cpp
} else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
    // 门 1: 表存在
    if (!sm_manager_->db_.is_table(x->tab_name)) {
        throw TableNotFoundError(x->tab_name);
    }
    query->tables = {x->tab_name};
    TabMeta &tab = sm_manager_->db_.get_table(x->tab_name);

    // 门 2: 值数 == 列数
    if (x->vals.size() != tab.cols.size()) {
        throw InternalError("INSERT values count does not match column count");
    }

    query->values.reserve(x->vals.size());
    for (size_t i = 0; i < x->vals.size(); ++i) {
        Value val = convert_sv_value(x->vals[i]);
        const ColMeta &col = tab.cols[i];

        // 门 3: INT→FLOAT 提升
        if (col.type == TYPE_FLOAT && val.type == TYPE_INT) {
            val.set_float((float)val.int_val);
        }
        if (col.type != val.type) {                       // 门 3b: 严格类型
            throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
        }
        // 门 4: CHAR 长度
        if (col.type == TYPE_STRING && (int)val.str_val.size() > col.len) {
            throw StringOverflowError();
        }
        query->values.push_back(std::move(val));
    }
}
```

### 5.5 模式 E：check_column 的双分支

```cpp
TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // 用户没写表名 — 在候选列里唯一推断
        // ...（框架提供，略）
    } else {
        // 用户写了 t.x — 确认 x 真属于 t
        auto it = std::find_if(all_cols.begin(), all_cols.end(),
            [&](const ColMeta &c) {
                return c.tab_name == target.tab_name
                    && c.name == target.col_name;
            });
        if (it == all_cols.end()) throw ColumnNotFoundError(target.col_name);
    }
    return target;
}
```

**两分支语义不同**：
- 没写表名：从候选列表里唯一化推断（AmbiguousColumnError / ColumnNotFoundError）
- 写了表名：精确校验（ColumnNotFoundError）

---

## 六、六个核心设计决策（为什么这么写）

### 决策 1：类型提升必须在 `init_raw` 之前

**背景**：`Value::init_raw(len)` 根据 `this->type` 决定往 raw buffer 写 int/float/string。

**反例**：FLOAT 列收到 INT 字面量，不做提升就 init_raw：
```cpp
// TYPE_INT 分支：assert(len == sizeof(int)); *(int*)raw->data = int_val;
// 但 col.len 是 4（float），恰好过 assert，于是把 int bit pattern 写进 float 槽 → 数据错乱
```

**正解**：先 `set_float((float)int_val)` 把 type 和 value 都改成 FLOAT，再 init_raw。

### 决策 2：只做 FLOAT←INT 单向提升

反向 `WHERE id > 1.5`（INT 列 vs FLOAT 字面量）：
- 语义不明确（截断？还是把 id 升成 float 比较？）
- 题目二/三不涉及

保守策略：**只做题目需要的方向**。

### 决策 3：is_table 显式校验是"冗余"但有意义

下游 `db_.get_table(name)` 也会对不存在的表抛异常，那为什么还要前置 `is_table`？

1. 错误路径更短，堆栈不埋在工具函数里
2. 符合"入口 fail-fast"的编码原则
3. 未来添加 ACL / 日志时有统一插入点

### 决策 4：InsertStmt 的 CHAR 长度前置检查

不加也会在 executor 的 `Value::init_raw` 里抛 `StringOverflowError`。为什么分析阶段还要再查一次？

- executor 崩了可能留下部分写入（虽然本案例不会）
- 用户体感更好：输入当场就被拒，不是"执行到一半失败"

这是**防御性编程**思路，不是必需但推荐。

### 决策 5：SetClause 两层同名结构的翻译

**不能**直接把 `ast::SetClause` 塞进 `query->set_clauses`。必须：
- `lhs` 补上 `tab_name`
- `rhs` 从 `shared_ptr<ast::Value>` 转成 `common.h::Value`

记住：**"AST 是贴近语法的，Query 是贴近执行的"**。两层意图不同，字段布局就不同。

### 决策 6：check_clause 可以被多个分支复用

Select / Update / Delete 的 WHERE 语义完全一样，只是 FROM 的表列表不同：
- Select: `query->tables`（多表）
- Update / Delete: `{x->tab_name}`（单表）

所以三处都调 `check_clause(tab_list, conds)`，**不要重复实现 WHERE 校验**。

---

## 七、典型错误（亲身踩过）

| 错误 | 症状 | 定位提示 |
|---|---|---|
| `reverse` vs `reserve` | vector 没有 reverse 成员方法 | 编译错误：`‘reverse’ is not a member of ‘std::vector’` |
| `sc.lhs = {.a = x. .b = y}` | 句号写成逗号 | 编译错误：`expected '}'` |
| `tab.get_col(sv_sc->val)` | 把 AST 字面量当列名传 | 编译错误：类型不匹配 |
| `TYPE.INT` | 用点号代替下划线 | 编译错误：`‘TYPE’ is not declared` |
| `query->set_clause` | 字段名漏复数 | 编译错误：`set_clause is not a member` |
| 多一个 `}` | 结构嵌套错位 | 编译错误：`extraneous closing brace` |
| `targer` vs `target` | 单词拼错 | 编译错误：`‘targer’ was not declared` |

**教训**：复制粘贴时逐行核对；编译器是你最诚实的朋友。

---

## 八、端到端验证方法

### 启动服务端 + 喂 SQL
```bash
cd /home/simpur/rmdb_2025/db2025/rmdb/build
pkill -9 rmdb; rm -rf test_db
./bin/rmdb test_db > /tmp/server.log 2>&1 &
sleep 2
./bin/rmdb_client < /tmp/your_test.sql
cat test_db/output.txt
```

### 题目二测试点回归
```sql
create table grade (name char(20), id int, score float);
insert into grade values ('Data Structure', 1, 90.5);
insert into grade values ('Calculus', 2, 92.0);
select * from grade where score > 90;              -- 测试点 2：类型提升
update grade set score = 90 where name = 'Calculus';  -- 测试点 3：UPDATE + SET 提升
select * from grade;
delete from grade where score < 90;
```

### 正确的预期输出
```
| name | id | score |
| Data Structure | 1 | 90.500000 |
| Calculus | 2 | 92.000000 |
| name | id | score |
| Data Structure | 1 | 90.500000 |
| Calculus | 2 | 90.000000 |
```

---

## 九、复习自测问题（读完本文应能答出）

1. 为什么 TransactionManager::begin 必须最小实现？只返回 `nullptr` 会发生什么？
2. `check_clause` 里 `init_raw` 为什么必须在 `set_float` 之后？
3. AST 的 `SetClause` 和 common.h 的 `SetClause` 分别有哪些字段？为什么要翻译？
4. `check_column` 的 `if` 和 `else` 分支各处理什么场景？错误类型有什么不同？
5. InsertStmt 的 4 重门分别查什么？哪一步是必须的（不加数据库就会崩）？
6. 为什么我们**只做** INT→FLOAT 单向提升？
7. 为什么 Update/Delete/Select 的 WHERE 都能复用 `check_clause`？
8. 如果有个新类型 `DOUBLE`，需要改 analyze 的哪些地方？

---

## 十、与其他学习文档的关系

| 文档 | 范围 | 什么时候看 |
|---|---|---|
| `@/home/simpur/rmdb_2025/db2025/learning_docs/AST_GUIDE.md` | AST 所有节点细节 | 想理解 parser 产出什么 |
| **本文档** | 题目二 / analyze 模块 | 复习 analyze 为什么这么改 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/EXECUTION_LAYER.md` | 执行器 | 想理解 Query 怎么跑起来 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/SYSTEM_LAYER.md` | SmManager / 元数据 | 想理解 is_table / get_col 是怎么工作的 |
| `@/home/simpur/rmdb_2025/db2025/learning_docs/STORAGE_LAYER.md` | 存储引擎 | 最底层 |

---

**完**
