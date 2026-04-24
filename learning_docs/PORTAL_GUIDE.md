# Portal 层详解 — 从执行计划到算子树

> Portal 是 RMDB 查询执行流水线中**计划（Plan）到执行（Executor）的桥梁**。
> 术语来源于 PostgreSQL，在 PG 中 Portal 表示"一条已准备好可执行的语句"。

---

## 一、Portal 在整体流水线中的位置

```
SQL 文本
  │
  ▼
Lexer (lex.l)          词法分析，生成 token
  │
  ▼
Parser (yacc.y)        语法分析，生成 AST
  │
  ▼
Analyzer (analyze.cpp) 语义分析，校验表/列/类型
  │
  ▼
Optimizer (optimizer.h + planner.cpp)
                       查询优化，生成 Plan 树
  │
  ▼
Portal (portal.h)      ★ Plan → Executor 树 + 分派执行
  │
  ▼
Executor (executor_*.h) 算子执行，真正读写数据
```

**Plan 不碰数据，Executor 不知道优化策略，Portal 把两者连接起来。**

---

## 二、源文件

`@/home/simpur/rmdb_2025/db2025/rmdb/src/portal.h`（单文件，约 182 行）

---

## 三、核心数据结构

### 3.1 `portalTag` 枚举

```cpp
typedef enum portalTag {
    PORTAL_Invalid_Query = 0,
    PORTAL_ONE_SELECT,           // SELECT 查询
    PORTAL_DML_WITHOUT_SELECT,   // INSERT / UPDATE / DELETE
    PORTAL_MULTI_QUERY,          // DDL（CREATE/DROP TABLE/INDEX）
    PORTAL_CMD_UTILITY           // 工具命令（SHOW/HELP/BEGIN/COMMIT/ABORT）
} portalTag;
```

每种 tag 对应不同的执行路径，Portal 据此决定调用 `QlManager` 的哪个方法。

### 3.2 `PortalStmt` 结构体

```cpp
struct PortalStmt {
    portalTag tag;                           // 执行类型
    std::vector<TabCol> sel_cols;            // SELECT 的输出列（仅 SELECT 用）
    std::unique_ptr<AbstractExecutor> root;  // Executor 算子树的根节点
    std::shared_ptr<Plan> plan;              // 原始 Plan（DDL/工具命令直接用）
};
```

**关键点**：`root` 和 `plan` 不一定同时有值。
- SELECT / DML → `root` 有值（Executor 树已构建）
- DDL / 工具命令 → `root` 为空，靠 `plan` 直接传给 `QlManager`

---

## 四、三个核心方法

### 4.1 `start()` — Plan → PortalStmt

这是 Portal 的入口，根据 Plan 类型做分流：

```
Plan 类型               → portalTag                  → 做什么
──────────────────────────────────────────────────────────────
OtherPlan (SHOW/HELP)   → PORTAL_CMD_UTILITY         → 不建 Executor，直传 plan
SetKnobPlan              → PORTAL_CMD_UTILITY         → 不建 Executor，直传 plan
DDLPlan (CREATE/DROP)    → PORTAL_MULTI_QUERY         → 不建 Executor，直传 plan
DMLPlan + T_select       → PORTAL_ONE_SELECT          → 构建完整 Executor 树
DMLPlan + T_Insert       → PORTAL_DML_WITHOUT_SELECT  → 构建 InsertExecutor
DMLPlan + T_Update       → PORTAL_DML_WITHOUT_SELECT  → 先扫描收集 rids → UpdateExecutor
DMLPlan + T_Delete       → PORTAL_DML_WITHOUT_SELECT  → 先扫描收集 rids → DeleteExecutor
```

#### Update / Delete 的特殊处理

```cpp
// Update 为例（Delete 同理）
case T_Update:
{
    // 1. 先用 scan executor 收集所有要修改的 rid
    std::unique_ptr<AbstractExecutor> scan = convert_plan_executor(x->subplan_, context);
    std::vector<Rid> rids;
    for (scan->beginTuple(); !scan->is_end(); scan->nextTuple()) {
        rids.push_back(scan->rid());
    }
    // 2. 再把 rids 传给 UpdateExecutor
    std::unique_ptr<AbstractExecutor> root =
        std::make_unique<UpdateExecutor>(sm_manager_, x->tab_name_,
                                         x->set_clauses_, x->conds_, rids, context);
    ...
}
```

**为什么要先收集 rids？**
如果边扫描边修改，修改后的记录可能被扫描到（幻读），或者索引结构变化导致迭代器失效。先收集 rids 再统一修改，避免这些问题。

### 4.2 `run()` — 分派执行

```cpp
void run(std::shared_ptr<PortalStmt> portal, QlManager* ql,
         txn_id_t *txn_id, Context *context)
```

按 `portalTag` 分派到 `QlManager`（即 `execution_manager.cpp`）的不同方法：

| portalTag | 调用 | 做什么 |
|---|---|---|
| `PORTAL_ONE_SELECT` | `ql->select_from()` | 迭代 Executor 树，打印/输出结果 |
| `PORTAL_DML_WITHOUT_SELECT` | `ql->run_dml()` | 调用 `root->Next()` 执行 Insert/Update/Delete |
| `PORTAL_MULTI_QUERY` | `ql->run_mutli_query()` | 调用 `sm_manager` 执行 DDL |
| `PORTAL_CMD_UTILITY` | `ql->run_cmd_utility()` | 执行 SHOW/HELP/事务命令 |

### 4.3 `convert_plan_executor()` — 递归翻译 Plan → Executor

这是最关键的方法，把 Plan 树**递归**翻译成 Executor 树：

```cpp
std::unique_ptr<AbstractExecutor> convert_plan_executor(
    std::shared_ptr<Plan> plan, Context *context)
```

翻译规则：

| Plan 类型 | Executor 类型 | 说明 |
|---|---|---|
| `ProjectionPlan` | `ProjectionExecutor` | 递归翻译子计划 |
| `ScanPlan (T_SeqScan)` | `SeqScanExecutor` | 全表扫描（叶子节点） |
| `ScanPlan (T_IndexScan)` | `IndexScanExecutor` | 索引扫描（叶子节点） |
| `JoinPlan` | `NestedLoopJoinExecutor` | 递归翻译左右子计划 |
| `SortPlan` | `SortExecutor` | 递归翻译子计划 |

#### 示例：一条 SELECT 的翻译过程

```sql
SELECT w_id, name FROM warehouse WHERE w_id = 1 ORDER BY w_id;
```

```
Plan 树                              Executor 树
────────                             ────────────
ProjectionPlan(w_id, name)    →    ProjectionExecutor
  └── SortPlan(w_id ASC)      →      └── SortExecutor
        └── ScanPlan(T_IndexScan) →        └── IndexScanExecutor
```

#### SeqScan vs IndexScan 的分叉点

```cpp
if (x->tag == T_SeqScan) {
    return std::make_unique<SeqScanExecutor>(...);
} else {
    return std::make_unique<IndexScanExecutor>(...);
}
```

**Planner 决定 tag 是什么，Portal 只是忠实翻译。** 索引选择的智能在 `planner.cpp::get_index_cols()`，不在 Portal。

---

## 五、四种执行路径完整链路

### 路径 1：SELECT

```
optimizer → DMLPlan(T_select, subplan=ProjectionPlan→ScanPlan)
  │
  ▼ portal.start()
convert_plan_executor() 递归构建:
  ProjectionExecutor → [SortExecutor →] IndexScanExecutor/SeqScanExecutor
  │
  ▼ portal.run()
ql->select_from(root, sel_cols)
  → 迭代 root->beginTuple() / Next() / nextTuple()
  → 打印到客户端 + 写 output.txt
```

### 路径 2：INSERT

```
optimizer → DMLPlan(T_Insert, values)
  │
  ▼ portal.start()
直接构建 InsertExecutor(values)
  │
  ▼ portal.run()
ql->run_dml(root)
  → root->Next()
  → 唯一性检查 → 插入记录 → 插入索引
```

### 路径 3：UPDATE / DELETE

```
optimizer → DMLPlan(T_Update, subplan=ScanPlan)
  │
  ▼ portal.start()
1. convert_plan_executor(subplan) → SeqScan/IndexScan
2. 遍历 scan 收集 rids
3. 构建 UpdateExecutor(rids)
  │
  ▼ portal.run()
ql->run_dml(root)
  → root->Next()
  → 逐 rid：pre-check 唯一性 → 删旧索引 → 改记录 → 插新索引
```

### 路径 4：DDL / 工具命令

```
optimizer → DDLPlan / OtherPlan
  │
  ▼ portal.start()
不构建 Executor，直传 plan
  │
  ▼ portal.run()
ql->run_mutli_query(plan) 或 ql->run_cmd_utility(plan)
  → 直接调用 sm_manager 的方法（create_table / show_index 等）
```

---

## 六、设计要点

### 6.1 为什么需要 Portal 这一层？

**解耦 Plan 和 Executor。** 如果没有 Portal：
- Optimizer 要知道所有 Executor 的构造函数
- 增加新算子时要同时改 Optimizer

有了 Portal，Optimizer 只管生成 Plan，Executor 只管执行，**翻译逻辑集中在一个地方**。

### 6.2 为什么 DDL/工具命令不建 Executor？

`CREATE TABLE`、`SHOW INDEX` 这类命令**不涉及数据扫描**，直接操作元数据即可。为它们建 Executor 是过度设计。

### 6.3 `convert_plan_executor()` 的递归结构

Executor 树是**火山模型（Volcano Model）**：
- 每个算子实现 `beginTuple()` / `Next()` / `nextTuple()` / `is_end()`
- 上层算子调用下层算子的 `Next()` 拉取数据
- 数据从叶子（Scan）流向根（Projection）

```
ProjectionExecutor.Next()
  → SortExecutor.Next()
    → IndexScanExecutor.Next()
      → 从 B+ 树读数据页 → 返回 record
    ← record
  ← 排序后的 record
← 投影后的 record
```

---

## 七、与其他学习文档的关系

| 文档 | 相关内容 |
|---|---|
| `EXECUTION_LAYER.md` | 各 Executor 的详细实现 |
| `Q3_UNIQUE_INDEX_GUIDE.md` | UpdateExecutor 的唯一性 pre-check 在 Portal 收集 rids 之后执行 |
| `AST_GUIDE.md` | Plan 的输入来源（AST → Analyze → Plan） |
| `SYSTEM_LAYER.md` | DDL 路径最终调用的 SmManager 方法 |
