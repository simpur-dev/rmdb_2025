# AST 学习指南 — RMDB 项目视角

> **读这份文档的收益**：读完后你应该能够
> 1. 看着 `ast.h` 任意一个 struct，知道它对应哪种 SQL 语法
> 2. 心算出任意一句 SQL 的 AST 长什么样
> 3. 在 `analyze.cpp` 里自己写出类似 `SelectStmt` / `UpdateStmt` 的分支代码

---

## 第一章：AST 是什么

### 1.1 形象类比

把 SQL 字符串想象成一句中文：

> "把学生表里叫 Calculus 的那一行的分数改成 90"

人脑会自动把它拆成几个成分：
- **动作**：改（UPDATE）
- **目标**：学生表（table=grade）
- **要改什么**：分数 = 90（SET score=90）
- **筛选条件**：名字是 Calculus（WHERE name='Calculus'）

AST（Abstract Syntax Tree，抽象语法树）就是**用 C++ struct 把这些成分显式存起来**。每个成分一个 struct，能嵌套就用 `shared_ptr` 指过去。

### 1.2 为什么要有 AST

如果没有 AST，你要处理 SQL 就只能拿字符串硬分析，代码会写成这样：

```cpp
// 反例（没 AST 的世界）
if (sql.substr(0, 6) == "UPDATE") {
    auto tab = extract_word_at(sql, 7);
    auto where_pos = sql.find("WHERE");
    // ... 一层一层 find/substr，噩梦
}
```

有了 AST，你可以写成这样：

```cpp
// 实际做法
if (auto x = dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
    auto tab = x->tab_name;                  // 直接取字段
    for (auto &cond : x->conds) { ... }      // 直接遍历条件列表
    for (auto &sc : x->set_clauses) { ... }  // 直接遍历 SET 子句
}
```

---

## 第二章：AST 节点的整体设计

### 2.1 继承关系

所有 AST 节点都继承自 `ast::TreeNode`（`@/home/simpur/rmdb_2025/db2025/rmdb/src/parser/ast.h:40`）：

```cpp
struct TreeNode {
    virtual ~TreeNode() = default;   // 有虚函数才能做 dynamic_cast
};
```

为什么要继承：parser 产出的根节点类型是 `shared_ptr<TreeNode>`，后续代码用 `dynamic_pointer_cast<具体类型>` 判断是哪种语句。

### 2.2 类层次总览

```
TreeNode
├── 语句类（5 个主力 + 若干 DDL）
│   ├── SelectStmt         SELECT ...
│   ├── UpdateStmt         UPDATE ...
│   ├── InsertStmt         INSERT ...
│   ├── DeleteStmt         DELETE ...
│   ├── CreateTable        CREATE TABLE ...
│   ├── DropTable          DROP TABLE ...
│   ├── CreateIndex / DropIndex
│   ├── DescTable          DESC tab
│   ├── ShowTables / Help / TxnBegin / TxnCommit / ...
│   └── SetStmt            SET enable_nestloop = true
│
├── 表达式类 (Expr)
│   ├── Value（抽象）
│   │   ├── IntLit         90
│   │   ├── FloatLit       90.5
│   │   ├── StringLit      'Calculus'
│   │   └── BoolLit        true
│   └── Col                name / grade.name
│
├── 子句类
│   ├── SetClause          score = 90
│   ├── BinaryExpr         name = 'Calculus'
│   ├── JoinExpr           t1 JOIN t2 ON ...
│   └── OrderBy            ORDER BY col ASC
│
└── 辅助类
    ├── TypeLen            INT / CHAR(20) 等类型描述
    ├── Field (抽象)
    │   └── ColDef         id INT / name CHAR(20)
```

### 2.3 从哪生产

`@/home/simpur/rmdb_2025/db2025/rmdb/src/parser/yacc.y` 定义了 grammar 规则，每条规则命中时 `new` 一个对应节点。最终根节点挂在全局变量：

```cpp
// ast.h:284
extern std::shared_ptr<ast::TreeNode> parse_tree;
```

`rmdb.cpp` 里 `yyparse()` 返回后就读 `ast::parse_tree`。

---

## 第三章：五个核心语句节点详解

### 3.1 SelectStmt — 查询语句

```cpp
// ast.h:217
struct SelectStmt : public TreeNode {
    std::vector<std::shared_ptr<Col>>        cols;      // 投影列
    std::vector<std::string>                 tabs;      // FROM 的表
    std::vector<std::shared_ptr<BinaryExpr>> conds;     // WHERE 条件
    std::vector<std::shared_ptr<JoinExpr>>   jointree;  // JOIN（可能为空）
    bool                                     has_sort;
    std::shared_ptr<OrderBy>                 order;
};
```

#### 示例对应

```sql
SELECT score, name FROM grade WHERE score > 90;
```

AST 实例：

```
SelectStmt
├── cols = [
│     Col { tab_name="", col_name="score" }
│     Col { tab_name="", col_name="name"  }
│   ]
├── tabs = ["grade"]
├── conds = [
│     BinaryExpr
│     ├── lhs = Col { tab_name="", col_name="score" }
│     ├── op  = SV_OP_GT
│     └── rhs = IntLit { val=90 }
│   ]
├── jointree = []
├── has_sort = false
└── order = nullptr
```

**注意点**
- `Col.tab_name` 用户没写就是空串 `""`，analyze 要推断补全
- `SELECT *` 时 `cols` 为空 vector，analyze 要展开所有列

---

### 3.2 UpdateStmt — 更新语句

```cpp
// ast.h:195
struct UpdateStmt : public TreeNode {
    std::string                                 tab_name;      // 单表
    std::vector<std::shared_ptr<SetClause>>     set_clauses;   // SET ...
    std::vector<std::shared_ptr<BinaryExpr>>    conds;         // WHERE ...
};
```

#### 示例对应

```sql
UPDATE grade SET score = 90 WHERE name = 'Calculus';
```

```
UpdateStmt
├── tab_name = "grade"
├── set_clauses = [
│     SetClause
│     ├── col_name = "score"
│     └── val      = IntLit { val=90 }
│   ]
└── conds = [
      BinaryExpr
      ├── lhs = Col { tab_name="", col_name="name" }
      ├── op  = SV_OP_EQ
      └── rhs = StringLit { val="Calculus" }
    ]
```

---

### 3.3 InsertStmt — 插入语句

```cpp
// ast.h:179
struct InsertStmt : public TreeNode {
    std::string                         tab_name;
    std::vector<std::shared_ptr<Value>> vals;   // VALUES 列表，按 schema 顺序
};
```

#### 示例

```sql
INSERT INTO grade VALUES ('Calculus', 2, 92.0);
```

```
InsertStmt
├── tab_name = "grade"
└── vals = [
      StringLit { val="Calculus" }
      IntLit    { val=2 }
      FloatLit  { val=92.0 }
    ]
```

注意：**AST 不检查列数和类型是否和表匹配**，这是 analyze 的活（目前还没做）。

---

### 3.4 DeleteStmt — 删除语句

```cpp
// ast.h:187
struct DeleteStmt : public TreeNode {
    std::string                                 tab_name;
    std::vector<std::shared_ptr<BinaryExpr>>    conds;
};
```

结构是 UpdateStmt 的子集（少了 set_clauses）。

---

### 3.5 CreateTable — 建表语句

```cpp
// ast.h:80
struct CreateTable : public TreeNode {
    std::string                          tab_name;
    std::vector<std::shared_ptr<Field>>  fields;   // Field 的子类是 ColDef
};

// ast.h:72
struct ColDef : public Field {
    std::string                  col_name;
    std::shared_ptr<TypeLen>     type_len;    // { type, len }
};
```

#### 示例

```sql
CREATE TABLE grade (name CHAR(20), id INT, score FLOAT);
```

```
CreateTable
├── tab_name = "grade"
└── fields = [
      ColDef { col_name="name",  type_len=TypeLen{SV_TYPE_STRING, 20} }
      ColDef { col_name="id",    type_len=TypeLen{SV_TYPE_INT,    4}  }
      ColDef { col_name="score", type_len=TypeLen{SV_TYPE_FLOAT,  4}  }
    ]
```

---

## 第四章：表达式节点详解

### 4.1 Expr 抽象基类

```cpp
struct Expr : public TreeNode {};        // 空基类
struct Value : public Expr {};           // 所有字面量的基类
```

为什么要分层：`BinaryExpr::rhs` 是 `shared_ptr<Expr>`，既能指向 `Value` 也能指向 `Col`。`dynamic_pointer_cast` 判断。

### 4.2 字面量（Value 的子类）

| 类 | 字段 | 对应 SQL |
|---|---|---|
| `IntLit` | `int val` | `90` |
| `FloatLit` | `float val` | `90.5` |
| `StringLit` | `std::string val` | `'Calculus'` |
| `BoolLit` | `bool val` | `true`（未使用） |

### 4.3 Col — 列引用

```cpp
// ast.h:146
struct Col : public Expr {
    std::string tab_name;   // 可能为空
    std::string col_name;
};
```

| 用户写法 | tab_name | col_name |
|---|---|---|
| `name` | `""` | `"name"` |
| `grade.name` | `"grade"` | `"name"` |

### 4.4 BinaryExpr — WHERE 里的单个比较

```cpp
// ast.h:162
struct BinaryExpr : public TreeNode {
    std::shared_ptr<Col>     lhs;   // 左边固定是列
    SvCompOp                 op;    // = <> < > <= >=
    std::shared_ptr<Expr>    rhs;   // 右边可能是 Value 也可能是 Col
};
```

注意**左边只能是列**，这是 parser 强制的。所以不会出现 `90 = score` 这种写法。

`SvCompOp` 枚举（`ast.h:25`）：

```cpp
enum SvCompOp { SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE };
```

analyze 会用 `convert_sv_comp_op` 把它映射成 `common.h` 的 `CompOp`。

### 4.5 SetClause — UPDATE 的单个赋值

```cpp
// ast.h:154
struct SetClause : public TreeNode {
    std::string                  col_name;   // 注意：没有 tab_name！
    std::shared_ptr<Value>       val;        // 右边只能是字面量，不能是列引用
};
```

**陷阱**：这是 AST 的 SetClause，和 `common.h:85` 的 `SetClause` 重名但结构不同。analyze 要做翻译。

---

## 第五章：AST 生产全链路

### 5.1 从字符串到 AST 的四步

```
"UPDATE grade SET score=90 WHERE name='Calculus';"
           │
           ▼
  ┌────────────────┐
  │   1. Lexer     │  lex.l 把字符流切成 token 流
  │   (lex.yy.cpp) │  [UPDATE] [IDENT "grade"] [SET] [IDENT "score"]
  └────────┬───────┘  [=] [INT 90] [WHERE] ...
           │
           ▼
  ┌────────────────┐
  │   2. Parser    │  yacc.y 的 grammar 规则匹配 token 序列
  │   (yacc.tab.cpp)│  触发 new ast::UpdateStmt(...)
  └────────┬───────┘
           │
           ▼
  ┌────────────────┐
  │ 3. parse_tree  │  全局变量 ast::parse_tree 指向树根
  │   (ast.h:284)  │
  └────────┬───────┘
           │
           ▼
  ┌────────────────┐
  │  4. do_analyze │  rmdb.cpp 里 yyparse() 之后读 parse_tree
  │  (analyze.cpp) │  dynamic_pointer_cast 分支处理
  └────────────────┘
```

### 5.2 Parser 规则长什么样（了解即可）

```
# yacc.y 节选
updateStmt:
    UPDATE tbName SET setClauses WHERE whereClause
    {
        $$ = std::make_shared<UpdateStmt>($2, $4, $6);
        //                                 ↑   ↑   ↑
        //                              tab_name set_clauses conds
    }
```

读法：`UPDATE tbName SET setClauses WHERE whereClause` 是 token 序列模板，匹配到之后执行 `{}` 里的 C++ 代码，产出一个 `UpdateStmt` 对象。

---

## 第六章：AST 消费端 — analyze 在干什么

### 6.1 为什么不能直接执行 AST

三个原因：

#### (1) 字面量是 AST 节点，不是运行时值
`IntLit{val=90}` 是一个树节点，但 executor 要把它写到记录的 raw buffer，需要 `common.h` 的 `Value { type=TYPE_INT, int_val=90 }`（带类型标记 + 能 `init_raw` 成字节数组）。

#### (2) 列引用缺表名
`Col{tab_name="", col_name="name"}` 没告诉你 name 属于哪张表，分析不了类型、查不到偏移。

#### (3) 没有类型校验
AST 只保证语法对，不保证语义对。`UPDATE grade SET score = 'abc'` 语法合法，语义错误。

### 6.2 analyze 的目标：把 AST 转成 Query

```cpp
// analyze/analyze.h:23
class Query {
public:
    std::shared_ptr<ast::TreeNode>   parse;         // 整棵 AST（optimizer 还会用）
    std::vector<std::string>         tables;        // FROM 的表（已校验存在）
    std::vector<TabCol>              cols;          // 投影列（已补全表名）
    std::vector<Condition>           conds;         // WHERE（Value 已就绪）
    std::vector<SetClause>           set_clauses;   // SET（类型已提升）
    std::vector<Value>               values;        // INSERT VALUES
};
```

### 6.3 典型翻译对照表

| AST 节点 | Query 字段 | 关键工具函数 |
|---|---|---|
| `ast::Value` 子类 | `common.h Value` | `convert_sv_value` |
| `ast::Col` | `TabCol`（带 tab_name） | `check_column` |
| `ast::BinaryExpr` | `Condition` | `get_clause` + `check_clause` |
| `ast::SetClause` | `common.h SetClause` | 手动翻译（UpdateStmt 分支） |
| `ast::SvCompOp` | `CompOp` | `convert_sv_comp_op` |

### 6.4 do_analyze 的骨架

```cpp
shared_ptr<Query> Analyze::do_analyze(shared_ptr<ast::TreeNode> parse) {
    auto query = make_shared<Query>();

    if      (auto x = dynamic_pointer_cast<ast::SelectStmt>(parse)) { /* ... */ }
    else if (auto x = dynamic_pointer_cast<ast::UpdateStmt>(parse)) { /* ... */ }
    else if (auto x = dynamic_pointer_cast<ast::DeleteStmt>(parse)) { /* ... */ }
    else if (auto x = dynamic_pointer_cast<ast::InsertStmt>(parse)) { /* ... */ }
    else { /* DDL / Show / Txn，不需要转换 */ }

    query->parse = move(parse);   // 保留 AST 供 optimizer 再读
    return query;
}
```

**每个分支的模式都是固定的三步**：
1. 搬运表名到 `query->tables`
2. 遍历 AST 列表字段，挨个翻译成 Query 里的列表字段
3. 对 WHERE 调用 `get_clause` + `check_clause`

---

## 第七章：常见陷阱

### 陷阱 1：AST 的 SetClause vs common.h 的 SetClause

```cpp
// AST 版（分析前）
ast::SetClause { col_name: string,        val: shared_ptr<Value> }

// 运行时版（分析后）
common.h SetClause { lhs: TabCol,         rhs: Value }
```

两者同名但字段完全不同。`analyze.cpp` 里两种都会用，注意 `using` 或者加 `ast::` 前缀区分。

### 陷阱 2：`Col.tab_name` 可能为空

用户写 `WHERE name = 'x'` 时，parser 给你的 `Col{tab_name="", col_name="name"}`，你要自己推断出是哪张表的列。`check_column` 做这件事。

### 陷阱 3：Value 有两次转换

- 第一次：`convert_sv_value` 把 `ast::IntLit` 转成 `common.h Value`
- 第二次：`Value::init_raw(len)` 把 `Value` 转成 raw 字节（写进记录或用于比较）

两者之间如果类型不对（例如 FLOAT 列但 Value 还是 TYPE_INT），必须先做类型提升再 `init_raw`，否则 `init_raw` 的 `assert(len == sizeof(int))` 会崩。

### 陷阱 4：`dynamic_pointer_cast` 顺序敏感

```cpp
if      (cast<Value>(expr))  { ... }
else if (cast<IntLit>(expr)) { ... }   // 永远不进来！IntLit 是 Value 的子类
```

父类 cast 会"吞掉"子类，要么倒过来写，要么 cast 具体子类。

---

## 第八章：一页速查表

| 你要处理的 SQL 成分 | 对应 AST struct | 关键字段 |
|---|---|---|
| `SELECT` 整句 | `SelectStmt` | `cols / tabs / conds / order` |
| `UPDATE` 整句 | `UpdateStmt` | `tab_name / set_clauses / conds` |
| `INSERT` 整句 | `InsertStmt` | `tab_name / vals` |
| `DELETE` 整句 | `DeleteStmt` | `tab_name / conds` |
| `CREATE TABLE` | `CreateTable` | `tab_name / fields (ColDef)` |
| 数字字面量 `90` | `IntLit` | `int val` |
| 浮点字面量 `90.5` | `FloatLit` | `float val` |
| 字符串字面量 `'abc'` | `StringLit` | `string val` |
| 列引用 `t.c` | `Col` | `tab_name / col_name` |
| 比较 `a > b` | `BinaryExpr` | `lhs(Col) / op / rhs(Expr)` |
| 赋值 `c = v` | `SetClause` | `col_name / val(Value)` |
| 类型声明 `INT / CHAR(n)` | `TypeLen` | `type / len` |
| 列定义 `c INT` | `ColDef` | `col_name / type_len` |

---

## 第九章：动手实验

### 实验 1：打印任意 SQL 的 AST 结构

项目里有现成的 `@/home/simpur/rmdb_2025/db2025/rmdb/src/parser/ast_printer.h`，可以递归打印 AST。可以写一个小工具：

```cpp
// 伪代码
YY_BUFFER_STATE buf = yy_scan_string("update grade set score = 90 where name = 'A';");
yyparse();
ast_print(ast::parse_tree);
```

会输出缩进格式的树形结构，非常适合验证你的"心算 AST"对不对。

### 实验 2：手画三句 SQL 的 AST

现在合上这份文档，手画下面三句的 AST（纸笔或脑内），再回来对照：

```sql
SELECT id FROM grade WHERE name = 'Data Structure';
DELETE FROM grade WHERE score > 90;
INSERT INTO grade VALUES ('Math', 3, 88.0);
```

画对了，第三章就真懂了。

---

## 第十章：学完这份文档之后可以做什么

- 读懂 `analyze.cpp` 的 SelectStmt / DeleteStmt 分支每一行在做什么
- 自己写出 UpdateStmt 分支的翻译代码
- 读懂 `execution_manager.cpp` 里 `dynamic_pointer_cast<ast::SelectStmt>` 之类的判断
- 将来要加新 SQL 语法时，知道要动哪几个模块（parser/ast/analyze/optimizer/execution）

---

**完**
