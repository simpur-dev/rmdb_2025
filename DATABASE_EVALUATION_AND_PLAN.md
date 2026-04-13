# RMDB 数据库内核评估与完善方案

作为一名在数据库内核开发领域深耕二十年的专家，我对你目前所处的 RMDB (2025年全国大学生计算机系统能力大赛数据库系统设计赛) 项目代码和架构进行了全面且深入的评估。

该项目采用经典的单机关系型数据库架构（System R / Volcano 模型），模块划分清晰，是理解和实现数据库核心机制（如存储引擎、B+树索引、查询执行、事务与恢复）的极佳框架。

以下是针对该代码库的**全面评估报告**以及**系统性的完善研发流程（Roadmap）**。

---

## 一、 当前代码库与架构全面评估

### 1. 架构概览与优势 (Strengths)
* **模块化设计优秀**：项目自底向上严格分层，分为：`storage`（存储与磁盘）、`replacer`（缓冲池替换策略）、`record`（记录与插槽页管理）、`index`（B+树索引）、`system`（元数据/目录管理）、`execution`（火山模型执行器）、`parser/analyze/optimizer`（SQL解析与优化）、`transaction/recovery`（事务与日志）。
* **标准 SQL 流水线**：从 Lex/Yacc 的 Parser 生成 AST，到 Binder (Analyze) 语义绑定，再到 Planner/Optimizer 生成物理计划，最后交由 Executor 迭代执行，符合成熟数据库（如 PostgreSQL）的经典处理流。
* **工程脚手架完备**：具备完整的 CMake 构建系统、GTest 单元测试框架以及预先定义好的数据结构和错误处理机制，开发者可以专注于核心逻辑的填空。

### 2. 核心缺失与待完善点 (Gaps & Weaknesses)
通过对全代码库 `Todo` 的静态分析，当前数据库的“骨架”已搭建完成，但核心“血肉”尚未实现。主要缺失如下：
* **存储与内存管理 (Storage & Buffer Pool)**：`disk_manager.cpp` 中的底层文件 I/O（`read_page`, `write_page` 等）尚未实现；`lru_replacer.cpp` 中的 LRU 缓存淘汰策略为空；`buffer_pool_manager.cpp` 中获取、刷盘、淘汰数据页的核心逻辑缺失。
* **数据组织与索引 (Record & Index)**：`rm_file_handle.cpp` 中基于 Slotted Page 的记录增删改查逻辑未完成；**最核心的挑战**——`ix_index_handle.cpp` 中的 B+ 树操作（节点分裂、合并、插入、扫描）具有多达 18 处空缺。
* **查询分析引擎 (Analyze & Optimizer)**：在 `analyze.cpp` 中，缺乏对表是否存在、列是否合法的语义校验（Semantic Check）。
* **事务与崩溃恢复 (Transaction & Recovery)**：`transaction_manager.cpp` 中的事务生命周期（Begin, Commit, Abort）及其锁释放、回滚操作未实现；LogManager 和崩溃恢复机制（ARIES 协议）亟待补全。

---

## 二、 数据库内核完善流程 (Development Roadmap)

数据库内核的开发具有极强的**底层依赖性**（上层逻辑严重依赖底层的正确性）。因此，必须采取**自底向上 (Bottom-Up)** 的研发流程，并辅以严格的 TDD（测试驱动开发）。

建议按照以下 5 个阶段（Phases）推进：

### 阶段一：筑基 —— 存储引擎与缓冲池 (Storage & Buffer Pool)
这是数据库的根基，任何上层的读写最终都会落到这里。
1. **Disk Manager (`storage/disk_manager.cpp`)**：
   * 实现基于 Linux 系统调用（`open`, `read`, `write`, `lseek`, `close`）的文件页级别读写。
   * 注意处理文件偏移量与 `page_no` 的映射关系，并保证强健的异常处理。
2. **LRU Replacer (`replacer/lru_replacer.cpp`)**：
   * 使用 `std::list` 和 `std::unordered_map` 实现 O(1) 复杂度的 LRU 缓存淘汰策略。
   * 实现 `victim`, `pin`, `unpin` 操作，注意 `std::scoped_lock` 的并发控制。
3. **Buffer Pool Manager (`storage/buffer_pool_manager.cpp`)**：
   * 将 Disk 和 Replacer 结合，实现 `fetch_page`, `flush_page`, `unpin_page`, `new_page` 等逻辑。
   * **陷阱提示**：小心处理 Page 的 `pin_count` 和 `is_dirty` 标记，避免内存泄漏或脏页丢失。

### 阶段二：数据骨架 —— 记录管理与 B+ 树索引 (Record & Index)
此阶段是整个项目中算法最复杂、最容易出现 Bug 的部分。
1. **Record Manager (`record/rm_file_handle.cpp` & `rm_scan.cpp`)**：
   * 理解 Slotted Page（定长/变长槽位页）结构。
   * 实现元组（Tuple）在页内的插入、删除（维护空闲位图或链表）以及基于 `Rid` (Record ID) 的点查。
2. **B+ Tree Index (`index/ix_index_handle.cpp`)**：
   * **第一步**：实现 B+ 树的查找和节点内二分查找。
   * **第二步**：实现插入逻辑，重点攻克**节点分裂 (Split)** 以及向上递归插入父节点。
   * **第三步**：实现删除逻辑，重点攻克**借用兄弟节点 (Redistribute)** 和**节点合并 (Merge)**。
   * **陷阱提示**：务必多写极其边界的单元测试（如连续插入递增序列、随机插入、大规模删除导致根节点坍缩）。

### 阶段三：执行引擎 —— 火山模型执行器 (Execution)
底层数据能存能取后，需要让 SQL 能够跑起来。
1. **系统目录 (System Catalog)**：确保可以正常创建表、创建索引，并将元数据序列化到磁盘。
2. **算子实现 (Executors)**：
   * 实现 `executor_seq_scan.h` (全表扫描)。
   * 实现 `executor_index_scan.h` (利用上一阶段的 B+ 树进行索引扫描)。
   * 实现 `executor_insert/delete/update`，注意维护数据时**必须同步更新索引**。
   * 实现 `executor_nestedloop_join` 等关联查询算子。

### 阶段四：查询大脑 —— 分析器与优化器 (Analyze & Optimizer)
为 SQL 提供语义校验和简单的执行计划优化。
1. **语义分析 (`analyze/analyze.cpp`)**：补全 `Todo`，检查 SQL 中的表名、列名是否存在于 Catalog 中，类型是否匹配。
2. **查询优化 (`optimizer/planner.cpp`)**：
   * 将逻辑计划转化为物理计划。
   * **关键优化**：基于 WHERE 条件判断是否可以使用索引（将 SeqScan 优化为 IndexScan）。

### 阶段五：ACID 保证 —— 事务与崩溃恢复 (Transaction & Recovery)
实现企业级数据库的核心要求：并发安全与数据不丢失。
1. **Transaction Manager (`transaction/transaction_manager.cpp`)**：
   * 实现两阶段锁协议（2PL）的获取与释放。
   * 实现 `Abort` 回滚逻辑（依赖于 Undo Log，或内存中的反向操作缓存）。
2. **Log & Recovery (`recovery/log_manager.h` 等)**：
   * 实现 WAL (Write-Ahead Logging) 预写日志机制。缓冲池刷脏页前必须先刷日志。
   * 实现类似 ARIES 协议的三阶段恢复：**Analysis**（找脏页和活跃事务）、**Redo**（重做历史）、**Undo**（回滚未提交事务）。

---

## 三、 专家研发建议与避坑指南

1. **测试驱动 (TDD)**：千万不要等所有代码写完再编译运行！写完 DiskManager 立刻跑对应的 GTest；写完 BufferPool 立刻跑 `SimpleTest` 和 `LRUTest`。
2. **内存泄漏**：C++ 开发内核极易内存溢出。注意 `unit_test.cpp` 中提示的 "fix detected memory leaks"，确保 `new` 和 `delete` 成对出现，善用智能指针，确保所有的 Buffer Frame 在测试结束时 `pin_count == 0`。
3. **并发死锁**：在 BufferPool 和 Index 的开发中会有多线程并发测试（Latch Crabbing / Lock Coupling），注意加锁顺序，避免死锁（Deadlock）。
4. **日志调试**：在 B+ 树相关的代码中，编写一个能将整棵树以 Graphviz / ASCII 形式打印出来的 Debug 函数，它将为你节省 80% 的排错时间。

祝你在 2025 年的数据库系统设计赛中披荆斩棘！如果你准备好开始，我们可以先从**阶段一（Disk Manager 和 LRU Replacer）**的底层代码实现着手。
