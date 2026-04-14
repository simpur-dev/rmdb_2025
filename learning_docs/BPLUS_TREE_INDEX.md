# B+树索引详解 —— 基于 RMDB 源码分析

> 本文档基于 `rmdb/src/index/` 目录下的源码，结合数据库原理，系统阐述 B+树索引的概念、作用、实现细节及其与同类索引结构的对比。

---

## 一、B+树是什么

### 1.1 基本定义

B+树（B-Plus Tree）是一种**自平衡的多路搜索树**，广泛用于数据库系统和文件系统的索引结构。它是 B 树的变种，具有以下核心性质：

- **所有数据记录（或指向数据记录的指针）只存储在叶子节点**，内部节点仅存储索引键（路由键）。
- **叶子节点之间通过双向链表相连**，支持高效的范围查询和顺序扫描。
- **所有叶子节点处于同一层**，保证从根到任意叶子的路径长度相同，查找时间复杂度稳定为 O(log_m N)，其中 m 为阶数（每个节点的最大子节点数），N 为总记录数。

### 1.2 B+树的阶（Order）

在 RMDB 中，B+树的阶（`btree_order_`）由页面大小和索引列总长度动态计算：

```
btree_order = (PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1
```

即在一个磁盘页面中，扣除页头元数据后，能容纳的最大键值对数量。实际上还**多保留了一个空位**，用于在分裂前临时存放新插入的键值对。

### 1.3 节点结构

RMDB 中每个 B+树节点对应一个磁盘页面（Page），其内存布局如下：

```
|  IxPageHdr  |       keys 区域        |       rids 区域        |
|  (页头元数据)  | key_0 | key_1 | ... | key_n | rid_0 | rid_1 | ... | rid_n+1 |
```

- **IxPageHdr**（`ix_defs.h: IxPageHdr`）：包含 `parent`（父节点页号）、`num_key`（当前键数量）、`is_leaf`（是否叶子）、`prev_leaf` / `next_leaf`（叶子双向链表指针）。
- **keys**：存储索引键，支持多列复合索引，每个 key 的长度为 `col_tot_len_`。
- **rids**：
  - 叶子节点中，`rid[i]` 存储第 i 个 key 对应的**记录物理位置** `Rid{page_no, slot_no}`。
  - 内部节点中，`rid[i]` 存储第 i 个**子节点的页号**（`rid[i].page_no`），共有 `num_key + 1` 个子指针。

---

## 二、B+树解决了什么问题

### 2.1 全表扫描的性能瓶颈

没有索引时，数据库查找特定记录需要**顺序扫描所有数据页**，时间复杂度为 O(N)。当表有百万甚至亿级记录时，这是不可接受的。

### 2.2 B+树索引的核心价值

| 问题 | B+树如何解决 |
|------|------------|
| **等值查询慢** | 从根到叶只需 O(log_m N) 次磁盘 I/O，通常 3~4 层即可覆盖数十亿条记录 |
| **范围查询低效** | 叶子节点通过双向链表连接，定位到起始位置后可顺序遍历，无需回溯内部节点 |
| **磁盘 I/O 开销大** | 每个节点恰好对应一个磁盘页面，最大化单次 I/O 获取的索引信息量 |
| **插入/删除后树退化** | 自平衡机制（分裂 + 合并/重分配）保证树高始终均衡 |
| **并发访问冲突** | 通过 Crabbing（Latch-Coupling）协议，允许多线程安全地并发读写 |

### 2.3 在 RMDB 中的具体应用

- **主键索引**：对主键列自动建立 B+树索引，加速 `WHERE pk = ?` 查询。
- **复合索引**：支持多列联合索引（`col_num_` > 1），按字段顺序逐列比较（`ix_compare` 函数）。
- **范围扫描**：`IxScan` 类利用叶子链表实现 `lower_bound` → `upper_bound` 的区间遍历，直接为 `WHERE col BETWEEN a AND b` 服务。

---

## 三、B+树是如何创建和使用的

### 3.1 索引创建流程（`IxManager::create_index`）

```
调用者传入：表文件名 + 索引列元信息 (ColMeta)
        │
        ▼
1. 生成索引文件名：表名_列名1_列名2.idx
2. 创建磁盘文件（DiskManager::create_file）
3. 计算 btree_order = (PAGE_SIZE - sizeof(IxPageHdr)) / (col_tot_len + sizeof(Rid)) - 1
4. 初始化 IxFileHdr（文件头），包含 root_page=2, first_leaf=2, last_leaf=2
5. 写入三个初始页面：
   ┌─────────────────────────────────────────────────┐
   │ Page 0: FILE_HDR_PAGE   ── 存储 IxFileHdr 序列化数据     │
   │ Page 1: LEAF_HEADER_PAGE ── 哨兵叶子头节点（双向链表的头）│
   │ Page 2: INIT_ROOT_PAGE  ── 初始根节点（同时也是叶子节点）│
   └─────────────────────────────────────────────────┘
6. 关闭文件
```

初始时，根节点就是唯一的叶子节点，`LEAF_HEADER_PAGE` 作为叶子链表的哨兵，其 `prev_leaf` 和 `next_leaf` 都指向根节点。

### 3.2 索引打开与关闭

- **打开**（`IxManager::open_index`）：打开 `.idx` 文件，从 Page 0 反序列化 `IxFileHdr`，构造 `IxIndexHandle` 对象。
- **关闭**（`IxManager::close_index`）：将 `IxFileHdr` 序列化写回 Page 0，刷出缓冲区所有脏页，关闭文件。

### 3.3 查找操作（`get_value`）

```
get_value(key)
    │
    ▼
find_leaf_page(key, FIND)    ← 从根节点逐层向下，使用 internal_lookup（upper_bound）
    │                            定位子节点，直到到达叶子节点
    ▼
leaf_node->lower_bound(key)  ← 在叶子节点内使用二分查找定位 key
    │
    ▼
沿叶子链表向后/向前遍历，收集所有匹配的 Rid
    │
    ▼
返回 result (vector<Rid>)
```

**关键函数**：
- `IxNodeHandle::lower_bound`：二分查找第一个 >= target 的位置。
- `IxNodeHandle::upper_bound`：二分查找第一个 > target 的位置。
- `IxNodeHandle::internal_lookup`：在内部节点中调用 `upper_bound` 定位应走的子树。

### 3.4 插入操作（`insert_entry`）

```
insert_entry(key, rid)
    │
    ▼
find_leaf_page(key, INSERT)  ← 使用 Crabbing 协议加写锁
    │
    ▼
leaf->insert_pairs(pos, key, &rid, 1)  ← lower_bound 定位插入位置后插入
    │
    ├── 未满 → 直接返回
    │
    └── 满了（size == max_size）
            │
            ▼
        split(leaf) → 将叶子一分为二
            │          左半部分留在原节点
            │          右半部分移到新节点
            │          更新叶子双向链表
            ▼
        insert_into_parent(old, key, new)
            │
            ├── old 是根节点 → 创建新根节点
            │                   设置两个子指针
            │
            └── old 不是根节点 → 在父节点中插入 (key, new_rid)
                    │
                    └── 父节点也满了 → 递归 split + insert_into_parent
```

**分裂策略**：
- **叶子节点**：从中间位置 `mid = total / 2` 分裂，右半部分的所有键值对复制到新节点，更新双向链表指针。
- **内部节点**：中间键 `key[mid]` 被**上推**到父节点，左右两部分分别保留在旧节点和新节点中。

### 3.5 删除操作（`delete_entry`）

```
delete_entry(key)
    │
    ▼
find_leaf_page(key, DELETE)  ← Crabbing 协议加写锁
    │
    ▼
leaf->erase_pair(pos)        ← 找到 key 并删除
    │
    ▼
maintain_parent(leaf)        ← 若删除的是第一个 key，向上更新父节点的路由键
    │
    ▼
coalesce_or_redistribute(leaf)
    │
    ├── 节点是根 → adjust_root()
    │       ├── 根为叶且空 → 标记树为空
    │       └── 根为内部节点且仅1子 → 子节点升级为新根
    │
    ├── size >= min_size → 无需操作
    │
    ├── 与兄弟键总数 >= 2 * min_size → redistribute()
    │       └── 从兄弟借一个键值对，更新父节点路由键
    │
    └── 否则 → coalesce()
            └── 合并到左兄弟，删除父节点中的路由键
                递归对父节点执行 coalesce_or_redistribute
```

### 3.6 范围扫描（`IxScan`）

`IxScan` 利用叶子链表实现高效的区间遍历：

```cpp
// 典型使用模式
Iid lower = ih->lower_bound(low_key);
Iid upper = ih->upper_bound(high_key);
IxScan scan(ih, lower, upper, bpm);

while (!scan.is_end()) {
    Rid rid = scan.rid();    // 获取当前记录位置
    // ... 使用 rid 读取实际记录 ...
    scan.next();             // 移动到下一个叶子槽位
}
```

`next()` 方法在当前叶子节点的槽位用完时，自动跳转到下一个叶子节点（通过 `next_leaf` 指针），实现无缝的顺序遍历。

### 3.7 并发控制（Crabbing / Latch-Coupling 协议）

RMDB 的 B+树实现了完整的 **Crabbing 协议**（见 `find_leaf_page`）：

| 操作类型 | 加锁策略 |
|---------|---------|
| **FIND** | 自顶向下获取 shared latch，获取子节点 latch 后立即释放父节点 latch |
| **INSERT / DELETE** | 自顶向下获取 exclusive latch；若子节点是"安全的"（不会触发分裂/合并），则释放所有祖先 latch |

**安全节点的判定**：
- **INSERT 安全**：`child->get_size() < max_size - 1`（还有空间不会分裂）
- **DELETE 安全**：`child->get_size() > min_size`（删除后不会下溢）

此外，根节点有一个专门的 `root_latch_`（`std::shared_mutex`），在写操作时独占锁定，防止根节点在分裂/合并时被并发修改。

---

## 四、B+树与其他索引结构的对比

### 4.1 B+树 vs B 树

| 特性 | B+树 | B 树 |
|------|------|------|
| 数据存储位置 | **仅叶子节点** | 所有节点都可存储数据 |
| 叶子链表 | **有**（双向链表） | 无 |
| 范围查询 | **高效**：沿叶子链表顺序遍历 | 需要中序遍历整棵树 |
| 内部节点扇出 | **更高**：因为不存数据，能放更多 key | 较低 |
| 树高 | 通常**更矮**（扇出更高） | 相对更高 |
| 适用场景 | 数据库索引（磁盘 I/O 敏感） | 内存数据结构、文件系统元数据 |

> RMDB 选择 B+树的核心原因：叶子链表使范围查询成为 O(k)（k 为结果数），且内部节点的高扇出减少了树高和磁盘 I/O 次数。

### 4.2 B+树 vs 哈希索引（Hash Index）

| 特性 | B+树 | 哈希索引 |
|------|------|---------|
| 等值查询 | O(log_m N) | **O(1)**（平均） |
| 范围查询 | **支持** | ✗ 不支持 |
| 排序输出 | **天然有序** | ✗ 无序 |
| 前缀匹配 | **支持**（最左前缀） | ✗ 不支持 |
| 哈希冲突 | 无 | 需要处理 |
| 空间效率 | 较好 | 需要预留空桶 |

> 哈希索引在纯等值查询场景下更快，但 B+树的通用性远胜，是关系型数据库的**默认索引选择**。

### 4.3 B+树 vs LSM-Tree

| 特性 | B+树 | LSM-Tree |
|------|------|---------|
| 写性能 | 随机写，中等 | **顺序写，极高** |
| 读性能 | **稳定高效** | 可能需要多层查找 |
| 空间放大 | 低 | 较高（多层冗余） |
| 写放大 | 中等 | 较高（Compaction） |
| 适用场景 | OLTP（读多写少） | 写密集型（日志、时序数据库） |

### 4.4 B+树 vs 跳表（Skip List）

| 特性 | B+树 | 跳表 |
|------|------|------|
| 磁盘友好性 | **极好**：节点对齐磁盘页 | 差：指针分散，缓存不友好 |
| 实现复杂度 | 较高 | **较低** |
| 并发控制 | 需要 latch coupling | 天然支持无锁并发 |
| 适用场景 | 磁盘索引 | 内存索引（如 Redis、LevelDB MemTable） |

---

## 五、RMDB B+树索引模块的文件结构

| 文件 | 职责 |
|------|------|
| `ix_defs.h` | 定义索引相关的常量、`IxFileHdr`（文件头）、`IxPageHdr`（页头）、`Iid`（索引内部标识） |
| `ix_index_handle.h` | 声明 `IxNodeHandle`（节点操作）和 `IxIndexHandle`（B+树整体操作），包含 `ix_compare` 比较函数 |
| `ix_index_handle.cpp` | 实现 B+树的所有核心算法：查找、插入、删除、分裂、合并、重分配、并发控制 |
| `ix_manager.h` | `IxManager` 类：索引文件的创建、打开、关闭、销毁等生命周期管理 |
| `ix_scan.h / .cpp` | `IxScan` 类：基于叶子链表的区间迭代器，实现 `next()` / `is_end()` / `rid()` |
| `ix.h` | 统一头文件，汇总导出 `ix_scan.h` 和 `ix_manager.h` |

---

## 六、关键数据结构速查

### IxFileHdr（索引文件头）

```
first_free_page_no_  → 第一个空闲页
num_pages_           → 文件总页数
root_page_           → B+树根节点页号
col_num_             → 索引列数
col_types_           → 各列类型 (INT / FLOAT / STRING)
col_lens_            → 各列长度
col_tot_len_         → 索引键总长度
btree_order_         → 每个节点最多键数
keys_size_           → keys 区域总字节数 = (btree_order + 1) * col_tot_len
first_leaf_          → 第一个叶子节点页号
last_leaf_           → 最后一个叶子节点页号
```

### IxPageHdr（节点页头）

```
parent      → 父节点页号（根节点为 INVALID_PAGE_ID）
num_key     → 当前键数量
is_leaf     → 是否为叶子节点
prev_leaf   → 前驱叶子页号（仅叶子有效）
next_leaf   → 后继叶子页号（仅叶子有效）
```

### Iid（索引内部标识）

```
page_no  → 叶子节点所在页号
slot_no  → 在叶子节点中的槽位号（即 key/rid 数组下标）
```

> 注意：`Iid` 是索引内部的定位标识，与上层的 `Rid`（记录物理位置）是不同的概念。`Iid` 通过 `get_rid(iid)` 转换为 `Rid`。

---

## 七、总结

B+树是关系型数据库中**最核心的索引结构**，它通过高扇出多路平衡树 + 叶子双向链表的设计，在磁盘 I/O 环境下同时提供了高效的等值查询、范围扫描和有序遍历能力。RMDB 的实现涵盖了 B+树的完整生命周期——从创建、插入、查找、删除到并发控制（Crabbing 协议），是学习数据库索引实现的优秀参考。
