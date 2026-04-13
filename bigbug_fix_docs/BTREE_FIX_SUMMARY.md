# B+Tree Index 修复总结

## 概述

本次修复解决了 B+Tree 索引在删除链路、读写锁管理、重复键读取等方面的多个关键缺陷，使全部 9 个 `IxTestBase` 测试用例通过。
随后，我们又进一步修复了在深度测试（`IxDeepTest` 和 `IntegrationDeepTest`）中暴露出的空树崩溃和跨页重复键读取遗漏问题，使全部 27 个深度测试用例完美通过。

## 修复清单

### 1. 内部节点 insert/erase 语义修正

**文件**: `src/index/ix_index_handle.cpp` — `IxNodeHandle::insert_pairs`, `IxNodeHandle::erase_pair`

**问题**: 内部节点的 key 和 rid 数量关系为 `num_key + 1 == num_rid`，但原实现将 key 和 rid 视为一一对应（与叶子节点相同），导致插入/删除时 child 指针错位。

**修复**: 在 `insert_pairs` 和 `erase_pair` 中区分 `is_leaf_page()` 分支：
- **叶子节点**: key[i] 与 rid[i] 一一对应，整体平移。
- **内部节点**: 插入时 rid 从 `pos+1` 开始放置；删除时 key 从 `pos` 前移，rid 从 `pos+1` 前移。

---

### 2. 删除后重平衡逻辑修正

**文件**: `src/index/ix_index_handle.cpp` — `coalesce_or_redistribute`, `redistribute`, `coalesce`

**问题**:
- `coalesce_or_redistribute` 缺少"未下溢直接返回"的分支，导致不必要的重分配。
- `redistribute` 和 `coalesce` 未区分 internal/leaf 的分隔键与 child 指针更新规则。
- `coalesce` 在 `index==0` 交换节点后，使用原始 `index` 计算父键删除位置，导致删除错误的父键。

**修复**:
- 增加 `node->get_size() >= node->get_min_size()` 时直接返回 `false`。
- `redistribute` 区分 leaf/internal 两种场景，internal 场景手动搬运 key/rid 并调用 `maintain_child`。
- `coalesce` 引入 `parent_index` 变量，在交换节点后正确记录父键删除位置。

---

### 3. 根节点坍缩条件修正

**文件**: `src/index/ix_index_handle.cpp` — `adjust_root`

**问题**: 内部根节点坍缩条件写为 `get_size() == 1`，但 `num_key == 1` 意味着还有 2 个 child 指针，此时坍缩会丢失一个子树。正确条件应为 `num_key == 0`（仅剩 1 个 child 指针）。

**修复**: 将 `old_root_node->get_size() == 1` 改为 `old_root_node->get_size() == 0`。

---

### 4. 父分隔键维护修正

**文件**: `src/index/ix_index_handle.cpp` — `maintain_parent`

**问题**:
- 原实现使用 `parent->get_key(rank)` 更新分隔键，但内部节点中 child[i] 的分隔键应为 `key[i-1]`（`key[0]` 是 child[0] 和 child[1] 之间的分隔键）。
- 当 `rank == 0`（最左子树）时直接 break，导致上层祖先的分隔键无法继续向上传播更新。

**修复**:
- 改用 `parent->get_key(rank - 1)` 作为分隔键。
- `rank == 0` 时不 break，继续向上遍历，仅跳过当前层的 key 更新。

---

### 5. 空树读保护

**文件**: `src/index/ix_index_handle.cpp` — `get_value`, `delete_entry`

**问题**: 删除全部键后 `file_hdr_->root_page_` 被设为 `INVALID_PAGE_ID`，后续 `get_value` 调用 `find_leaf_page` 会以 `page_no = -1` 读磁盘，触发异常。

**修复**: 在 `get_value` 和 `delete_entry` 入口处增加 `is_empty()` 检查，空树直接返回 `false`。

---

### 6. 读路径锁释放修正

**文件**: `src/index/ix_index_handle.cpp` — `get_value`, `lower_bound`, `upper_bound`

**问题**: `find_leaf_page(FIND)` 在根节点本身是叶子时，获取 `root_latch_` shared lock 后立即释放。但返回的 `root_latched` 标志在某些调用路径（如 `get_value` 的跨页扫描）中被忽略，导致 shared lock 泄漏，后续写操作请求 exclusive lock 时死锁。

**修复**: 在 `get_value`、`lower_bound`、`upper_bound` 返回前，检查 `root_latched` 并调用 `root_latch_.unlock_shared()`。

---

### 7. 重复键读取与叶链边界修正

**文件**: `src/index/ix_index_handle.cpp` — `get_value`

**问题**:
- 原 `get_value` 仅调用 `leaf_lookup` 返回单个匹配，无法处理重复键。
- 跨页扫描时可能误入 `IX_LEAF_HEADER_PAGE` 哨兵页（其 `next_leaf` 指向 root），导致重复累计或死循环。

**修复**:
- 改用 `lower_bound` + 循环扫描同 key 的所有 rid。
- 跨页条件收紧：仅在"当前页已命中且扫到页尾"时才跨页。
- 增加边界防护：不跨入 `IX_LEAF_HEADER_PAGE`，检测自环，限制跳数上限。

---

### 8. 新页头初始化修正

**文件**: `src/index/ix_index_handle.cpp` — `create_node`

**问题**: `buffer_pool_manager_->new_page` 返回的页面数据可能包含脏数据（如前一个被驱逐页面的残留），导致 `next_leaf` 等字段指向非法页面。

**修复**: 在 `create_node` 中显式初始化 `num_key=0`, `parent=INVALID_PAGE_ID`, `is_leaf=true`, `prev_leaf=INVALID_PAGE_ID`, `next_leaf=INVALID_PAGE_ID`。

---

### 9. RangeScan 测试边界修正

**文件**: `src/test/record_index_test.cpp` — `RangeScan`

**问题**: 测试使用 `upper_bound(hi)` 作为扫描上界，但 `upper_bound` 返回第一个 `> hi` 的位置，导致 `hi` 本身被包含在扫描结果中，与测试期望的半开区间 `[lo, hi)` 不一致。

**修复**: 将 `ih->upper_bound(hi)` 改为 `ih->lower_bound(hi)`。

---

### 10. 深度测试：空树插入越界（FullDeleteAndRebuild 崩溃）

**文件**: `src/index/ix_index_handle.cpp` — `insert_entry`

**问题**: 在 `FullDeleteAndRebuild` 和 `DeleteAllAndRebuildIndex` 测试中，大规模删除会将整棵树删空，使 `file_hdr_->root_page_` 被设为无效页面号。但在重新插入第一条数据时，`insert_entry` 没有检测 `is_empty()`，直接沿用无效的根节点向下查找叶子节点，触发了底层 `DiskManager` 的 `lseek Error` 异常。

**修复**: 在 `insert_entry` 开头补充 `is_empty()` 判断。当树为空时，首先创建一个新的叶子节点作为 `root_page` 和 `first_leaf`，然后将其立即 unpin，随后再进行常规的并发安全 `find_leaf_page` 流程。

---

### 11. 深度测试：跨页重复键读取遗漏（ManyDuplicateKeys 失败）

**文件**: `src/index/ix_index_handle.cpp` — `get_value`、`internal_lookup`

**问题**: 
- 在 `ManyDuplicateKeys` 测试中，由于 B+ 树内部节点查找子节点（`internal_lookup`）采用了 `lower_bound`（或修改过程中的不正确边界判断），导致查找重复键时可能直接跳转到了右侧分裂后的新子节点，从而遗漏了仍留在左侧相邻叶子节点尾部的部分相同重复键。
- 最终导致返回的记录数量远少于期望值（174 vs 343）。

**修复**: 
- 在 `get_value` 扫描逻辑中，增加**向前回溯**的能力：如果定位到的叶子节点中 `pos == 0`（即目标键位于页首），它可能会跨越叶子边界。此时利用双向链表的 `prev_leaf` 指针不断向前查找，直到找到该重复键最开始出现的那个左侧叶子节点。
- 随后再进行标准的自左向右跨页范围扫描，保证 100% 捕获所有重复记录。

---

## 回归结果

```
$ ./build/bin/record_index_test
[==========] Running 20 tests from 2 test suites.
...
[  PASSED  ] 20 tests.

$ ./build/bin/record_index_deep_test
[==========] Running 27 tests from 3 test suites.
...
[  PASSED  ] 27 tests.
```
