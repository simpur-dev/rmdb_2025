# RMDB 存储引擎与缓冲池（Storage & Buffer Pool）架构图与流程图

> 对应 roadmap 阶段一：**筑基**。
>
> 目标：用一组**架构图 + 流程图**把这一层的结构、交互、关键路径讲清楚。
>
> 代码入口：
> - `rmdb/src/storage/page.h`
> - `rmdb/src/storage/disk_manager.{h,cpp}`
> - `rmdb/src/replacer/lru_replacer.{h,cpp}`
> - `rmdb/src/storage/buffer_pool_manager.{h,cpp}`

---

## 一、整体架构图

### 1.1 分层视角

```
┌────────────────────────────────────────────────────────────┐
│  上层调用方：                                                │
│    RmFileHandle   IxIndexHandle   LogManager                │
└───────────────────────┬────────────────────────────────────┘
                        │  仅依赖 PageId + fetch/unpin/flush
                        ▼
┌────────────────────────────────────────────────────────────┐
│                    BufferPoolManager                        │
│                                                             │
│   page_table_ : PageId → frame_id                          │
│   pages_[]    : Frame 数组（每个 Frame 装一个 Page）          │
│   free_list_  : 空闲帧编号链表                                │
│   latch_      : 全局互斥锁                                    │
└────────┬───────────────────────────────┬────────────────────┘
         │ 需要淘汰谁?                      │ 真正读写磁盘
         ▼                                ▼
┌─────────────────────┐          ┌─────────────────────────┐
│    LRUReplacer      │          │       DiskManager       │
│   LRUlist_  (list)  │          │   path2fd_ / fd2path_   │
│   LRUhash_  (hash)  │          │   fd2pageno_[MAX_FD]    │
└─────────────────────┘          └──────────┬──────────────┘
                                            │ syscall
                                            ▼
                                ┌────────────────────────┐
                                │   Linux 文件系统       │
                                │ open read write lseek  │
                                └────────────────────────┘
```

### 1.2 三大核心集合的不变量

BPM 在任意时刻，每个 `frame_id` 只能属于**下面三种状态之一**：

```
┌──────────────────────────────────────────────────────────┐
│                      Frame 状态机                         │
│                                                          │
│   ┌──────────────┐   new_page / fetch_page             │
│   │  free_list_  │ ─────────────────────┐               │
│   └──────────────┘                      │               │
│          ▲                              ▼               │
│          │ delete_page            ┌───────────────┐     │
│          │                        │  page_table_  │     │
│          │                        │ (pin_count>0) │     │
│          │                        └───────┬───────┘     │
│          │                     unpin      │             │
│          │                  (count→0)     ▼             │
│          │                    ┌────────────────────┐    │
│          │   victim / pin     │   LRUReplacer      │    │
│          └────────────────────│ (可淘汰候选池)      │    │
│                               └────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

**关键结论**：

- **`free_list_`**：从未被使用 or 已被 `delete_page` 清空
- **`page_table_` 中 pin_count > 0**：正在被使用
- **`page_table_` 中 pin_count = 0 且在 LRUReplacer**：可淘汰
- **三者互斥，不能同时出现在两个集合**

### 1.3 数据结构图

```
BufferPoolManager
├── pages_[0..N-1]          ← Frame 连续数组
│     ├── Page.id_          (PageId)
│     ├── Page.data_[4096]  (磁盘数据副本)
│     ├── Page.is_dirty_
│     ├── Page.pin_count_
│     └── Page.latch_       (shared_mutex)
│
├── page_table_ : unordered_map<PageId, frame_id_t>
├── free_list_  : list<frame_id_t>
│
├── replacer_ ──► LRUReplacer
│                  ├── LRUlist_  : list<frame_id_t>     ← 头部=最近,尾部=最久
│                  └── LRUhash_  : unordered_map<frame_id_t, list::iterator>
│
└── disk_manager_ ──► DiskManager
                       ├── path2fd_  : unordered_map<string,int>
                       ├── fd2path_  : unordered_map<int,string>
                       └── fd2pageno_[MAX_FD] : atomic<page_id_t>
```

---

## 二、DiskManager 流程图

### 2.1 职责图

```
┌──────────────────────── DiskManager ────────────────────────┐
│                                                              │
│   文件生命周期                页级 I/O             页号分配     │
│   ─────────────             ──────────            ─────────  │
│   create_file               read_page             allocate_  │
│   open_file                 write_page            page       │
│   close_file                                      (fd2page   │
│   destroy_file                                     no_++)    │
│   is_file / is_dir                                           │
│                                                              │
│   日志 I/O                                                    │
│   ─────────                                                  │
│   read_log / write_log                                       │
└──────────────────────────────────────────────────────────────┘
```

### 2.2 `write_page` 流程

```
write_page(fd, page_no, buf, num_bytes)
      │
      ▼
off_t offset = (off_t)page_no * PAGE_SIZE   ← 防止 32 位乘法溢出
      │
      ▼
retry_lseek(fd, offset, SEEK_SET) ───► 失败 ► throw InternalError
      │
      ▼
retry_write_full(fd, buf, num_bytes)
      │           └── 循环处理短写 + EINTR
      ▼
written == num_bytes?  ── no ──► throw InternalError
      │ yes
      ▼
    return
```

### 2.3 `read_page` 流程

```
read_page(fd, page_no, buf, num_bytes)
      │
      ▼
计算 offset ─► lseek ─► 循环 read
      │                   │
      │                   └─ 处理 EINTR 与短读
      ▼
读取字节数 == num_bytes ?  ── no ──► throw InternalError
      │ yes
      ▼
    return
```

### 2.4 `open_file / close_file` 流程

```
open_file(path)                        close_file(fd)
   │                                      │
   ▼                                      ▼
lock fd_map_latch_                     lock fd_map_latch_
   │                                      │
   ▼                                      ▼
path2fd_.count(path)?                  fd2path_.count(fd)?
   ├─ 有 ──► FileExistsError            ├─ 无 ──► FileNotOpenError
   │                                      │
   ▼                                      ▼
is_file(path)?                         ::close(fd)  (循环处理 EINTR)
   ├─ 否 ──► FileNotFoundError            │
   │                                      ▼
   ▼                                   erase path2fd_ & fd2path_
fd = ::open(path, O_RDWR)
   │
   ▼
再次加锁 → 二次校验 path2fd_
   │
   ▼
path2fd_[path] = fd
fd2path_[fd]   = path
   │
   ▼
return fd
```

---

## 三、LRUReplacer 流程图

### 3.1 数据结构

```
    最近访问 ◄──────────────── LRUlist_ ────────────────► 最久未用（淘汰目标）
       head                                                  tail
        │                                                     ▲
        │          LRUhash_: frame_id → iterator              │
        │                                                     │
        └─────────────────────────────────────────────────────┘
                       O(1) 定位并摘除任意帧
```

### 3.2 三个核心操作

```
unpin(frame_id)                         pin(frame_id)
   │                                       │
   ▼                                       ▼
scoped_lock(latch_)                     scoped_lock(latch_)
   │                                       │
   ▼                                       ▼
LRUhash_.count(frame_id)?              LRUhash_.find(frame_id)?
   ├─ 有 ──► return (幂等)                ├─ 无 ──► return (幂等)
   │                                       │
   ▼                                       ▼
LRUlist_.push_front(frame_id)          LRUlist_.erase(iter)
LRUhash_[frame_id] = begin()           LRUhash_.erase(frame_id)


victim(&frame_id)
   │
   ▼
scoped_lock(latch_)
   │
   ▼
LRUlist_.empty()? ── yes ──► return false
   │
   ▼
*frame_id = LRUlist_.back()
LRUhash_.erase(*frame_id)
LRUlist_.pop_back()
   │
   ▼
return true
```

---

## 四、BufferPoolManager 关键流程

### 4.1 `fetch_page`（最核心路径）

```
fetch_page(page_id)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
page_table_.find(page_id)
      │
      ├── 命中 ─────────────────────────┐
      │                                 ▼
      │                           page = pages_[frame_id]
      │                           page.pin_count_++
      │                           replacer_->pin(frame_id)
      │                           return page
      │
      └── 未命中
            │
            ▼
      find_victim_page(&frame_id)
            │
            ├── free_list_ 非空 ── 取 front()，返回
            │
            └── 否则 replacer_->victim()
                      │
                      └── 失败 ──► return nullptr
            │
            ▼
      update_page(page, page_id, frame_id)
            ├─ 若 page->is_dirty_ → write_page()
            ├─ erase page_table_[page->id_]
            └─ page_table_[new_id] = frame_id
               page->reset_memory()
               page->id_ = new_id
            │
            ▼
      disk_manager_->read_page(page_id, page->data_)
      page->pin_count_ = 1
      replacer_->pin(frame_id)
            │
            ▼
      return page
```

### 4.2 `unpin_page`

```
unpin_page(page_id, is_dirty)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
page_table_.find(page_id) ── 未命中 ──► return false
      │
      ▼
page = pages_[frame_id]
      │
      ▼
pin_count_ <= 0 ? ──► return false
      │ no
      ▼
pin_count_--
      │
      ▼
pin_count_ == 0 ? ── yes ──► replacer_->unpin(frame_id)
      │
      ▼
is_dirty ? ── yes ──► page->is_dirty_ = true
      │
      ▼
return true
```

### 4.3 `new_page`

```
new_page(&page_id)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
find_victim_page(&frame_id) ── 失败 ──► return nullptr
      │
      ▼
page_id->page_no = disk_manager_->allocate_page(page_id->fd)
      │
      ▼
update_page(page, *page_id, frame_id)
      │    （若旧页脏，内部会先写回磁盘）
      ▼
page->pin_count_ = 1
replacer_->pin(frame_id)
      │
      ▼
return page
```

### 4.4 `flush_page`

```
flush_page(page_id)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
page_table_.find(page_id) ── 未命中 ──► return false
      │
      ▼
disk_manager_->write_page(...)          ← 不论脏否都写盘（规范要求）
page->is_dirty_ = false
      │
      ▼
return true
```

### 4.5 `delete_page`

```
delete_page(page_id)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
page_table_.find(page_id)
      │
      ├── 未命中 ── return true（已不存在即视为成功）
      │
      └── 命中
            │
            ▼
      pin_count_ != 0 ? ── yes ──► return false
            │ no
            ▼
      若 is_dirty_ → write_page()
            │
            ▼
      page_table_.erase(page_id)
      replacer_->pin(frame_id)      ← 把帧从 LRU 摘出
            │
            ▼
      page->reset_memory()
      page->id_ = INVALID
            │
            ▼
      free_list_.emplace_back(frame_id)
            │
            ▼
      return true
```

### 4.6 `flush_all_pages`

```
flush_all_pages(fd)
      │
      ▼
scoped_lock(latch_)
      │
      ▼
for (page_id, frame_id) in page_table_:
      if page_id.fd == fd and page->is_dirty_:
            disk_manager_->write_page(...)
            page->is_dirty_ = false
```

---

## 五、端到端典型时序

### 5.1 读路径：上层 `fetch → 用 → unpin`

```
Executor / RmFileHandle             BufferPool                 DiskManager
        │                               │                            │
        │  fetch_page(pid)              │                            │
        ├──────────────────────────────►│                            │
        │                               │ page_table_ 命中?          │
        │                               │   └─ 否 ──► victim         │
        │                               │               └─ 若脏 ► write_page
        │                               │                            │
        │                               │  read_page(pid)            │
        │                               ├───────────────────────────►│
        │                               │◄──────────────── data ─────┤
        │                               │                            │
        │                               │  pin_count_=1              │
        │◄────────────── page ──────────┤                            │
        │                               │                            │
        │  （读 data_ 使用）             │                            │
        │                               │                            │
        │  unpin_page(pid, false)       │                            │
        ├──────────────────────────────►│                            │
        │                               │  pin_count_--              │
        │                               │  若 ==0 → replacer->unpin  │
        │◄──────────── true ────────────┤                            │
```

### 5.2 写路径：上层 `fetch → 改 → unpin(dirty=true)`

```
        │ fetch_page(pid) ─────────────► BPM ───────► 命中或 IO 上来
        │                                 │
        │ 修改 data_ 内容                   │
        │ unpin_page(pid, is_dirty=true)──►│ page->is_dirty_ = true
        │                                 │
        │ （后续 flush_page 或 victim 时 写回磁盘）
```

### 5.3 替换路径：缓冲池满 + fetch 新页

```
                  ┌─────────────── BufferPool 已满 ──────────────┐
                  │                                              │
fetch_page(new)   │  free_list_.empty() = true                   │
────────────────► │                                              │
                  │  replacer_->victim(&f)                       │
                  │        ↓                                     │
                  │  pages_[f] 是脏页?  ── yes → write_page      │
                  │        ↓                                     │
                  │  page_table_.erase(old_id)                   │
                  │  page_table_[new_id] = f                     │
                  │  read_page(new_id)                           │
                  │  pin_count_=1; replacer_->pin(f)             │
                  │                                              │
                  └──────────────────────────────────────────────┘
```

---

## 六、关键点速查表

| 场景 | 必须调用 | 原因 |
|---|---|---|
| 读/写一个页前 | `fetch_page` | 拿到稳定的内存副本并 pin 住 |
| 使用完毕 | `unpin_page` | 归还 pin_count，脏则标记 |
| 页不再需要 | `delete_page` | 回收到 free_list |
| 关闭文件前 | `flush_all_pages(fd)` | 防止脏页丢失 |
| 建新页 | `new_page` | 分配 page_no + 占据一个 Frame |
| 强制落盘 | `flush_page` | 无条件 write_page |

| Frame 所在位置 | pin_count | 是否在 LRU |
|---|---|---|
| `free_list_` | 0 | ❌ |
| `page_table_` 使用中 | > 0 | ❌ |
| `page_table_` 空闲 | 0 | ✅ |

---

## 七、一句话收尾

- **DiskManager** 是磁盘抽象：把 `page_no` 翻译成 `lseek+read/write`
- **LRUReplacer** 是淘汰决策器：`list+hash` 做 O(1) LRU
- **BufferPoolManager** 是调度中心：维护 `free_list / page_table / replacer` 三大集合的一致性，配合 DiskManager 完成 fetch / flush / evict / new / delete 的全部语义

> 看懂这一层的**三大集合不变量**和 **fetch_page 流程**，整个阶段一的知识就算吃透了。
