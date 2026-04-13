# 记录管理与 B+ 树索引 — 模块架构图

## 一、整体模块关系

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          上层调用 (Executor)                            │
│              SeqScanExecutor / IndexScanExecutor / Insert / Delete      │
└──────────┬──────────────────────────────────────┬───────────────────────┘
           │                                      │
           ▼                                      ▼
┌─────────────────────────┐          ┌──────────────────────────────┐
│   Record Manager 模块    │          │     B+ Tree Index 模块        │
│                         │          │                              │
│  RmManager              │          │  IxManager                   │
│    ├── create_file()    │          │    ├── create_index()        │
│    ├── open_file()      │          │    ├── open_index()          │
│    ├── close_file()     │          │    ├── close_index()         │
│    └── destroy_file()   │          │    └── destroy_index()       │
│                         │          │                              │
│  RmFileHandle           │          │  IxIndexHandle               │
│    ├── get_record()     │◄─Rid─────┤    ├── insert_entry()       │
│    ├── insert_record()──┼──Rid────►│    ├── delete_entry()       │
│    ├── delete_record()  │          │    ├── get_value()           │
│    ├── update_record()  │          │    ├── lower_bound()         │
│    └── is_record()      │          │    └── upper_bound()         │
│                         │          │                              │
│  RmScan                 │          │  IxScan                      │
│    ├── next()           │          │    ├── next()                │
│    ├── is_end()         │          │    ├── is_end()              │
│    └── rid()            │          │    └── rid()                 │
└──────────┬──────────────┘          └──────────────┬───────────────┘
           │                                        │
           └────────────────┬───────────────────────┘
                            ▼
              ┌──────────────────────────┐
              │   Storage Engine 模块     │
              │                          │
              │  BufferPoolManager       │
              │    ├── fetch_page()      │
              │    ├── new_page()        │
              │    ├── unpin_page()      │
              │    └── flush_all_pages() │
              │                          │
              │  DiskManager             │
              │    ├── read_page()       │
              │    ├── write_page()      │
              │    └── create/open/close │
              │                          │
              │  Page                    │
              │    ├── get_data()        │
              │    ├── latch_lock()      │
              │    └── latch_unlock()    │
              └──────────────────────────┘
```

## 二、Record Manager 模块架构

### 2.1 类层次与职责

```
RmManager (文件级生命周期管理)
  │
  │  open_file() → unique_ptr<RmFileHandle>
  ▼
RmFileHandle (表级记录操作)         RmScan (全表顺序扫描)
  │  持有:                             │  持有:
  │  ├── disk_manager_                 │  ├── file_handle_ (const)
  │  ├── buffer_pool_manager_          │  └── rid_ (当前位置)
  │  ├── fd_ (文件描述符)               │
  │  └── file_hdr_ (文件头元数据)       │  接口: next() / is_end() / rid()
  │                                    │
  │  公开接口:                          │  遍历方式:
  │  ├── get_record(Rid)               │  page_no: 1 → num_pages-1
  │  ├── insert_record(buf)            │  slot_no: Bitmap::next_bit(true,...)
  │  ├── insert_record(Rid, buf)       │
  │  ├── delete_record(Rid)            │
  │  └── update_record(Rid, buf)       │
  │                                    
  │  内部辅助:                          
  │  ├── fetch_page_handle(page_no)    
  │  ├── create_new_page_handle()      
  │  ├── create_page_handle()          
  │  └── release_page_handle()         
  ▼
RmPageHandle (单页封装，值语义)
  ├── file_hdr   → RmFileHdr*
  ├── page       → Page*
  ├── page_hdr   → RmPageHdr*   (页内元数据)
  ├── bitmap     → char*        (slot占用位图)
  └── slots      → char*        (定长记录数组)
```

### 2.2 页面物理布局

```
Record Data Page (page_no ≥ 1):
┌──────────┬───────────────┬──────────────────┬──────────────────────────┐
│   LSN    │  RmPageHdr    │     Bitmap       │        Slots             │
│  (4B)    │  (8B)         │  (bitmap_size B) │  (record_size × N)       │
├──────────┼───────────────┼──────────────────┼──────────────────────────┤
│ OFFSET=0 │ OFFSET=4      │                  │                          │
│          │ next_free: int│ 0/1 per slot     │ slot[0] slot[1] ... [N-1]│
│          │ num_rec:   int│                  │                          │
└──────────┴───────────────┴──────────────────┴──────────────────────────┘

File Header Page (page_no = 0):
┌─────────────────────────────────────────────┐
│ RmFileHdr                                   │
│  ├── record_size:            int            │
│  ├── num_pages:              int            │
│  ├── num_records_per_page:   int            │
│  ├── first_free_page_no:     int  → 空闲链表 │
│  └── bitmap_size:            int            │
└─────────────────────────────────────────────┘
```

### 2.3 空闲页链表

```
file_hdr_.first_free_page_no
         │
         ▼
    ┌─────────┐     ┌─────────┐     ┌─────────┐
    │ Page A  │────►│ Page B  │────►│ Page C  │────► RM_NO_PAGE (-1)
    │ 未满    │ next│ 未满    │ next│ 未满    │ next
    └─────────┘     └─────────┘     └─────────┘

insert_record: 从链表头取页 → 插入 → 若满则摘掉链表头
delete_record: 删除slot → 若从满→未满 → 头插法链入空闲链表
```

### 2.4 关键操作流程

```
insert_record(buf):
  create_page_handle()                     ← 获取/创建空闲页
    ├── 有空闲页 → fetch_page_handle(first_free)
    └── 无空闲页 → create_new_page_handle() → 链入空闲链表
  Bitmap::first_bit(false, ...) → slot_no  ← 找空闲slot
  Bitmap::set + memcpy                     ← 写入数据
  num_records++
  if (满) → first_free = next_free         ← 满页摘链
  unpin_page(dirty=true)
  return Rid{page_no, slot_no}

delete_record(Rid):
  fetch_page_handle(page_no)
  Bitmap::reset + num_records--
  if (从满→未满) → release_page_handle()    ← 头插空闲链表
  unpin_page(dirty=true)
```

---

## 三、B+ Tree Index 模块架构

### 3.1 类层次与职责

```
IxManager (索引文件生命周期管理)
  │
  │  open_index() → unique_ptr<IxIndexHandle>
  ▼
IxIndexHandle (B+树操作核心)          IxScan (叶节点范围扫描)
  │  持有:                              │  持有:
  │  ├── disk_manager_                  │  ├── ih_ (const)
  │  ├── buffer_pool_manager_           │  ├── iid_ (当前位置)
  │  ├── fd_                            │  ├── end_ (终止位置)
  │  ├── file_hdr_ → IxFileHdr*        │  ├── bpm_
  │  └── root_latch_ (shared_mutex)    │  └── current_page_
  │                                     │
  │  公开接口:                           │  接口:
  │  ├── get_value(key)                 │    next() / is_end() / rid()
  │  ├── insert_entry(key, Rid, txn)   │
  │  ├── delete_entry(key, txn)        │  遍历: 叶子链表 + shared latch
  │  ├── lower_bound(key)              │
  │  └── upper_bound(key)              │
  │                                     
  │  内部核心:                           
  │  ├── find_leaf_page()   ← Crabbing协议
  │  ├── split()            ← 节点分裂
  │  ├── insert_into_parent()
  │  ├── coalesce_or_redistribute()
  │  ├── coalesce()         ← 节点合并
  │  └── redistribute()     ← 借用兄弟
  ▼
IxNodeHandle (B+树节点封装)
  ├── file_hdr  → IxFileHdr*
  ├── page      → Page*
  ├── page_hdr  → IxPageHdr*
  ├── keys      → char*       (排序键数组)
  └── rids      → Rid*        (叶子=记录Rid / 内部=子页号)
```

### 3.2 索引文件页面布局

```
Index File 页面组织:
┌────────────────┬────────────────┬────────────────┬─────────────────┐
│ Page 0         │ Page 1         │ Page 2         │ Page 3,4,...    │
│ IxFileHdr      │ Leaf Header    │ Root Node      │ 其他节点        │
│ (序列化元数据)  │ (哨兵叶子)     │ (初始为叶节点)  │                 │
└────────────────┴────────────────┴────────────────┴─────────────────┘

B+树节点页面 (Internal / Leaf):
┌──────────┬───────────────────────┬──────────────────────────────────┐
│   LSN    │     IxPageHdr         │      Keys + Rids                 │
│  (4B)    │  next_free_page_no    │                                  │
│          │  parent               │  key[0] key[1] ... key[n-1]      │
│          │  num_key              │  rid[0] rid[1] ... rid[n]        │
│          │  is_leaf              │                                  │
│          │  prev_leaf            │  (叶子: rid[i] = 记录位置Rid)    │
│          │  next_leaf            │  (内部: rid[i].page_no = 子页号) │
└──────────┴───────────────────────┴──────────────────────────────────┘
```

### 3.3 B+ 树结构示意

```
                    ┌─────────────────┐
                    │   Internal Root  │
                    │  keys: [30, 60]  │
                    │  rids: [P1,P2,P3]│
                    └──┬──────┬──────┬─┘
                       │      │      │
            ┌──────────┘      │      └──────────┐
            ▼                 ▼                  ▼
     ┌──────────┐      ┌──────────┐       ┌──────────┐
     │  Leaf P1  │─────►│  Leaf P2  │─────►│  Leaf P3  │
     │ keys<30   │◄─────│ 30≤k<60  │◄─────│ keys≥60  │
     │ rids→Rid  │      │ rids→Rid │       │ rids→Rid │
     └──────────┘      └──────────┘       └──────────┘
         ▲                                      │
         │          Leaf Header (Page 1)        │
         └──────────── prev ◄── next ───────────┘

叶子节点双向链表: Leaf Header ↔ P1 ↔ P2 ↔ P3 ↔ Leaf Header
```

### 3.4 并发控制 — Crabbing 协议

```
find_leaf_page(key, operation, transaction):

  [FIND - 读操作]                    [INSERT/DELETE - 写操作]
  ┌──────────────────┐              ┌──────────────────┐
  │ root_latch_      │              │ root_latch_      │
  │ .lock_shared()   │              │ .lock()          │
  └────────┬─────────┘              └────────┬─────────┘
           │                                 │
           ▼                                 ▼
  ┌──────────────────┐              ┌──────────────────┐
  │ child.latch_lock │              │ child.latch_lock │
  │   (shared)       │              │   (exclusive)    │
  │                  │              │                  │
  │ 释放parent latch │              │ if child安全:    │
  │ unpin parent     │              │   释放所有祖先   │
  └────────┬─────────┘              │   latch+unpin    │
           │                        └────────┬─────────┘
           ▼                                 ▼
     到达叶子节点                       到达叶子节点
     (持有叶子shared latch)            (持有叶子exclusive latch)
                                       (可能持有祖先exclusive latches)

  安全节点判定:
  ├── INSERT: child.size < child.max_size - 1  (不会分裂)
  └── DELETE: child.size > child.min_size      (不会合并)
```

### 3.5 关键操作流程

```
insert_entry(key, rid, txn):
  find_leaf_page(key, INSERT, txn)  ← Crabbing加锁
  leaf->insert(key, rid)
  if (leaf满):
    new_node = split(leaf)           ← 分裂
    insert_into_parent(leaf, split_key, new_node, txn)  ← 可能递归
  release_latch_page_set(txn)        ← 释放所有持有的锁
  unpin + delete leaf

delete_entry(key, txn):
  find_leaf_page(key, DELETE, txn)   ← Crabbing加锁
  leaf->remove(key)
  if (leaf少于半满):
    coalesce_or_redistribute(leaf, txn)  ← 合并或借用
  release_latch_page_set(txn)
  unpin + delete leaf
```

---

## 四、两模块协作关系

```
         SQL: INSERT INTO t VALUES(...)

              ┌──────────────────┐
              │    Executor      │
              └───────┬──────────┘
                      │
         ┌────────────┼────────────┐
         │ 1. 插入记录 │ 2. 更新索引 │
         ▼            │            ▼
  ┌──────────────┐    │    ┌────────────────┐
  │ RmFileHandle │    │    │ IxIndexHandle  │
  │ .insert_     │    │    │ .insert_entry  │
  │  record(buf) │    │    │  (key, rid)    │
  └──────┬───────┘    │    └───────┬────────┘
         │            │            │
         │ return Rid ─┘            │
         │  {page_no,              │
         │   slot_no}──────────────┘
         │            作为value存入B+树叶节点
         ▼
  ┌─────────────────┐      ┌──────────────────┐
  │   Buffer Pool   │◄─────│   Buffer Pool    │
  │  (Record Pages) │      │  (Index Pages)   │
  └────────┬────────┘      └────────┬─────────┘
           │                        │
           ▼                        ▼
  ┌─────────────────────────────────────────────┐
  │              Disk Manager                    │
  │         table_name.db    table_name_col.idx  │
  └─────────────────────────────────────────────┘
```

---

## 五、核心数据结构速查

| 结构 | 所属模块 | 关键字段 | 用途 |
|------|---------|---------|------|
| `Rid{page_no, slot_no}` | 共用 | 页号+槽号 | 定位一条记录 |
| `Iid{page_no, slot_no}` | Index | 叶页号+键位 | 定位B+树中一个键值对 |
| `RmFileHdr` | Record | record_size, num_pages, first_free_page_no, bitmap_size | 表文件元数据 |
| `RmPageHdr` | Record | next_free_page_no, num_records | 记录页元数据 |
| `IxFileHdr` | Index | root_page, btree_order, col_types/lens, first/last_leaf | 索引文件元数据 |
| `IxPageHdr` | Index | parent, num_key, is_leaf, prev/next_leaf | B+树节点元数据 |
| `RmPageHandle` | Record | page + page_hdr + bitmap + slots | 记录页操作句柄 |
| `IxNodeHandle` | Index | page + page_hdr + keys + rids | B+树节点操作句柄 |
