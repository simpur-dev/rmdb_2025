/* 三层联动集成测试
 * 阶段一: Storage (DiskManager, LRU Replacer, BufferPoolManager)
 * 阶段二: Record Manager + B+ Tree Index
 * 阶段三: Execution Engine
 * 重点: 跨层数据完整性、缓冲池压力下的正确性、边缘条件
 */

#undef NDEBUG
#define private public

#include "storage/disk_manager.h"
#include "storage/buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "record/rm.h"
#include "index/ix.h"
#include "system/sm_manager.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"
#include "execution/executor_insert.h"
#include "execution/executor_delete.h"
#include "execution/executor_update.h"
#include "execution/executor_nestedloop_join.h"
#include "execution/executor_projection.h"
#include "execution/execution_sort.h"

#undef private

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <numeric>
#include <random>
#include <set>
#include <vector>

#include "gtest/gtest.h"

// ═══════════════════════════════════════════════════════════════
//  Part 1: Storage Layer Tests (DiskManager + LRU + BufferPool)
// ═══════════════════════════════════════════════════════════════

class StorageLayerTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> dm_;
    const std::string TEST_DIR = "StorageLayerTest_db";
    const std::string TEST_FILE = "test_storage_file";

    void SetUp() override {
        dm_ = std::make_unique<DiskManager>();
        if (!dm_->is_dir(TEST_DIR)) dm_->create_dir(TEST_DIR);
        if (chdir(TEST_DIR.c_str()) < 0) throw UnixError();
        if (dm_->is_file(TEST_FILE)) dm_->destroy_file(TEST_FILE);
    }

    void TearDown() override {
        if (dm_->is_file(TEST_FILE)) {
            dm_->destroy_file(TEST_FILE);
        }
        if (chdir("..") < 0) throw UnixError();
    }
};

// ─── DiskManager: 文件生命周期 ──────────────────────────────

TEST_F(StorageLayerTest, DM_FileLifecycle) {
    ASSERT_FALSE(dm_->is_file(TEST_FILE));
    dm_->create_file(TEST_FILE);
    ASSERT_TRUE(dm_->is_file(TEST_FILE));

    // 不能重复创建
    ASSERT_THROW(dm_->create_file(TEST_FILE), FileExistsError);

    int fd = dm_->open_file(TEST_FILE);
    ASSERT_GE(fd, 0);

    // 不能重复打开
    ASSERT_THROW(dm_->open_file(TEST_FILE), FileExistsError);

    dm_->close_file(fd);

    // 不能关闭已关闭的 fd
    ASSERT_THROW(dm_->close_file(fd), FileNotOpenError);

    dm_->destroy_file(TEST_FILE);
    ASSERT_FALSE(dm_->is_file(TEST_FILE));
}

// ─── DiskManager: 页面读写往返 ─────────────────────────────

TEST_F(StorageLayerTest, DM_PageReadWriteRoundtrip) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    // 写入多个页面
    for (int page_no = 0; page_no < 5; page_no++) {
        char buf[PAGE_SIZE];
        memset(buf, 0, PAGE_SIZE);
        // 在页面开头写入页号
        *(int *)buf = page_no;
        // 在页面末尾写入标记
        *(int *)(buf + PAGE_SIZE - 4) = page_no * 100;
        dm_->write_page(fd, page_no, buf, PAGE_SIZE);
    }

    // 逆序读回验证
    for (int page_no = 4; page_no >= 0; page_no--) {
        char buf[PAGE_SIZE];
        dm_->read_page(fd, page_no, buf, PAGE_SIZE);
        ASSERT_EQ(*(int *)buf, page_no);
        ASSERT_EQ(*(int *)(buf + PAGE_SIZE - 4), page_no * 100);
    }

    dm_->close_file(fd);
}

// ─── DiskManager: 不能删除已打开的文件 ──────────────────────

TEST_F(StorageLayerTest, DM_CannotDestroyOpenFile) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);
    ASSERT_THROW(dm_->destroy_file(TEST_FILE), FileNotClosedError);
    dm_->close_file(fd);
}

// ─── LRU Replacer: 基本淘汰策略 ────────────────────────────

TEST_F(StorageLayerTest, LRU_BasicVictimOrder) {
    LRUReplacer replacer(4);
    ASSERT_EQ(replacer.Size(), 0u);

    // 按 0,1,2 顺序 unpin
    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);
    ASSERT_EQ(replacer.Size(), 3u);

    // victim 应该返回 LRU（最早 unpin 的 0）
    frame_id_t fid;
    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 0);

    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 1);

    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 2);

    // 空了
    ASSERT_FALSE(replacer.victim(&fid));
    ASSERT_EQ(replacer.Size(), 0u);
}

// ─── LRU Replacer: pin 将帧从淘汰池移除 ────────────────────

TEST_F(StorageLayerTest, LRU_PinRemovesFromPool) {
    LRUReplacer replacer(4);
    replacer.unpin(0);
    replacer.unpin(1);
    replacer.unpin(2);

    // pin(1) → 1 不可被淘汰
    replacer.pin(1);
    ASSERT_EQ(replacer.Size(), 2u);

    frame_id_t fid;
    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 0);
    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 2);
    ASSERT_FALSE(replacer.victim(&fid));
}

// ─── LRU Replacer: 重复 unpin 是幂等的 ─────────────────────

TEST_F(StorageLayerTest, LRU_DuplicateUnpin) {
    LRUReplacer replacer(4);
    replacer.unpin(0);
    replacer.unpin(0);  // 重复
    replacer.unpin(0);  // 重复
    ASSERT_EQ(replacer.Size(), 1u);

    frame_id_t fid;
    ASSERT_TRUE(replacer.victim(&fid));
    ASSERT_EQ(fid, 0);
    ASSERT_FALSE(replacer.victim(&fid));
}

// ─── LRU Replacer: pin 不存在的帧不报错 ────────────────────

TEST_F(StorageLayerTest, LRU_PinNonexistent) {
    LRUReplacer replacer(4);
    replacer.pin(99);  // 不在池中，不应崩溃
    ASSERT_EQ(replacer.Size(), 0u);
}

// ─── LRU Replacer: 空池 victim 返回 false ──────────────────

TEST_F(StorageLayerTest, LRU_EmptyVictim) {
    LRUReplacer replacer(4);
    frame_id_t fid = -1;
    ASSERT_FALSE(replacer.victim(&fid));
}

// ─── BufferPoolManager: 基本 new/fetch/unpin ────────────────

TEST_F(StorageLayerTest, BPM_BasicNewFetchUnpin) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    BufferPoolManager bpm(4, dm_.get());

    // 创建新页
    PageId pid = {fd, INVALID_PAGE_ID};
    Page *page = bpm.new_page(&pid);
    ASSERT_NE(page, nullptr);
    ASSERT_EQ(pid.page_no, 0);

    // 写入数据
    memset(page->data_, 0, PAGE_SIZE);
    *(int *)page->data_ = 12345;
    bpm.mark_dirty(page);
    bpm.unpin_page(pid, true);

    // 重新 fetch 应该得到相同数据
    Page *page2 = bpm.fetch_page(pid);
    ASSERT_NE(page2, nullptr);
    ASSERT_EQ(*(int *)page2->data_, 12345);
    bpm.unpin_page(pid, false);

    dm_->close_file(fd);
}

// ─── BufferPoolManager: 缓冲池满时淘汰 ─────────────────────

TEST_F(StorageLayerTest, BPM_EvictionUnderPressure) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    BufferPoolManager bpm(3, dm_.get());  // 只有3帧

    // 创建4个页面（第4个需要淘汰）
    std::vector<PageId> pids;
    for (int i = 0; i < 4; i++) {
        PageId pid = {fd, INVALID_PAGE_ID};
        Page *page = bpm.new_page(&pid);
        ASSERT_NE(page, nullptr) << "Failed at page " << i;
        *(int *)page->data_ = i * 100;
        bpm.unpin_page(pid, true);
        pids.push_back(pid);
    }

    // 所有4个页面都应该可以通过 fetch 读回
    for (int i = 0; i < 4; i++) {
        Page *page = bpm.fetch_page(pids[i]);
        ASSERT_NE(page, nullptr) << "Cannot fetch page " << i;
        ASSERT_EQ(*(int *)page->data_, i * 100) << "Data mismatch on page " << i;
        bpm.unpin_page(pids[i], false);
    }

    dm_->close_file(fd);
}

// ─── BufferPoolManager: 脏页淘汰后数据持久化 ───────────────

TEST_F(StorageLayerTest, BPM_DirtyPagePersistence) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    {
        BufferPoolManager bpm(2, dm_.get());  // 极小缓冲池

        for (int i = 0; i < 5; i++) {
            PageId pid = {fd, INVALID_PAGE_ID};
            Page *page = bpm.new_page(&pid);
            ASSERT_NE(page, nullptr);
            *(int *)page->data_ = i + 1;
            bpm.unpin_page(pid, true);  // 标记脏页
        }
        // BPM 析构时不自动刷盘，但脏页在淘汰时会被写回
        bpm.flush_all_pages(fd);
    }

    // 新 BPM 实例，重新读取，验证持久化
    {
        BufferPoolManager bpm(4, dm_.get());
        for (int i = 0; i < 5; i++) {
            PageId pid = {fd, i};
            Page *page = bpm.fetch_page(pid);
            ASSERT_NE(page, nullptr) << "Cannot fetch page " << i;
            ASSERT_EQ(*(int *)page->data_, i + 1) << "Persistence failed for page " << i;
            bpm.unpin_page(pid, false);
        }
    }

    dm_->close_file(fd);
}

// ─── BufferPoolManager: 所有帧都 pinned 时无法分配 ─────────

TEST_F(StorageLayerTest, BPM_AllPinnedReturnsNull) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    BufferPoolManager bpm(2, dm_.get());

    PageId pid1 = {fd, INVALID_PAGE_ID};
    PageId pid2 = {fd, INVALID_PAGE_ID};
    Page *p1 = bpm.new_page(&pid1);
    Page *p2 = bpm.new_page(&pid2);
    ASSERT_NE(p1, nullptr);
    ASSERT_NE(p2, nullptr);
    // 不 unpin → 两帧都被 pinned

    // 再请求一个新页 → 应返回 nullptr
    PageId pid3 = {fd, INVALID_PAGE_ID};
    Page *p3 = bpm.new_page(&pid3);
    ASSERT_EQ(p3, nullptr);

    bpm.unpin_page(pid1, false);
    bpm.unpin_page(pid2, false);
    dm_->close_file(fd);
}

// ─── BufferPoolManager: delete_page 正确性 ──────────────────

TEST_F(StorageLayerTest, BPM_DeletePage) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    BufferPoolManager bpm(3, dm_.get());

    PageId pid = {fd, INVALID_PAGE_ID};
    Page *page = bpm.new_page(&pid);
    *(int *)page->data_ = 999;
    bpm.unpin_page(pid, true);

    // 删除页面
    ASSERT_TRUE(bpm.delete_page(pid));

    // 删除不存在的页面返回 true
    PageId fake = {fd, 99};
    ASSERT_TRUE(bpm.delete_page(fake));

    // 删除后应该能分配新帧（帧被回收到 free_list）
    PageId pid2 = {fd, INVALID_PAGE_ID};
    Page *page2 = bpm.new_page(&pid2);
    ASSERT_NE(page2, nullptr);
    bpm.unpin_page(pid2, false);

    dm_->close_file(fd);
}

// ─── BufferPoolManager: delete_page 不能删除 pinned 页 ─────

TEST_F(StorageLayerTest, BPM_CannotDeletePinned) {
    dm_->create_file(TEST_FILE);
    int fd = dm_->open_file(TEST_FILE);

    BufferPoolManager bpm(3, dm_.get());

    PageId pid = {fd, INVALID_PAGE_ID};
    Page *page = bpm.new_page(&pid);
    // 不 unpin
    ASSERT_FALSE(bpm.delete_page(pid));

    bpm.unpin_page(pid, false);
    dm_->close_file(fd);
}

// ═══════════════════════════════════════════════════════════════
//  Part 2: Record & Index Integration Tests
// ═══════════════════════════════════════════════════════════════

const std::string INTEG_DB = "IntegrationTest_db";
const std::string INTEG_TAB = "integ_tab";
const std::string INTEG_TAB2 = "integ_tab2";

class RecordIndexIntegTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> dm_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_;
    std::unique_ptr<IxManager> ix_;
    std::unique_ptr<SmManager> sm_;
    Transaction txn_{0};
    std::unique_ptr<LogManager> log_mgr_;
    std::unique_ptr<LockManager> lock_mgr_;
    Context *ctx_;

    // integ_tab: id(INT,4), val(INT,4) → 8 bytes/record
    std::vector<ColDef> col_defs_ = {{"id", TYPE_INT, 4}, {"val", TYPE_INT, 4}};

    void SetUp() override {
        dm_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(64, dm_.get());  // 小缓冲池
        rm_ = std::make_unique<RmManager>(dm_.get(), bpm_.get());
        ix_ = std::make_unique<IxManager>(dm_.get(), bpm_.get());
        sm_ = std::make_unique<SmManager>(dm_.get(), bpm_.get(), rm_.get(), ix_.get());
        log_mgr_ = std::make_unique<LogManager>(dm_.get());
        lock_mgr_ = std::make_unique<LockManager>();

        if (!dm_->is_dir(INTEG_DB)) dm_->create_dir(INTEG_DB);
        if (chdir(INTEG_DB.c_str()) < 0) throw UnixError();

        for (auto &t : {INTEG_TAB, INTEG_TAB2}) {
            if (dm_->is_file(t)) dm_->destroy_file(t);
        }
        sm_->db_.tabs_.clear();
        ctx_ = new Context(lock_mgr_.get(), log_mgr_.get(), &txn_);
        sm_->create_table(INTEG_TAB, col_defs_, nullptr);
    }

    void TearDown() override {
        for (auto &[n, ih] : sm_->ihs_) ix_->close_index(ih.get());
        sm_->ihs_.clear();
        for (auto &[n, fh] : sm_->fhs_) rm_->close_file(fh.get());
        sm_->fhs_.clear();
        for (auto &t : {INTEG_TAB, INTEG_TAB2}) {
            if (dm_->is_file(t)) dm_->destroy_file(t);
        }
        sm_->db_.tabs_.clear();
        delete ctx_;
        if (chdir("..") < 0) throw UnixError();
    }

    Rid insert_rec(int id, int val) {
        auto *fh = sm_->fhs_.at(INTEG_TAB).get();
        char buf[8];
        *(int *)(buf + 0) = id;
        *(int *)(buf + 4) = val;
        return fh->insert_record(buf, nullptr);
    }

    void create_index_on_id() {
        auto &tab = sm_->db_.get_table(INTEG_TAB);
        std::vector<ColMeta> idx_cols;
        for (auto &c : tab.cols) { if (c.name == "id") { idx_cols.push_back(c); break; } }
        std::string ix_name = ix_->get_index_name(INTEG_TAB, idx_cols);
        if (ix_->exists(INTEG_TAB, idx_cols)) ix_->destroy_index(INTEG_TAB, idx_cols);
        ix_->create_index(INTEG_TAB, idx_cols);
        sm_->ihs_.emplace(ix_name, ix_->open_index(INTEG_TAB, idx_cols));
        IndexMeta im;
        im.tab_name = INTEG_TAB; im.col_num = 1;
        im.col_tot_len = idx_cols[0].len; im.cols = idx_cols;
        tab.indexes.push_back(im);
    }

    void insert_idx(int id, const Rid &rid) {
        std::vector<std::string> names = {"id"};
        auto *ih = sm_->ihs_.at(ix_->get_index_name(INTEG_TAB, names)).get();
        char key[4]; *(int *)key = id;
        ih->insert_entry(key, rid, &txn_);
    }

    Condition make_cond(const std::string &col, CompOp op, int v) {
        Condition c;
        c.lhs_col = {INTEG_TAB, col}; c.op = op; c.is_rhs_val = true;
        c.rhs_val.type = TYPE_INT; c.rhs_val.int_val = v;
        auto &tab = sm_->db_.get_table(INTEG_TAB);
        for (auto &col_m : tab.cols) {
            if (col_m.name == col) { c.rhs_val.init_raw(col_m.len); break; }
        }
        return c;
    }
};

// ─── 记录: 大量插入+扫描（跨多页，触发缓冲池淘汰）────────

TEST_F(RecordIndexIntegTest, Record_BulkInsertScan) {
    int N = 1000;  // 8 bytes/record, ~500/page, 1000 records ≈ 2+ pages
    std::set<int> expected;
    for (int i = 0; i < N; i++) {
        insert_rec(i, i * 10);
        expected.insert(i);
    }

    SeqScanExecutor scan(sm_.get(), INTEG_TAB, {}, nullptr);
    std::set<int> got;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        ASSERT_NE(rec, nullptr);
        int id = *(int *)(rec->data);
        int val = *(int *)(rec->data + 4);
        ASSERT_EQ(val, id * 10) << "Data corruption at id=" << id;
        got.insert(id);
    }
    ASSERT_EQ(got, expected);
}

// ─── 记录: 插入→删除→重新扫描 ─────────────────────────────

TEST_F(RecordIndexIntegTest, Record_DeleteAndRescan) {
    for (int i = 0; i < 20; i++) insert_rec(i, i);

    // 删除偶数 id
    auto cond = make_cond("id", OP_LT, 10);  // 先获取 id < 10 的记录
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {}, nullptr);
    std::vector<Rid> to_delete;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple()) {
        auto rec = scan1.Next();
        int id = *(int *)(rec->data);
        if (id % 2 == 0) to_delete.push_back(scan1.rid());
    }

    DeleteExecutor del(sm_.get(), INTEG_TAB, {}, to_delete, ctx_);
    del.Next();

    // 验证: 只剩奇数 id
    SeqScanExecutor scan2(sm_.get(), INTEG_TAB, {}, nullptr);
    int count = 0;
    for (scan2.beginTuple(); !scan2.is_end(); scan2.nextTuple()) {
        auto rec = scan2.Next();
        int id = *(int *)(rec->data);
        ASSERT_EQ(id % 2, 1) << "Even id=" << id << " should have been deleted";
        count++;
    }
    ASSERT_EQ(count, 10);
}

// ─── 索引: 大量插入+点查（跨页 B+ 树）─────────────────────

TEST_F(RecordIndexIntegTest, Index_BulkInsertPointQuery) {
    create_index_on_id();

    int N = 500;
    for (int i = 0; i < N; i++) {
        Rid rid = insert_rec(i, i * 2);
        insert_idx(i, rid);
    }

    // 随机点查验证
    std::mt19937 rng(42);
    for (int trial = 0; trial < 50; trial++) {
        int target = rng() % N;
        auto cond = make_cond("id", OP_EQ, target);
        IndexScanExecutor iscan(sm_.get(), INTEG_TAB, {cond}, {"id"}, nullptr);
        int found = 0;
        for (iscan.beginTuple(); !iscan.is_end(); iscan.nextTuple()) {
            auto rec = iscan.Next();
            ASSERT_NE(rec, nullptr);
            ASSERT_EQ(*(int *)(rec->data), target);
            ASSERT_EQ(*(int *)(rec->data + 4), target * 2);
            found++;
        }
        ASSERT_EQ(found, 1) << "Point query for id=" << target << " failed";
    }
}

// ─── 索引: 范围查询边界 ────────────────────────────────────

TEST_F(RecordIndexIntegTest, Index_RangeBoundary) {
    create_index_on_id();

    for (int i = 0; i < 100; i++) {
        Rid rid = insert_rec(i, i);
        insert_idx(i, rid);
    }

    // WHERE id >= 0 AND id < 1 → 只有 id=0
    auto c1 = make_cond("id", OP_GE, 0);
    auto c2 = make_cond("id", OP_LT, 1);
    IndexScanExecutor iscan(sm_.get(), INTEG_TAB, {c1, c2}, {"id"}, nullptr);
    int count = 0;
    for (iscan.beginTuple(); !iscan.is_end(); iscan.nextTuple()) {
        auto rec = iscan.Next();
        ASSERT_EQ(*(int *)(rec->data), 0);
        count++;
    }
    ASSERT_EQ(count, 1);
}

// ─── 索引: 查询无结果 ─────────────────────────────────────

TEST_F(RecordIndexIntegTest, Index_EmptyResult) {
    create_index_on_id();
    for (int i = 10; i < 20; i++) {
        Rid rid = insert_rec(i, i);
        insert_idx(i, rid);
    }

    auto cond = make_cond("id", OP_EQ, 5);
    IndexScanExecutor iscan(sm_.get(), INTEG_TAB, {cond}, {"id"}, nullptr);
    iscan.beginTuple();
    ASSERT_TRUE(iscan.is_end());
}

// ═══════════════════════════════════════════════════════════════
//  Part 3: Full Stack Integration Tests
// ═══════════════════════════════════════════════════════════════

// ─── 全栈: Insert→Update→Scan 数据完整性 ──────────────────

TEST_F(RecordIndexIntegTest, FullStack_InsertUpdateVerify) {
    for (int i = 0; i < 50; i++) insert_rec(i, i * 10);

    // UPDATE SET val=999 WHERE id=25
    auto cond = make_cond("id", OP_EQ, 25);
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {cond}, nullptr);
    std::vector<Rid> rids;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple()) rids.push_back(scan1.rid());
    ASSERT_EQ(rids.size(), 1u);

    SetClause sc;
    sc.lhs = {INTEG_TAB, "val"};
    sc.rhs.type = TYPE_INT; sc.rhs.int_val = 999;
    UpdateExecutor upd(sm_.get(), INTEG_TAB, {sc}, {cond}, rids, ctx_);
    upd.Next();

    // 验证
    SeqScanExecutor scan2(sm_.get(), INTEG_TAB, {cond}, nullptr);
    for (scan2.beginTuple(); !scan2.is_end(); scan2.nextTuple()) {
        auto rec = scan2.Next();
        ASSERT_EQ(*(int *)(rec->data + 0), 25);
        ASSERT_EQ(*(int *)(rec->data + 4), 999);
    }

    // 其他记录未受影响
    auto cond2 = make_cond("id", OP_EQ, 24);
    SeqScanExecutor scan3(sm_.get(), INTEG_TAB, {cond2}, nullptr);
    for (scan3.beginTuple(); !scan3.is_end(); scan3.nextTuple()) {
        auto rec = scan3.Next();
        ASSERT_EQ(*(int *)(rec->data + 4), 240);
    }
}

// ─── 全栈: Insert with Index → Delete → Index 一致性 ──────

TEST_F(RecordIndexIntegTest, FullStack_DeleteIndexConsistency) {
    create_index_on_id();

    for (int i = 0; i < 30; i++) {
        Rid rid = insert_rec(i, i * 10);
        insert_idx(i, rid);
    }

    // 删除 id=15
    auto cond = make_cond("id", OP_EQ, 15);
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {cond}, nullptr);
    std::vector<Rid> rids;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple()) rids.push_back(scan1.rid());
    ASSERT_EQ(rids.size(), 1u);

    DeleteExecutor del(sm_.get(), INTEG_TAB, {cond}, rids, ctx_);
    del.Next();

    // 索引查询 id=15 应为空
    IndexScanExecutor iscan(sm_.get(), INTEG_TAB, {cond}, {"id"}, nullptr);
    iscan.beginTuple();
    ASSERT_TRUE(iscan.is_end());

    // 顺序扫描也不应包含 id=15
    SeqScanExecutor scan2(sm_.get(), INTEG_TAB, {}, nullptr);
    for (scan2.beginTuple(); !scan2.is_end(); scan2.nextTuple()) {
        auto rec = scan2.Next();
        ASSERT_NE(*(int *)(rec->data), 15);
    }
}

// ─── 全栈: 大量数据下的 Sort + Projection 流水线 ──────────

TEST_F(RecordIndexIntegTest, FullStack_SortProjectionPipeline) {
    std::mt19937 rng(123);
    int N = 200;
    std::vector<int> ids(N);
    std::iota(ids.begin(), ids.end(), 0);
    std::shuffle(ids.begin(), ids.end(), rng);

    for (int i = 0; i < N; i++) insert_rec(ids[i], ids[i] * 3);

    // Sort by id ASC → Projection(val only)
    auto scan = std::make_unique<SeqScanExecutor>(sm_.get(), INTEG_TAB,
                                                   std::vector<Condition>{}, nullptr);
    TabCol sort_col = {INTEG_TAB, "id"};
    auto sort = std::make_unique<SortExecutor>(std::move(scan), sort_col, false);

    std::vector<TabCol> sel_cols = {{INTEG_TAB, "val"}};
    ProjectionExecutor proj(std::move(sort), sel_cols);

    ASSERT_EQ(proj.tupleLen(), 4u);

    std::vector<int> vals;
    for (proj.beginTuple(); !proj.is_end(); proj.nextTuple()) {
        auto rec = proj.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(rec->size, 4);
        vals.push_back(*(int *)(rec->data));
    }

    ASSERT_EQ((int)vals.size(), N);
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(vals[i], i * 3) << "Mismatch at position " << i;
    }
}

// ─── 全栈: Insert→Delete→Insert 回收空间 ──────────────────

TEST_F(RecordIndexIntegTest, FullStack_SpaceRecycling) {
    // 插入一批记录
    for (int i = 0; i < 100; i++) insert_rec(i, i);

    // 删除所有
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {}, nullptr);
    std::vector<Rid> all_rids;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple())
        all_rids.push_back(scan1.rid());
    ASSERT_EQ(all_rids.size(), 100u);

    DeleteExecutor del(sm_.get(), INTEG_TAB, {}, all_rids, ctx_);
    del.Next();

    // 验证空了
    SeqScanExecutor scan2(sm_.get(), INTEG_TAB, {}, nullptr);
    scan2.beginTuple();
    ASSERT_TRUE(scan2.is_end());

    // 重新插入，应能正常分配空间
    for (int i = 1000; i < 1050; i++) insert_rec(i, i);

    SeqScanExecutor scan3(sm_.get(), INTEG_TAB, {}, nullptr);
    int count = 0;
    for (scan3.beginTuple(); !scan3.is_end(); scan3.nextTuple()) {
        auto rec = scan3.Next();
        ASSERT_GE(*(int *)(rec->data), 1000);
        count++;
    }
    ASSERT_EQ(count, 50);
}

// ─── 全栈: 缓冲池压力下的 Update 批量操作 ─────────────────

TEST_F(RecordIndexIntegTest, FullStack_BulkUpdateUnderPressure) {
    int N = 300;  // 足够多以触发缓冲池淘汰（pool=64 pages）
    for (int i = 0; i < N; i++) insert_rec(i, 0);

    // UPDATE SET val = id * 7 for all
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {}, nullptr);
    std::vector<Rid> rids;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple())
        rids.push_back(scan1.rid());
    ASSERT_EQ((int)rids.size(), N);

    // 逐条更新（模拟 UPDATE ... WHERE id = X）
    auto *fh = sm_->fhs_.at(INTEG_TAB).get();
    for (auto &rid : rids) {
        auto rec = fh->get_record(rid, ctx_);
        int id = *(int *)(rec->data);
        *(int *)(rec->data + 4) = id * 7;
        fh->update_record(rid, rec->data, ctx_);
    }

    // 验证所有记录
    SeqScanExecutor scan2(sm_.get(), INTEG_TAB, {}, nullptr);
    int verified = 0;
    for (scan2.beginTuple(); !scan2.is_end(); scan2.nextTuple()) {
        auto rec = scan2.Next();
        int id = *(int *)(rec->data);
        int val = *(int *)(rec->data + 4);
        ASSERT_EQ(val, id * 7) << "Update corruption at id=" << id;
        verified++;
    }
    ASSERT_EQ(verified, N);
}

// ─── 全栈: Sort 降序 + 大数据完整性 ──────────────────────

TEST_F(RecordIndexIntegTest, FullStack_SortDescLargeData) {
    int N = 500;
    for (int i = 0; i < N; i++) insert_rec(i, N - i);

    auto scan = std::make_unique<SeqScanExecutor>(sm_.get(), INTEG_TAB,
                                                   std::vector<Condition>{}, nullptr);
    TabCol sort_col = {INTEG_TAB, "val"};
    SortExecutor sort(std::move(scan), sort_col, true);  // DESC by val

    std::vector<int> vals;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        vals.push_back(*(int *)(rec->data + 4));
    }

    ASSERT_EQ((int)vals.size(), N);
    // vals should be N, N-1, ..., 1
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(vals[i], N - i) << "Sort desc failed at position " << i;
    }
}

// ─── 全栈: Index 插入→全部删除→重新插入 ──────────────────

TEST_F(RecordIndexIntegTest, FullStack_IndexRebuild) {
    create_index_on_id();

    // 第一轮: 插入 0..49
    for (int i = 0; i < 50; i++) {
        Rid rid = insert_rec(i, i);
        insert_idx(i, rid);
    }

    // 删除所有
    SeqScanExecutor scan1(sm_.get(), INTEG_TAB, {}, nullptr);
    std::vector<Rid> rids;
    for (scan1.beginTuple(); !scan1.is_end(); scan1.nextTuple())
        rids.push_back(scan1.rid());

    DeleteExecutor del(sm_.get(), INTEG_TAB, {}, rids, ctx_);
    del.Next();

    // 第二轮: 插入 100..149
    for (int i = 100; i < 150; i++) {
        Rid rid = insert_rec(i, i);
        insert_idx(i, rid);
    }

    // 旧 key 不应存在
    auto c1 = make_cond("id", OP_EQ, 25);
    IndexScanExecutor is1(sm_.get(), INTEG_TAB, {c1}, {"id"}, nullptr);
    is1.beginTuple();
    ASSERT_TRUE(is1.is_end());

    // 新 key 应存在
    auto c2 = make_cond("id", OP_EQ, 125);
    IndexScanExecutor is2(sm_.get(), INTEG_TAB, {c2}, {"id"}, nullptr);
    is2.beginTuple();
    ASSERT_FALSE(is2.is_end());
    auto rec = is2.Next();
    ASSERT_EQ(*(int *)(rec->data), 125);
}
