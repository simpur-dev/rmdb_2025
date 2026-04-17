/* 执行层全面单元测试
 * 覆盖: Insert, Delete, Update, NestedLoopJoin, Projection, Sort
 * 重点: 边缘条件（空表、单条记录、大量数据、重复值、无匹配等）
 */

#undef NDEBUG
#define private public

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
#include <random>
#include <set>
#include <vector>
#include <numeric>

#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"
#include "transaction/transaction.h"

const std::string TEST_DB_NAME = "ExecutorFullTest_db";
constexpr size_t TEST_BUFFER_POOL_SIZE = 512;
const std::string TAB_A = "tab_a";
const std::string TAB_B = "tab_b";

// ─── 测试 Fixture ─────────────────────────────────────────────

class ExecutorFullTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    Transaction txn_{0};
    std::unique_ptr<LogManager> log_mgr_;
    std::unique_ptr<LockManager> lock_mgr_;
    Context *ctx_;

    // tab_a: id(INT,4), score(INT,4), name(STRING,16) → 24 bytes
    std::vector<ColDef> col_defs_a_ = {
        {"id", TYPE_INT, 4},
        {"score", TYPE_INT, 4},
        {"name", TYPE_STRING, 16},
    };
    int rec_size_a_ = 24;

    // tab_b: bid(INT,4), val(INT,4) → 8 bytes
    std::vector<ColDef> col_defs_b_ = {
        {"bid", TYPE_INT, 4},
        {"val", TYPE_INT, 4},
    };
    int rec_size_b_ = 8;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), bpm_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), bpm_.get(),
                                                   rm_manager_.get(), ix_manager_.get());
        log_mgr_ = std::make_unique<LogManager>(disk_manager_.get());
        lock_mgr_ = std::make_unique<LockManager>();

        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }

        // 清理残留
        for (auto &name : {TAB_A, TAB_B}) {
            if (disk_manager_->is_file(name)) {
                disk_manager_->destroy_file(name);
            }
        }
        sm_manager_->db_.tabs_.clear();

        ctx_ = new Context(lock_mgr_.get(), log_mgr_.get(), &txn_);

        sm_manager_->create_table(TAB_A, col_defs_a_, nullptr);
        sm_manager_->create_table(TAB_B, col_defs_b_, nullptr);
    }

    void TearDown() override {
        // 关闭索引
        for (auto &[name, ih] : sm_manager_->ihs_) {
            ix_manager_->close_index(ih.get());
        }
        sm_manager_->ihs_.clear();

        // 关闭记录文件
        for (auto &[name, fh] : sm_manager_->fhs_) {
            rm_manager_->close_file(fh.get());
        }
        sm_manager_->fhs_.clear();

        // 销毁表文件
        for (auto &name : {TAB_A, TAB_B}) {
            if (disk_manager_->is_file(name)) {
                disk_manager_->destroy_file(name);
            }
        }
        sm_manager_->db_.tabs_.clear();

        delete ctx_;

        if (chdir("..") < 0) {
            throw UnixError();
        }
    }

    // ─── 辅助方法 ──────────────────────────────────────────────

    Rid insert_a(int id, int score, const std::string &name) {
        auto *fh = sm_manager_->fhs_.at(TAB_A).get();
        char buf[24];
        memset(buf, 0, sizeof(buf));
        *(int *)(buf + 0) = id;
        *(int *)(buf + 4) = score;
        memcpy(buf + 8, name.c_str(), std::min(name.size(), (size_t)16));
        return fh->insert_record(buf, nullptr);
    }

    Rid insert_b(int bid, int val) {
        auto *fh = sm_manager_->fhs_.at(TAB_B).get();
        char buf[8];
        memset(buf, 0, sizeof(buf));
        *(int *)(buf + 0) = bid;
        *(int *)(buf + 4) = val;
        return fh->insert_record(buf, nullptr);
    }

    // 读取记录的 id 字段（tab_a 的第一个 INT）
    int read_id(const RmRecord *rec) { return *(int *)(rec->data + 0); }
    int read_score(const RmRecord *rec) { return *(int *)(rec->data + 4); }

    // 通过 SeqScan 收集 tab_a 的所有 id
    std::vector<int> scan_all_ids(const std::string &tab) {
        SeqScanExecutor scan(sm_manager_.get(), tab, {}, nullptr);
        std::vector<int> ids;
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            auto rec = scan.Next();
            if (rec) ids.push_back(*(int *)(rec->data));
        }
        return ids;
    }

    // 构造 tab_a 上的比较条件
    Condition make_cond_a(const std::string &col_name, CompOp op, int val) {
        Condition cond;
        cond.lhs_col = {TAB_A, col_name};
        cond.op = op;
        cond.is_rhs_val = true;
        cond.rhs_val.type = TYPE_INT;
        cond.rhs_val.int_val = val;
        auto &tab = sm_manager_->db_.get_table(TAB_A);
        for (auto &col : tab.cols) {
            if (col.name == col_name) {
                cond.rhs_val.init_raw(col.len);
                break;
            }
        }
        return cond;
    }

    // 构造 Join 条件: tab_a.col_a = tab_b.col_b
    Condition make_join_cond(const std::string &col_a, const std::string &col_b) {
        Condition cond;
        cond.lhs_col = {TAB_A, col_a};
        cond.op = OP_EQ;
        cond.is_rhs_val = false;
        cond.rhs_col = {TAB_B, col_b};
        return cond;
    }

    // 通过 SeqScan 收集所有记录的 RID（用于 Delete/Update）
    std::vector<Rid> scan_rids(const std::string &tab, std::vector<Condition> conds = {}) {
        SeqScanExecutor scan(sm_manager_.get(), tab, conds, nullptr);
        std::vector<Rid> rids;
        for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
            rids.push_back(scan.rid());
        }
        return rids;
    }

    // 创建 tab_a.id 上的索引
    void create_index_a_id() {
        auto &tab = sm_manager_->db_.get_table(TAB_A);
        std::vector<ColMeta> idx_cols;
        for (auto &col : tab.cols) {
            if (col.name == "id") { idx_cols.push_back(col); break; }
        }
        std::string ix_name = ix_manager_->get_index_name(TAB_A, idx_cols);
        if (ix_manager_->exists(TAB_A, idx_cols)) {
            ix_manager_->destroy_index(TAB_A, idx_cols);
        }
        ix_manager_->create_index(TAB_A, idx_cols);
        sm_manager_->ihs_.emplace(ix_name, ix_manager_->open_index(TAB_A, idx_cols));

        IndexMeta idx_meta;
        idx_meta.tab_name = TAB_A;
        idx_meta.col_num = 1;
        idx_meta.col_tot_len = idx_cols[0].len;
        idx_meta.cols = idx_cols;
        tab.indexes.push_back(idx_meta);
    }

    void insert_index_a(int id, const Rid &rid) {
        std::vector<std::string> idx_col_names = {"id"};
        std::string ix_name = ix_manager_->get_index_name(TAB_A, idx_col_names);
        auto *ih = sm_manager_->ihs_.at(ix_name).get();
        char key[4];
        *(int *)key = id;
        ih->insert_entry(key, rid, &txn_);
    }
};

// ═══════════════════════════════════════════════════════════════
//  InsertExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Insert_SingleRecord) {
    std::vector<Value> vals(3);
    vals[0].type = TYPE_INT; vals[0].int_val = 42;
    vals[1].type = TYPE_INT; vals[1].int_val = 100;
    vals[2].type = TYPE_STRING; vals[2].str_val = "hello";

    InsertExecutor ins(sm_manager_.get(), TAB_A, vals, ctx_);
    auto ret = ins.Next();
    ASSERT_EQ(ret, nullptr);

    auto ids = scan_all_ids(TAB_A);
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(ids[0], 42);
}

TEST_F(ExecutorFullTest, Insert_MultipleRecords) {
    for (int i = 0; i < 50; i++) {
        std::vector<Value> vals(3);
        vals[0].type = TYPE_INT; vals[0].int_val = i;
        vals[1].type = TYPE_INT; vals[1].int_val = i * 10;
        vals[2].type = TYPE_STRING; vals[2].str_val = "name_" + std::to_string(i);

        InsertExecutor ins(sm_manager_.get(), TAB_A, vals, ctx_);
        ins.Next();
    }

    auto ids = scan_all_ids(TAB_A);
    ASSERT_EQ(ids.size(), 50u);
    std::sort(ids.begin(), ids.end());
    for (int i = 0; i < 50; i++) {
        ASSERT_EQ(ids[i], i);
    }
}

TEST_F(ExecutorFullTest, Insert_WrongColumnCount) {
    std::vector<Value> vals(2);
    vals[0].type = TYPE_INT; vals[0].int_val = 1;
    vals[1].type = TYPE_INT; vals[1].int_val = 2;

    ASSERT_THROW(InsertExecutor(sm_manager_.get(), TAB_A, vals, ctx_), InvalidValueCountError);
}

TEST_F(ExecutorFullTest, Insert_TypeMismatch) {
    std::vector<Value> vals(3);
    vals[0].type = TYPE_STRING; vals[0].str_val = "not_int";
    vals[1].type = TYPE_INT; vals[1].int_val = 100;
    vals[2].type = TYPE_STRING; vals[2].str_val = "hello";

    InsertExecutor ins(sm_manager_.get(), TAB_A, vals, ctx_);
    ASSERT_THROW(ins.Next(), IncompatibleTypeError);
}

TEST_F(ExecutorFullTest, Insert_WithIndex) {
    create_index_a_id();

    std::vector<Value> vals(3);
    vals[0].type = TYPE_INT; vals[0].int_val = 7;
    vals[1].type = TYPE_INT; vals[1].int_val = 70;
    vals[2].type = TYPE_STRING; vals[2].str_val = "indexed";

    InsertExecutor ins(sm_manager_.get(), TAB_A, vals, ctx_);
    ins.Next();

    // 通过索引扫描验证
    auto cond = make_cond_a("id", OP_EQ, 7);
    IndexScanExecutor iscan(sm_manager_.get(), TAB_A, {cond}, {"id"}, nullptr);
    std::vector<int> ids;
    for (iscan.beginTuple(); !iscan.is_end(); iscan.nextTuple()) {
        auto rec = iscan.Next();
        if (rec) ids.push_back(*(int *)(rec->data));
    }
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(ids[0], 7);
}

// ═══════════════════════════════════════════════════════════════
//  DeleteExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Delete_AllRecords) {
    for (int i = 1; i <= 10; i++) insert_a(i, i * 10, "name");

    auto rids = scan_rids(TAB_A);
    ASSERT_EQ(rids.size(), 10u);

    DeleteExecutor del(sm_manager_.get(), TAB_A, {}, rids, ctx_);
    del.Next();

    auto remaining = scan_all_ids(TAB_A);
    ASSERT_TRUE(remaining.empty());
}

TEST_F(ExecutorFullTest, Delete_EmptyRids) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name");

    // 删除空列表 → 不删任何东西
    DeleteExecutor del(sm_manager_.get(), TAB_A, {}, {}, ctx_);
    del.Next();

    auto remaining = scan_all_ids(TAB_A);
    ASSERT_EQ(remaining.size(), 5u);
}

TEST_F(ExecutorFullTest, Delete_Selective) {
    for (int i = 1; i <= 10; i++) insert_a(i, i * 10, "name");

    // 只删除 id > 7 的记录
    auto cond = make_cond_a("id", OP_GT, 7);
    auto rids = scan_rids(TAB_A, {cond});
    ASSERT_EQ(rids.size(), 3u);

    DeleteExecutor del(sm_manager_.get(), TAB_A, {cond}, rids, ctx_);
    del.Next();

    auto remaining = scan_all_ids(TAB_A);
    std::sort(remaining.begin(), remaining.end());
    ASSERT_EQ(remaining, (std::vector<int>{1, 2, 3, 4, 5, 6, 7}));
}

TEST_F(ExecutorFullTest, Delete_SingleRecord) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name");

    auto cond = make_cond_a("id", OP_EQ, 3);
    auto rids = scan_rids(TAB_A, {cond});
    ASSERT_EQ(rids.size(), 1u);

    DeleteExecutor del(sm_manager_.get(), TAB_A, {cond}, rids, ctx_);
    del.Next();

    auto remaining = scan_all_ids(TAB_A);
    std::sort(remaining.begin(), remaining.end());
    ASSERT_EQ(remaining, (std::vector<int>{1, 2, 4, 5}));
}

TEST_F(ExecutorFullTest, Delete_WithIndex) {
    create_index_a_id();

    for (int i = 1; i <= 5; i++) {
        Rid rid = insert_a(i, i * 10, "name");
        insert_index_a(i, rid);
    }

    auto cond = make_cond_a("id", OP_EQ, 3);
    auto rids = scan_rids(TAB_A, {cond});
    DeleteExecutor del(sm_manager_.get(), TAB_A, {cond}, rids, ctx_);
    del.Next();

    // 索引中也不应找到 id=3
    IndexScanExecutor iscan(sm_manager_.get(), TAB_A, {cond}, {"id"}, nullptr);
    std::vector<int> ids;
    for (iscan.beginTuple(); !iscan.is_end(); iscan.nextTuple()) {
        auto rec = iscan.Next();
        if (rec) ids.push_back(*(int *)(rec->data));
    }
    ASSERT_TRUE(ids.empty());
}

// ═══════════════════════════════════════════════════════════════
//  UpdateExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Update_SingleField) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name");

    // UPDATE tab_a SET score = 999 WHERE id = 3
    auto cond = make_cond_a("id", OP_EQ, 3);
    auto rids = scan_rids(TAB_A, {cond});
    ASSERT_EQ(rids.size(), 1u);

    SetClause sc;
    sc.lhs = {TAB_A, "score"};
    sc.rhs.type = TYPE_INT;
    sc.rhs.int_val = 999;

    UpdateExecutor upd(sm_manager_.get(), TAB_A, {sc}, {cond}, rids, ctx_);
    upd.Next();

    // 验证更新后 id=3 的 score 应为 999
    SeqScanExecutor scan(sm_manager_.get(), TAB_A, {cond}, nullptr);
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(read_id(rec.get()), 3);
        ASSERT_EQ(read_score(rec.get()), 999);
    }
}

TEST_F(ExecutorFullTest, Update_AllRecords) {
    for (int i = 1; i <= 10; i++) insert_a(i, i * 10, "name");

    auto rids = scan_rids(TAB_A);
    ASSERT_EQ(rids.size(), 10u);

    // UPDATE tab_a SET score = 0
    SetClause sc;
    sc.lhs = {TAB_A, "score"};
    sc.rhs.type = TYPE_INT;
    sc.rhs.int_val = 0;

    UpdateExecutor upd(sm_manager_.get(), TAB_A, {sc}, {}, rids, ctx_);
    upd.Next();

    // 所有 score 都应该是 0
    SeqScanExecutor scan(sm_manager_.get(), TAB_A, {}, nullptr);
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(read_score(rec.get()), 0);
    }
}

TEST_F(ExecutorFullTest, Update_EmptyRids) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name");

    SetClause sc;
    sc.lhs = {TAB_A, "score"};
    sc.rhs.type = TYPE_INT;
    sc.rhs.int_val = 999;

    // 空 rids → 不更新任何东西
    UpdateExecutor upd(sm_manager_.get(), TAB_A, {sc}, {}, {}, ctx_);
    upd.Next();

    // 所有 score 仍为原值
    SeqScanExecutor scan(sm_manager_.get(), TAB_A, {}, nullptr);
    int count = 0;
    for (scan.beginTuple(); !scan.is_end(); scan.nextTuple()) {
        auto rec = scan.Next();
        ASSERT_NE(rec, nullptr);
        int id = read_id(rec.get());
        ASSERT_EQ(read_score(rec.get()), id * 10);
        count++;
    }
    ASSERT_EQ(count, 5);
}

// ═══════════════════════════════════════════════════════════════
//  NestedLoopJoinExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Join_BothEmpty) {
    // 两张空表 Join
    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    NestedLoopJoinExecutor join(std::move(left), std::move(right), {});
    join.beginTuple();
    ASSERT_TRUE(join.is_end());
    ASSERT_EQ(join.Next(), nullptr);
}

TEST_F(ExecutorFullTest, Join_LeftEmpty) {
    // 左表空，右表有数据
    insert_b(1, 100);
    insert_b(2, 200);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    NestedLoopJoinExecutor join(std::move(left), std::move(right), {});
    join.beginTuple();
    ASSERT_TRUE(join.is_end());
}

TEST_F(ExecutorFullTest, Join_RightEmpty) {
    // 左表有数据，右表空
    insert_a(1, 10, "a");
    insert_a(2, 20, "b");

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    NestedLoopJoinExecutor join(std::move(left), std::move(right), {});
    join.beginTuple();
    ASSERT_TRUE(join.is_end());
}

TEST_F(ExecutorFullTest, Join_CrossProduct_NoCondition) {
    // 2 x 3 = 6 条记录
    insert_a(1, 10, "a1");
    insert_a(2, 20, "a2");

    insert_b(10, 100);
    insert_b(20, 200);
    insert_b(30, 300);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    NestedLoopJoinExecutor join(std::move(left), std::move(right), {});

    int count = 0;
    for (join.beginTuple(); !join.is_end(); join.nextTuple()) {
        auto rec = join.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ((int)rec->size, rec_size_a_ + rec_size_b_);
        count++;
    }
    ASSERT_EQ(count, 6);
}

TEST_F(ExecutorFullTest, Join_WithCondition) {
    // tab_a: id=1,2,3  tab_b: bid=2,3,4
    // JOIN ON tab_a.id = tab_b.bid → 匹配 (2,2) 和 (3,3)
    insert_a(1, 10, "a1");
    insert_a(2, 20, "a2");
    insert_a(3, 30, "a3");

    insert_b(2, 200);
    insert_b(3, 300);
    insert_b(4, 400);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    auto cond = make_join_cond("id", "bid");
    NestedLoopJoinExecutor join(std::move(left), std::move(right), {cond});

    std::vector<std::pair<int, int>> results;
    for (join.beginTuple(); !join.is_end(); join.nextTuple()) {
        auto rec = join.Next();
        ASSERT_NE(rec, nullptr);
        int a_id = *(int *)(rec->data + 0);
        int b_bid = *(int *)(rec->data + rec_size_a_ + 0);
        results.push_back({a_id, b_bid});
    }

    std::sort(results.begin(), results.end());
    ASSERT_EQ(results.size(), 2u);
    ASSERT_EQ(results[0], (std::pair<int, int>{2, 2}));
    ASSERT_EQ(results[1], (std::pair<int, int>{3, 3}));
}

TEST_F(ExecutorFullTest, Join_NoMatch) {
    // tab_a: id=1,2  tab_b: bid=10,20 → 无匹配
    insert_a(1, 10, "a1");
    insert_a(2, 20, "a2");

    insert_b(10, 100);
    insert_b(20, 200);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    auto cond = make_join_cond("id", "bid");
    NestedLoopJoinExecutor join(std::move(left), std::move(right), {cond});

    join.beginTuple();
    ASSERT_TRUE(join.is_end());
}

TEST_F(ExecutorFullTest, Join_SingleRow_Each) {
    insert_a(5, 50, "a");
    insert_b(5, 500);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    auto cond = make_join_cond("id", "bid");
    NestedLoopJoinExecutor join(std::move(left), std::move(right), {cond});

    int count = 0;
    for (join.beginTuple(); !join.is_end(); join.nextTuple()) {
        auto rec = join.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(*(int *)(rec->data), 5);
        ASSERT_EQ(*(int *)(rec->data + rec_size_a_), 5);
        count++;
    }
    ASSERT_EQ(count, 1);
}

TEST_F(ExecutorFullTest, Join_Duplicate_Keys) {
    // tab_a: id=1,1,2  tab_b: bid=1,1 → 应产生 2*2 + 0 = 4 条匹配
    insert_a(1, 10, "a1");
    insert_a(1, 11, "a1dup");
    insert_a(2, 20, "a2");

    insert_b(1, 100);
    insert_b(1, 101);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    auto cond = make_join_cond("id", "bid");
    NestedLoopJoinExecutor join(std::move(left), std::move(right), {cond});

    int count = 0;
    for (join.beginTuple(); !join.is_end(); join.nextTuple()) {
        auto rec = join.Next();
        ASSERT_NE(rec, nullptr);
        count++;
    }
    ASSERT_EQ(count, 4);
}

TEST_F(ExecutorFullTest, Join_MetaInfo) {
    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    NestedLoopJoinExecutor join(std::move(left), std::move(right), {});
    ASSERT_EQ(join.tupleLen(), (size_t)(rec_size_a_ + rec_size_b_));
    ASSERT_EQ(join.cols().size(), 5u);  // 3 from A + 2 from B
    ASSERT_EQ(join.getType(), "NestedLoopJoinExecutor");
}

// ═══════════════════════════════════════════════════════════════
//  ProjectionExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Projection_EmptyTable) {
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    std::vector<TabCol> sel_cols = {{TAB_A, "id"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);
    proj.beginTuple();
    ASSERT_TRUE(proj.is_end());
    ASSERT_EQ(proj.Next(), nullptr);
}

TEST_F(ExecutorFullTest, Projection_SingleColumn) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name_" + std::to_string(i));

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    std::vector<TabCol> sel_cols = {{TAB_A, "score"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);

    ASSERT_EQ(proj.tupleLen(), 4u);  // INT = 4 bytes
    ASSERT_EQ(proj.cols().size(), 1u);
    ASSERT_EQ(proj.cols()[0].name, "score");

    std::vector<int> scores;
    for (proj.beginTuple(); !proj.is_end(); proj.nextTuple()) {
        auto rec = proj.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(rec->size, 4);
        scores.push_back(*(int *)(rec->data));
    }
    std::sort(scores.begin(), scores.end());
    ASSERT_EQ(scores, (std::vector<int>{10, 20, 30, 40, 50}));
}

TEST_F(ExecutorFullTest, Projection_ReorderColumns) {
    insert_a(42, 99, "hello");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    // 选 score, id（倒序）
    std::vector<TabCol> sel_cols = {{TAB_A, "score"}, {TAB_A, "id"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);

    ASSERT_EQ(proj.tupleLen(), 8u);
    ASSERT_EQ(proj.cols().size(), 2u);
    ASSERT_EQ(proj.cols()[0].name, "score");
    ASSERT_EQ(proj.cols()[1].name, "id");

    proj.beginTuple();
    ASSERT_FALSE(proj.is_end());
    auto rec = proj.Next();
    ASSERT_NE(rec, nullptr);

    // 输出记录：offset 0 = score=99, offset 4 = id=42
    ASSERT_EQ(*(int *)(rec->data + 0), 99);
    ASSERT_EQ(*(int *)(rec->data + 4), 42);

    proj.nextTuple();
    ASSERT_TRUE(proj.is_end());
}

TEST_F(ExecutorFullTest, Projection_AllColumns) {
    insert_a(1, 10, "abc");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    std::vector<TabCol> sel_cols = {{TAB_A, "id"}, {TAB_A, "score"}, {TAB_A, "name"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);
    ASSERT_EQ(proj.tupleLen(), (size_t)rec_size_a_);

    proj.beginTuple();
    auto rec = proj.Next();
    ASSERT_NE(rec, nullptr);
    ASSERT_EQ(*(int *)(rec->data + 0), 1);
    ASSERT_EQ(*(int *)(rec->data + 4), 10);
    // name 从 offset 8
    std::string name(rec->data + 8, 3);
    ASSERT_EQ(name, "abc");
}

TEST_F(ExecutorFullTest, Projection_MetaInfo) {
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    std::vector<TabCol> sel_cols = {{TAB_A, "name"}, {TAB_A, "id"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);
    ASSERT_EQ(proj.getType(), "ProjectionExecutor");
    ASSERT_EQ(proj.cols().size(), 2u);
    ASSERT_EQ(proj.tupleLen(), 20u);  // name(16) + id(4)
}

TEST_F(ExecutorFullTest, Projection_LargeData) {
    int n = 200;
    for (int i = 0; i < n; i++) insert_a(i, i * 2, "x");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    std::vector<TabCol> sel_cols = {{TAB_A, "id"}};

    ProjectionExecutor proj(std::move(scan), sel_cols);
    int count = 0;
    for (proj.beginTuple(); !proj.is_end(); proj.nextTuple()) {
        auto rec = proj.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(rec->size, 4);
        count++;
    }
    ASSERT_EQ(count, n);
}

// ═══════════════════════════════════════════════════════════════
//  SortExecutor 测试
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Sort_EmptyTable) {
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);
    sort.beginTuple();
    ASSERT_TRUE(sort.is_end());
    ASSERT_EQ(sort.Next(), nullptr);
}

TEST_F(ExecutorFullTest, Sort_SingleRecord) {
    insert_a(42, 100, "only");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);
    sort.beginTuple();
    ASSERT_FALSE(sort.is_end());

    auto rec = sort.Next();
    ASSERT_NE(rec, nullptr);
    ASSERT_EQ(*(int *)(rec->data), 42);

    sort.nextTuple();
    ASSERT_TRUE(sort.is_end());
}

TEST_F(ExecutorFullTest, Sort_AscendingOrder) {
    // 插入乱序
    insert_a(5, 50, "e");
    insert_a(3, 30, "c");
    insert_a(1, 10, "a");
    insert_a(4, 40, "d");
    insert_a(2, 20, "b");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);  // ASC

    std::vector<int> sorted_ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        sorted_ids.push_back(*(int *)(rec->data));
    }
    ASSERT_EQ(sorted_ids, (std::vector<int>{1, 2, 3, 4, 5}));
}

TEST_F(ExecutorFullTest, Sort_DescendingOrder) {
    insert_a(1, 10, "a");
    insert_a(3, 30, "c");
    insert_a(2, 20, "b");
    insert_a(5, 50, "e");
    insert_a(4, 40, "d");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, true);  // DESC

    std::vector<int> sorted_ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        sorted_ids.push_back(*(int *)(rec->data));
    }
    ASSERT_EQ(sorted_ids, (std::vector<int>{5, 4, 3, 2, 1}));
}

TEST_F(ExecutorFullTest, Sort_ByNonFirstColumn) {
    insert_a(1, 30, "c");
    insert_a(2, 10, "a");
    insert_a(3, 20, "b");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "score"};

    SortExecutor sort(std::move(scan), sort_col, false);  // ASC by score

    std::vector<int> ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        ids.push_back(*(int *)(rec->data));  // id
    }
    // score 10→20→30 对应 id 2→3→1
    ASSERT_EQ(ids, (std::vector<int>{2, 3, 1}));
}

TEST_F(ExecutorFullTest, Sort_DuplicateValues) {
    insert_a(3, 10, "a");
    insert_a(1, 10, "b");
    insert_a(2, 10, "c");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "score"};

    SortExecutor sort(std::move(scan), sort_col, false);

    int count = 0;
    int prev_score = -1;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        int score = *(int *)(rec->data + 4);
        ASSERT_GE(score, prev_score);
        prev_score = score;
        count++;
    }
    ASSERT_EQ(count, 3);
}

TEST_F(ExecutorFullTest, Sort_LargeData_Asc) {
    std::mt19937 rng(42);
    int n = 300;
    std::vector<int> expected_ids(n);
    for (int i = 0; i < n; i++) expected_ids[i] = i;
    std::shuffle(expected_ids.begin(), expected_ids.end(), rng);

    for (int i = 0; i < n; i++) {
        insert_a(expected_ids[i], expected_ids[i] * 2, "x");
    }

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);

    std::vector<int> sorted_ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        sorted_ids.push_back(*(int *)(rec->data));
    }

    ASSERT_EQ((int)sorted_ids.size(), n);
    for (int i = 0; i < n; i++) {
        ASSERT_EQ(sorted_ids[i], i);
    }
}

TEST_F(ExecutorFullTest, Sort_PreservesRecordContent) {
    insert_a(2, 200, "two");
    insert_a(1, 100, "one");

    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);

    sort.beginTuple();
    auto rec1 = sort.Next();
    ASSERT_NE(rec1, nullptr);
    ASSERT_EQ(*(int *)(rec1->data + 0), 1);
    ASSERT_EQ(*(int *)(rec1->data + 4), 100);

    sort.nextTuple();
    auto rec2 = sort.Next();
    ASSERT_NE(rec2, nullptr);
    ASSERT_EQ(*(int *)(rec2->data + 0), 2);
    ASSERT_EQ(*(int *)(rec2->data + 4), 200);
}

TEST_F(ExecutorFullTest, Sort_MetaInfo) {
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};

    SortExecutor sort(std::move(scan), sort_col, false);
    ASSERT_EQ(sort.getType(), "SortExecutor");
    ASSERT_EQ(sort.tupleLen(), (size_t)rec_size_a_);
    ASSERT_EQ(sort.cols().size(), 3u);
}

// ═══════════════════════════════════════════════════════════════
//  组合测试: Projection + Sort 流水线
// ═══════════════════════════════════════════════════════════════

TEST_F(ExecutorFullTest, Sort_Then_Projection) {
    insert_a(3, 30, "c");
    insert_a(1, 10, "a");
    insert_a(2, 20, "b");

    // Sort by id ASC → Projection(score)
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};
    auto sort = std::make_unique<SortExecutor>(std::move(scan), sort_col, false);

    std::vector<TabCol> sel_cols = {{TAB_A, "score"}};
    ProjectionExecutor proj(std::move(sort), sel_cols);

    std::vector<int> scores;
    for (proj.beginTuple(); !proj.is_end(); proj.nextTuple()) {
        auto rec = proj.Next();
        ASSERT_NE(rec, nullptr);
        ASSERT_EQ(rec->size, 4);
        scores.push_back(*(int *)(rec->data));
    }
    // Sorted by id(1,2,3), projected scores should be (10,20,30)
    ASSERT_EQ(scores, (std::vector<int>{10, 20, 30}));
}

TEST_F(ExecutorFullTest, Join_Then_Sort) {
    // tab_a: id=3,1  tab_b: bid=1,3
    // JOIN ON id=bid → (1,1),(3,3)  then sort by id ASC
    insert_a(3, 30, "c");
    insert_a(1, 10, "a");

    insert_b(1, 100);
    insert_b(3, 300);

    auto left = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    auto right = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_B, std::vector<Condition>{}, nullptr);

    auto join_cond = make_join_cond("id", "bid");
    auto join = std::make_unique<NestedLoopJoinExecutor>(std::move(left), std::move(right), std::vector<Condition>{join_cond});

    TabCol sort_col = {TAB_A, "id"};
    SortExecutor sort(std::move(join), sort_col, false);

    std::vector<int> ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        ids.push_back(*(int *)(rec->data));
    }
    ASSERT_EQ(ids, (std::vector<int>{1, 3}));
}

TEST_F(ExecutorFullTest, Delete_Then_Sort) {
    for (int i = 1; i <= 5; i++) insert_a(i, i * 10, "name");

    // 删除 id=3
    auto cond = make_cond_a("id", OP_EQ, 3);
    auto rids = scan_rids(TAB_A, {cond});
    DeleteExecutor del(sm_manager_.get(), TAB_A, {cond}, rids, ctx_);
    del.Next();

    // Sort remaining by id DESC
    auto scan = std::make_unique<SeqScanExecutor>(sm_manager_.get(), TAB_A, std::vector<Condition>{}, nullptr);
    TabCol sort_col = {TAB_A, "id"};
    SortExecutor sort(std::move(scan), sort_col, true);

    std::vector<int> ids;
    for (sort.beginTuple(); !sort.is_end(); sort.nextTuple()) {
        auto rec = sort.Next();
        ASSERT_NE(rec, nullptr);
        ids.push_back(*(int *)(rec->data));
    }
    ASSERT_EQ(ids, (std::vector<int>{5, 4, 2, 1}));
}
