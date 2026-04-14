/* SeqScanExecutor & IndexScanExecutor 单元测试 */

#undef NDEBUG
#define private public

#include "record/rm.h"
#include "index/ix.h"
#include "system/sm_manager.h"
#include "execution/executor_seq_scan.h"
#include "execution/executor_index_scan.h"

#undef private

#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <vector>

#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"
#include "transaction/transaction.h"

const std::string TEST_DB_NAME = "ExecutorScanTest_db";
constexpr size_t TEST_BUFFER_POOL_SIZE = 256;
const std::string TEST_TAB_NAME = "test_table";

// ─── 测试 Fixture ─────────────────────────────────────────────

class ExecutorScanTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;
    std::unique_ptr<IxManager> ix_manager_;
    std::unique_ptr<SmManager> sm_manager_;
    Transaction txn_{0};

    // 表结构: id (INT, 4), score (INT, 4), name (STRING, 16) → record_size = 24
    std::vector<ColDef> col_defs_ = {
        {"id", TYPE_INT, 4},
        {"score", TYPE_INT, 4},
        {"name", TYPE_STRING, 16},
    };
    int record_size_ = 24;  // 4 + 4 + 16

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), bpm_.get());
        sm_manager_ = std::make_unique<SmManager>(disk_manager_.get(), bpm_.get(),
                                                   rm_manager_.get(), ix_manager_.get());

        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }

        // 清理可能残留的表文件
        if (disk_manager_->is_file(TEST_TAB_NAME)) {
            disk_manager_->destroy_file(TEST_TAB_NAME);
        }
        sm_manager_->db_.tabs_.clear();

        // 通过 SmManager::create_table 建表（该方法已实现），会注册 db_.tabs_ 和 fhs_
        sm_manager_->create_table(TEST_TAB_NAME, col_defs_, nullptr);
    }

    void TearDown() override {
        // 关闭所有索引 handle 并销毁索引文件
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
        if (disk_manager_->is_file(TEST_TAB_NAME)) {
            disk_manager_->destroy_file(TEST_TAB_NAME);
        }
        sm_manager_->db_.tabs_.clear();

        if (chdir("..") < 0) {
            throw UnixError();
        }
    }

    // 插入一条记录: (id, score, name)
    Rid insert_record(int id, int score, const std::string &name) {
        auto *fh = sm_manager_->fhs_.at(TEST_TAB_NAME).get();
        char buf[24];
        memset(buf, 0, sizeof(buf));
        *(int *)(buf + 0) = id;
        *(int *)(buf + 4) = score;
        memcpy(buf + 8, name.c_str(), std::min(name.size(), (size_t)16));
        return fh->insert_record(buf, nullptr);
    }

    // 构造一个等值条件: col_name = int_val
    Condition make_eq_cond_int(const std::string &col_name, int val) {
        Condition cond;
        cond.lhs_col = {TEST_TAB_NAME, col_name};
        cond.op = OP_EQ;
        cond.is_rhs_val = true;

        cond.rhs_val.type = TYPE_INT;
        cond.rhs_val.int_val = val;

        // 查找列长度
        auto &tab = sm_manager_->db_.get_table(TEST_TAB_NAME);
        for (auto &col : tab.cols) {
            if (col.name == col_name) {
                cond.rhs_val.init_raw(col.len);
                break;
            }
        }
        return cond;
    }

    // 构造一个比较条件: col_name op int_val
    Condition make_cmp_cond_int(const std::string &col_name, CompOp op, int val) {
        Condition cond;
        cond.lhs_col = {TEST_TAB_NAME, col_name};
        cond.op = op;
        cond.is_rhs_val = true;

        cond.rhs_val.type = TYPE_INT;
        cond.rhs_val.int_val = val;

        auto &tab = sm_manager_->db_.get_table(TEST_TAB_NAME);
        for (auto &col : tab.cols) {
            if (col.name == col_name) {
                cond.rhs_val.init_raw(col.len);
                break;
            }
        }
        return cond;
    }

    // 为 id 列创建索引，并注册到 sm_manager_->ihs_ 和 db_ 元数据
    void create_index_on_id() {
        auto &tab = sm_manager_->db_.get_table(TEST_TAB_NAME);
        std::vector<ColMeta> idx_cols;
        for (auto &col : tab.cols) {
            if (col.name == "id") {
                idx_cols.push_back(col);
                break;
            }
        }
        std::string ix_name = ix_manager_->get_index_name(TEST_TAB_NAME, idx_cols);
        if (ix_manager_->exists(TEST_TAB_NAME, idx_cols)) {
            ix_manager_->destroy_index(TEST_TAB_NAME, idx_cols);
        }
        ix_manager_->create_index(TEST_TAB_NAME, idx_cols);

        // 注册到 ihs_
        sm_manager_->ihs_.emplace(ix_name, ix_manager_->open_index(TEST_TAB_NAME, idx_cols));

        // 注册索引元数据到 tab
        IndexMeta idx_meta;
        idx_meta.tab_name = TEST_TAB_NAME;
        idx_meta.col_num = 1;
        idx_meta.col_tot_len = idx_cols[0].len;
        idx_meta.cols = idx_cols;
        tab.indexes.push_back(idx_meta);
    }

    // 向索引中插入一条 (key=id, rid)
    void insert_index_entry(int id, const Rid &rid) {
        std::vector<std::string> idx_col_names = {"id"};
        std::string ix_name = ix_manager_->get_index_name(TEST_TAB_NAME, idx_col_names);
        auto *ih = sm_manager_->ihs_.at(ix_name).get();
        char key[4];
        *(int *)key = id;
        ih->insert_entry(key, rid, &txn_);
    }

    // 收集扫描器的所有结果 id
    std::vector<int> collect_ids(AbstractExecutor *exec) {
        std::vector<int> ids;
        for (exec->beginTuple(); !exec->is_end(); exec->nextTuple()) {
            auto rec = exec->Next();
            if (rec) {
                ids.push_back(*(int *)(rec->data));
            }
        }
        return ids;
    }
};

// ═══════════════════════════════════════════════════════════════
//  SeqScanExecutor 测试
// ═══════════════════════════════════════════════════════════════

// ─── 空表扫描 ─────────────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_EmptyTable) {
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {}, nullptr);

    exec.beginTuple();
    ASSERT_TRUE(exec.is_end());
    auto rec = exec.Next();
    ASSERT_EQ(rec, nullptr);
}

// ─── 无条件全表扫描 ──────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_NoCondition) {
    // 插入 10 条记录
    for (int i = 1; i <= 10; i++) {
        insert_record(i, i * 10, "name_" + std::to_string(i));
    }

    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {}, nullptr);

    std::set<int> scanned_ids;
    for (exec.beginTuple(); !exec.is_end(); exec.nextTuple()) {
        auto rec = exec.Next();
        ASSERT_NE(rec, nullptr);
        int id = *(int *)(rec->data);
        scanned_ids.insert(id);
    }

    ASSERT_EQ(scanned_ids.size(), 10u);
    for (int i = 1; i <= 10; i++) {
        ASSERT_TRUE(scanned_ids.count(i) > 0) << "Missing id=" << i;
    }
}

// ─── 等值条件扫描 ────────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_EqualCondition) {
    for (int i = 1; i <= 20; i++) {
        insert_record(i, i * 10, "name_" + std::to_string(i));
    }

    // WHERE id = 15
    auto cond = make_eq_cond_int("id", 15);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(ids[0], 15);
}

// ─── 大于条件扫描 ────────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_GreaterThan) {
    for (int i = 1; i <= 10; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE id > 7
    auto cond = make_cmp_cond_int("id", OP_GT, 7);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{8, 9, 10}));
}

// ─── 小于等于条件扫描 ────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_LessEqual) {
    for (int i = 1; i <= 10; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE id <= 3
    auto cond = make_cmp_cond_int("id", OP_LE, 3);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{1, 2, 3}));
}

// ─── 多条件 AND 扫描 ────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_MultiCondition) {
    for (int i = 1; i <= 20; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE id >= 5 AND id <= 10
    auto cond1 = make_cmp_cond_int("id", OP_GE, 5);
    auto cond2 = make_cmp_cond_int("id", OP_LE, 10);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond1, cond2}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{5, 6, 7, 8, 9, 10}));
}

// ─── 不等于条件扫描 ─────────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_NotEqual) {
    for (int i = 1; i <= 5; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE id != 3
    auto cond = make_cmp_cond_int("id", OP_NE, 3);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{1, 2, 4, 5}));
}

// ─── 无匹配结果的条件扫描 ────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_NoMatch) {
    for (int i = 1; i <= 10; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE id = 999 → 无匹配
    auto cond = make_eq_cond_int("id", 999);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_TRUE(ids.empty());
}

// ─── 对非首列字段过滤 ───────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_FilterOnScore) {
    for (int i = 1; i <= 10; i++) {
        insert_record(i, i * 10, "name");
    }

    // WHERE score = 50 → 对应 id=5
    auto cond = make_eq_cond_int("score", 50);
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(ids[0], 5);
}

// ─── 大量数据扫描（跨页）─────────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_LargeData) {
    int n = 500;
    for (int i = 1; i <= n; i++) {
        insert_record(i, i % 100, "name");
    }

    // 无条件全扫描
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_EQ((int)ids.size(), n);

    // 带条件：WHERE score = 0 → 对应 id=100,200,300,400,500
    auto cond = make_eq_cond_int("score", 0);
    SeqScanExecutor exec2(sm_manager_.get(), TEST_TAB_NAME, {cond}, nullptr);
    auto ids2 = collect_ids(&exec2);
    ASSERT_EQ(ids2.size(), 5u);
}

// ─── tupleLen 和 cols 元信息 ─────────────────────────────────

TEST_F(ExecutorScanTest, SeqScan_MetaInfo) {
    SeqScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {}, nullptr);

    ASSERT_EQ(exec.tupleLen(), (size_t)record_size_);
    ASSERT_EQ(exec.cols().size(), 3u);
    ASSERT_EQ(exec.cols()[0].name, "id");
    ASSERT_EQ(exec.cols()[1].name, "score");
    ASSERT_EQ(exec.cols()[2].name, "name");
    ASSERT_EQ(exec.getType(), "SeqScanExecutor");
}

// ═══════════════════════════════════════════════════════════════
//  IndexScanExecutor 测试
// ═══════════════════════════════════════════════════════════════

// ─── 索引等值查询 ────────────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_EqualCondition) {
    create_index_on_id();

    for (int i = 1; i <= 20; i++) {
        Rid rid = insert_record(i, i * 10, "name_" + std::to_string(i));
        insert_index_entry(i, rid);
    }

    // WHERE id = 10
    auto cond = make_eq_cond_int("id", 10);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_EQ(ids.size(), 1u);
    ASSERT_EQ(ids[0], 10);
}

// ─── 索引范围查询 GT ─────────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_GreaterThan) {
    create_index_on_id();

    for (int i = 1; i <= 10; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // WHERE id > 7
    auto cond = make_cmp_cond_int("id", OP_GT, 7);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{8, 9, 10}));
}

// ─── 索引范围查询 LT ─────────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_LessThan) {
    create_index_on_id();

    for (int i = 1; i <= 10; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // WHERE id < 4
    auto cond = make_cmp_cond_int("id", OP_LT, 4);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{1, 2, 3}));
}

// ─── 索引范围查询 GE + LE ────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_RangeBetween) {
    create_index_on_id();

    for (int i = 1; i <= 20; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // WHERE id >= 5 AND id <= 10
    auto cond1 = make_cmp_cond_int("id", OP_GE, 5);
    auto cond2 = make_cmp_cond_int("id", OP_LE, 10);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond1, cond2}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());
    ASSERT_EQ(ids, (std::vector<int>{5, 6, 7, 8, 9, 10}));
}

// ─── 索引扫描无匹配 ─────────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_NoMatch) {
    create_index_on_id();

    for (int i = 1; i <= 10; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // WHERE id = 999
    auto cond = make_eq_cond_int("id", 999);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    ASSERT_TRUE(ids.empty());
}

// ─── 索引扫描元信息 ─────────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_MetaInfo) {
    create_index_on_id();

    auto cond = make_eq_cond_int("id", 1);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);

    ASSERT_EQ(exec.tupleLen(), (size_t)record_size_);
    ASSERT_EQ(exec.cols().size(), 3u);
    ASSERT_EQ(exec.getType(), "IndexScanExecutor");
}

// ─── 索引大量数据等值查询 ────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_LargeDataPointQuery) {
    create_index_on_id();

    int n = 200;
    for (int i = 1; i <= n; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // 逐个点查验证
    for (int target = 1; target <= n; target += 17) {
        auto cond = make_eq_cond_int("id", target);
        IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond}, {"id"}, nullptr);
        auto ids = collect_ids(&exec);
        ASSERT_EQ(ids.size(), 1u) << "Failed for target=" << target;
        ASSERT_EQ(ids[0], target);
    }
}

// ─── 索引大量数据范围查询 ────────────────────────────────────

TEST_F(ExecutorScanTest, IndexScan_LargeDataRangeQuery) {
    create_index_on_id();

    int n = 200;
    for (int i = 1; i <= n; i++) {
        Rid rid = insert_record(i, i * 10, "name");
        insert_index_entry(i, rid);
    }

    // WHERE id >= 50 AND id <= 100
    auto cond1 = make_cmp_cond_int("id", OP_GE, 50);
    auto cond2 = make_cmp_cond_int("id", OP_LE, 100);
    IndexScanExecutor exec(sm_manager_.get(), TEST_TAB_NAME, {cond1, cond2}, {"id"}, nullptr);

    auto ids = collect_ids(&exec);
    std::sort(ids.begin(), ids.end());

    std::vector<int> expected;
    for (int i = 50; i <= 100; i++) expected.push_back(i);
    ASSERT_EQ(ids, expected);
}
