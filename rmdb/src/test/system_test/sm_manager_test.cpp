/* SmManager DDL 综合测试
 * 覆盖: open_db / close_db / drop_table / create_index / drop_index
 * 重点: 元数据持久化、索引回填、多表多索引隔离、边界错误路径
 */

#undef NDEBUG
#define private public

#include "storage/disk_manager.h"
#include "storage/buffer_pool_manager.h"
#include "record/rm.h"
#include "index/ix.h"
#include "system/sm_manager.h"

#undef private

#include <unistd.h>
#include <sys/stat.h>
#include <algorithm>
#include <cstring>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace {

const std::string TEST_DB = "SmManagerTest_db";
const std::string TAB_A = "sm_tab_a";
const std::string TAB_B = "sm_tab_b";

class SmManagerTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> dm_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_;
    std::unique_ptr<IxManager> ix_;
    std::unique_ptr<SmManager> sm_;

    // tab_a: id(INT,4), val(INT,4), name(STRING,8) → 16 bytes
    std::vector<ColDef> col_defs_a_ = {
        {"id", TYPE_INT, 4},
        {"val", TYPE_INT, 4},
        {"name", TYPE_STRING, 8}};
    // tab_b: bid(INT,4), ref(INT,4) → 8 bytes
    std::vector<ColDef> col_defs_b_ = {
        {"bid", TYPE_INT, 4},
        {"ref", TYPE_INT, 4}};

    void SetUp() override {
        build_managers();
        // 清理残留
        if (sm_->is_dir(TEST_DB)) {
            sm_->drop_db(TEST_DB);
        }
        sm_->create_db(TEST_DB);
        sm_->open_db(TEST_DB);
    }

    void TearDown() override {
        // 关闭 (可能已被测试手动关闭)
        if (!sm_->fhs_.empty() || !sm_->ihs_.empty() || !sm_->db_.name_.empty()) {
            sm_->close_db();
        }
        // 删库
        if (sm_->is_dir(TEST_DB)) {
            sm_->drop_db(TEST_DB);
        }
    }

    void build_managers() {
        dm_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(64, dm_.get());
        rm_ = std::make_unique<RmManager>(dm_.get(), bpm_.get());
        ix_ = std::make_unique<IxManager>(dm_.get(), bpm_.get());
        sm_ = std::make_unique<SmManager>(dm_.get(), bpm_.get(), rm_.get(), ix_.get());
    }

    // 向 TAB_A 插入一行
    Rid insert_a(int id, int val, const std::string &name) {
        auto *fh = sm_->fhs_.at(TAB_A).get();
        char buf[16];
        memset(buf, 0, sizeof(buf));
        *(int *)(buf + 0) = id;
        *(int *)(buf + 4) = val;
        std::strncpy(buf + 8, name.c_str(), 8);
        return fh->insert_record(buf, nullptr);
    }
};

// ═════════════════════════════════════════════════════════════
//  Section 1: open_db / close_db 生命周期
// ═════════════════════════════════════════════════════════════

// 基础往返: 打开 → 关闭 → 再打开, 元数据正确恢复
TEST_F(SmManagerTest, DB_OpenCloseRoundtrip_Empty) {
    // 当前状态: open 完成, 库为空
    ASSERT_TRUE(sm_->db_.tabs_.empty());
    sm_->close_db();
    ASSERT_TRUE(sm_->db_.tabs_.empty());
    ASSERT_TRUE(sm_->fhs_.empty());
    ASSERT_TRUE(sm_->ihs_.empty());

    // 再次打开
    sm_->open_db(TEST_DB);
    ASSERT_TRUE(sm_->db_.tabs_.empty());
}

// 打开不存在的库应抛异常
TEST_F(SmManagerTest, DB_OpenNonexistent_Throws) {
    // 当前已在 TEST_DB 内, close 后即可
    sm_->close_db();
    ASSERT_THROW(sm_->open_db("nonexistent_db_xyz"), DatabaseNotFoundError);
    // 恢复以便 TearDown
    sm_->open_db(TEST_DB);
}

// close_db 后 chdir 回到上级目录
TEST_F(SmManagerTest, DB_CloseReturnsToParentDir) {
    char before[4096]; ASSERT_TRUE(getcwd(before, sizeof(before)) != nullptr);
    sm_->close_db();
    char after[4096]; ASSERT_TRUE(getcwd(after, sizeof(after)) != nullptr);
    ASSERT_STRNE(before, after);
    sm_->open_db(TEST_DB);
}

// ═════════════════════════════════════════════════════════════
//  Section 2: create_table / drop_table + 元数据持久化
// ═════════════════════════════════════════════════════════════

// 创建表后关闭并重新打开, 表应仍存在
TEST_F(SmManagerTest, Table_CreateTable_PersistsAfterReopen) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    ASSERT_TRUE(sm_->db_.is_table(TAB_A));
    ASSERT_EQ(sm_->fhs_.count(TAB_A), 1u);

    sm_->close_db();
    sm_->open_db(TEST_DB);

    ASSERT_TRUE(sm_->db_.is_table(TAB_A));
    ASSERT_EQ(sm_->fhs_.count(TAB_A), 1u);
    auto &tab = sm_->db_.get_table(TAB_A);
    ASSERT_EQ(tab.cols.size(), 3u);
    ASSERT_EQ(tab.cols[0].name, "id");
    ASSERT_EQ(tab.cols[2].type, TYPE_STRING);
    ASSERT_EQ(tab.cols[2].len, 8);
}

// drop_table: 删除表 + 数据文件 + 元数据
TEST_F(SmManagerTest, Table_DropTable_RemovesFileAndMeta) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    ASSERT_TRUE(dm_->is_file(TAB_A));

    sm_->drop_table(TAB_A, nullptr);

    ASSERT_FALSE(sm_->db_.is_table(TAB_A));
    ASSERT_EQ(sm_->fhs_.count(TAB_A), 0u);
    ASSERT_FALSE(dm_->is_file(TAB_A));

    // 关闭重开仍为空
    sm_->close_db();
    sm_->open_db(TEST_DB);
    ASSERT_FALSE(sm_->db_.is_table(TAB_A));
}

// drop_table 不存在的表应抛异常
TEST_F(SmManagerTest, Table_DropNonexistent_Throws) {
    ASSERT_THROW(sm_->drop_table("no_such_table", nullptr), TableNotFoundError);
}

// drop_table 会连带删除该表所有索引
TEST_F(SmManagerTest, Table_DropTable_CascadeDropsAllIndexes) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"val"}, nullptr);
    ASSERT_EQ(sm_->ihs_.size(), 2u);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).indexes.size(), 2u);

    auto id_ix = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    auto val_ix = ix_->get_index_name(TAB_A, std::vector<std::string>{"val"});
    ASSERT_TRUE(dm_->is_file(id_ix));
    ASSERT_TRUE(dm_->is_file(val_ix));

    sm_->drop_table(TAB_A, nullptr);

    ASSERT_TRUE(sm_->ihs_.empty());
    ASSERT_FALSE(dm_->is_file(id_ix));
    ASSERT_FALSE(dm_->is_file(val_ix));
    ASSERT_FALSE(dm_->is_file(TAB_A));
}

// 同名表可先删后重建
TEST_F(SmManagerTest, Table_CreateDropRecreate) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    insert_a(1, 100, "hello");
    sm_->drop_table(TAB_A, nullptr);

    // 用不同 schema 重建
    sm_->create_table(TAB_A, col_defs_b_, nullptr);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).cols.size(), 2u);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).cols[0].name, "bid");
}

// ═════════════════════════════════════════════════════════════
//  Section 3: create_index 正确性 + 回填
// ═════════════════════════════════════════════════════════════

// 在空表上创建索引
TEST_F(SmManagerTest, Index_CreateOnEmptyTable) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);

    auto ix_name = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    ASSERT_TRUE(dm_->is_file(ix_name));
    ASSERT_EQ(sm_->ihs_.count(ix_name), 1u);
    auto &tab = sm_->db_.get_table(TAB_A);
    ASSERT_EQ(tab.indexes.size(), 1u);
    ASSERT_EQ(tab.indexes[0].col_num, 1);
    ASSERT_EQ(tab.indexes[0].cols[0].name, "id");
}

// 在已有数据上创建索引, 应回填所有记录
TEST_F(SmManagerTest, Index_CreateAfterData_BackfillsExisting) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);

    constexpr int N = 200;
    std::vector<Rid> rids(N);
    for (int i = 0; i < N; i++) {
        rids[i] = insert_a(i, i * 10, "x");
    }

    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);

    // 验证: 所有已存在记录都能通过索引查到
    auto ix_name = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    auto *ih = sm_->ihs_.at(ix_name).get();
    Transaction txn(0);
    for (int i = 0; i < N; i++) {
        char key[4]; *(int *)key = i;
        std::vector<Rid> out;
        bool found = ih->get_value(key, &out, &txn);
        ASSERT_TRUE(found) << "key " << i << " not found after backfill";
        ASSERT_EQ(out.size(), 1u);
        ASSERT_EQ(out[0].page_no, rids[i].page_no);
        ASSERT_EQ(out[0].slot_no, rids[i].slot_no);
    }
}

// 重复创建同一索引应抛异常
TEST_F(SmManagerTest, Index_CreateDuplicate_Throws) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    ASSERT_THROW(sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr),
                 IndexExistsError);
}

// 多列联合索引
TEST_F(SmManagerTest, Index_MultiColumnIndex) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id", "val"}, nullptr);

    auto &tab = sm_->db_.get_table(TAB_A);
    ASSERT_EQ(tab.indexes.size(), 1u);
    ASSERT_EQ(tab.indexes[0].col_num, 2);
    ASSERT_EQ(tab.indexes[0].col_tot_len, 8);
}

// 对不存在的表建索引应抛异常
TEST_F(SmManagerTest, Index_CreateOnNonexistentTable_Throws) {
    ASSERT_THROW(sm_->create_index("no_tab", std::vector<std::string>{"x"}, nullptr),
                 TableNotFoundError);
}

// ═════════════════════════════════════════════════════════════
//  Section 4: drop_index 两个重载
// ═════════════════════════════════════════════════════════════

TEST_F(SmManagerTest, Index_DropIndex_StringOverload) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    auto ix_name = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    ASSERT_TRUE(dm_->is_file(ix_name));

    sm_->drop_index(TAB_A, std::vector<std::string>{"id"}, nullptr);

    ASSERT_FALSE(dm_->is_file(ix_name));
    ASSERT_EQ(sm_->ihs_.count(ix_name), 0u);
    ASSERT_TRUE(sm_->db_.get_table(TAB_A).indexes.empty());
}

TEST_F(SmManagerTest, Index_DropIndex_ColMetaOverload) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"val"}, nullptr);

    auto &tab = sm_->db_.get_table(TAB_A);
    auto cols = tab.indexes[0].cols;
    sm_->drop_index(TAB_A, cols, nullptr);

    auto ix_name = ix_->get_index_name(TAB_A, cols);
    ASSERT_FALSE(dm_->is_file(ix_name));
    ASSERT_TRUE(tab.indexes.empty());
}

// 删除不存在的索引应抛异常
TEST_F(SmManagerTest, Index_DropNonexistent_Throws) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    // 无索引, 访问 ihs_.at 会抛 out_of_range
    ASSERT_ANY_THROW(sm_->drop_index(TAB_A, std::vector<std::string>{"id"}, nullptr));
}

// 删除索引不影响其他同表索引
TEST_F(SmManagerTest, Index_DropOne_KeepOthers) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"val"}, nullptr);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).indexes.size(), 2u);

    sm_->drop_index(TAB_A, std::vector<std::string>{"id"}, nullptr);

    auto id_ix = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    auto val_ix = ix_->get_index_name(TAB_A, std::vector<std::string>{"val"});
    ASSERT_FALSE(dm_->is_file(id_ix));
    ASSERT_TRUE(dm_->is_file(val_ix));
    ASSERT_EQ(sm_->db_.get_table(TAB_A).indexes.size(), 1u);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).indexes[0].cols[0].name, "val");
}

// ═════════════════════════════════════════════════════════════
//  Section 5: 跨 close_db/open_db 的持久化
// ═════════════════════════════════════════════════════════════

// 索引元数据和索引文件应跨关闭重开持久
TEST_F(SmManagerTest, Roundtrip_IndexPersistsAfterClose) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);

    constexpr int N = 50;
    for (int i = 0; i < N; i++) insert_a(i, i, "r");
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);

    sm_->close_db();
    sm_->open_db(TEST_DB);

    auto &tab = sm_->db_.get_table(TAB_A);
    ASSERT_EQ(tab.indexes.size(), 1u);
    auto ix_name = ix_->get_index_name(TAB_A, std::vector<std::string>{"id"});
    ASSERT_EQ(sm_->ihs_.count(ix_name), 1u);

    // 重新打开后索引依然可查
    auto *ih = sm_->ihs_.at(ix_name).get();
    Transaction txn(0);
    for (int i = 0; i < N; i++) {
        char key[4]; *(int *)key = i;
        std::vector<Rid> out;
        ASSERT_TRUE(ih->get_value(key, &out, &txn)) << "key=" << i;
    }
}

// ═════════════════════════════════════════════════════════════
//  Section 6: 多表多索引隔离
// ═════════════════════════════════════════════════════════════

TEST_F(SmManagerTest, MultiTable_MultiIndex_Isolation) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_table(TAB_B, col_defs_b_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    sm_->create_index(TAB_B, std::vector<std::string>{"bid"}, nullptr);

    ASSERT_EQ(sm_->db_.tabs_.size(), 2u);
    ASSERT_EQ(sm_->ihs_.size(), 2u);

    // 删 TAB_A 不影响 TAB_B
    sm_->drop_table(TAB_A, nullptr);

    ASSERT_FALSE(sm_->db_.is_table(TAB_A));
    ASSERT_TRUE(sm_->db_.is_table(TAB_B));
    ASSERT_EQ(sm_->db_.get_table(TAB_B).indexes.size(), 1u);
    auto b_ix = ix_->get_index_name(TAB_B, std::vector<std::string>{"bid"});
    ASSERT_TRUE(dm_->is_file(b_ix));
    ASSERT_EQ(sm_->ihs_.count(b_ix), 1u);
}

// 关闭后重开, 多表多索引均应恢复
TEST_F(SmManagerTest, MultiTable_RoundtripAllMetaPersists) {
    sm_->create_table(TAB_A, col_defs_a_, nullptr);
    sm_->create_table(TAB_B, col_defs_b_, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"id"}, nullptr);
    sm_->create_index(TAB_A, std::vector<std::string>{"name"}, nullptr);
    sm_->create_index(TAB_B, std::vector<std::string>{"bid", "ref"}, nullptr);

    sm_->close_db();
    sm_->open_db(TEST_DB);

    ASSERT_EQ(sm_->db_.tabs_.size(), 2u);
    ASSERT_EQ(sm_->db_.get_table(TAB_A).indexes.size(), 2u);
    ASSERT_EQ(sm_->db_.get_table(TAB_B).indexes.size(), 1u);
    ASSERT_EQ(sm_->db_.get_table(TAB_B).indexes[0].col_num, 2);
    ASSERT_EQ(sm_->ihs_.size(), 3u);
    ASSERT_EQ(sm_->fhs_.size(), 2u);
}

}  // namespace
