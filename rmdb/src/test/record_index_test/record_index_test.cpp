/* Record Manager & B+ Tree Index 单元测试 */

#undef NDEBUG
#define private public

#include "record/rm.h"
#include "index/ix.h"
#include "storage/buffer_pool_manager.h"

#undef private

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"
#include "system/sm_meta.h"
#include "transaction/transaction.h"

const std::string TEST_DB_NAME = "RecordIndexTest_db";
constexpr int MAX_PAGES = 128;
constexpr size_t TEST_BUFFER_POOL_SIZE = 256;

// ─── 辅助工具 ────────────────────────────────────────────────

void rand_buf(int size, char *buf) {
    for (int i = 0; i < size; i++) {
        buf[i] = rand() & 0xff;
    }
}

struct rid_hash_t {
    size_t operator()(const Rid &rid) const { return (rid.page_no << 16) | rid.slot_no; }
};

struct rid_equal_t {
    bool operator()(const Rid &x, const Rid &y) const { return x.page_no == y.page_no && x.slot_no == y.slot_no; }
};

std::ostream &operator<<(std::ostream &os, const Rid &rid) {
    return os << '(' << rid.page_no << ", " << rid.slot_no << ')';
}

// 验证 mock 与 file_handle 一致性
void check_equal(const RmFileHandle *file_handle,
                 const std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> &mock) {
    // 逐条验证 get_record
    for (auto &entry : mock) {
        Rid rid = entry.first;
        auto mock_buf = (char *)entry.second.c_str();
        auto rec = file_handle->get_record(rid, nullptr);
        ASSERT_EQ(0, memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size));
    }
    // 随机验证 is_record
    for (int i = 0; i < 10; i++) {
        Rid rid = {.page_no = 1 + rand() % (file_handle->file_hdr_.num_pages - 1),
                   .slot_no = rand() % file_handle->file_hdr_.num_records_per_page};
        bool mock_exist = mock.count(rid) > 0;
        bool rm_exist = file_handle->is_record(rid);
        ASSERT_EQ(rm_exist, mock_exist);
    }
    // RmScan 全表扫描验证
    size_t num_records = 0;
    for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
        ASSERT_TRUE(mock.count(scan.rid()) > 0);
        auto rec = file_handle->get_record(scan.rid(), nullptr);
        ASSERT_EQ(0, memcmp(rec->data, mock.at(scan.rid()).c_str(), file_handle->file_hdr_.record_size));
        num_records++;
    }
    ASSERT_EQ(num_records, mock.size());
}

// ─── Record Manager 测试 Fixture ──────────────────────────────

class RmTestBase : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;
    int fd_ = -1;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());

        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
    }

    void TearDown() override {
        if (chdir("..") < 0) {
            throw UnixError();
        }
    }

    std::unique_ptr<RmFileHandle> open_file(const std::string &filename, int record_size) {
        if (disk_manager_->is_file(filename)) {
            disk_manager_->destroy_file(filename);
        }
        rm_manager_->create_file(filename, record_size);
        return rm_manager_->open_file(filename);
    }

    void close_file(RmFileHandle *fh) { rm_manager_->close_file(fh); }
};

// ─── Test 1: 基本插入与读取 ─────────────────────────────────

TEST_F(RmTestBase, InsertAndGetBasic) {
    auto fh = open_file("test_basic.txt", 8);
    char buf[8];
    std::vector<Rid> rids;

    // 插入 10 条记录
    for (int i = 0; i < 10; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        rids.push_back(rid);
    }

    // 逐条读取验证
    for (int i = 0; i < 10; i++) {
        auto rec = fh->get_record(rids[i], nullptr);
        ASSERT_EQ(i, *(int *)rec->data);
    }

    close_file(fh.get());
}

// ─── Test 2: 单页填满 + 跨页插入 ────────────────────────────

TEST_F(RmTestBase, InsertFillPage) {
    int record_size = 16;
    auto fh = open_file("test_fill.txt", record_size);
    int n_per_page = fh->file_hdr_.num_records_per_page;

    char buf[16];
    std::vector<Rid> rids;

    // 插入超过一页容量的记录
    for (int i = 0; i < n_per_page + 5; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        rids.push_back(rid);
    }

    // 验证所有记录
    for (int i = 0; i < n_per_page + 5; i++) {
        auto rec = fh->get_record(rids[i], nullptr);
        ASSERT_EQ(i, *(int *)rec->data);
    }

    // 应该有2个数据页
    ASSERT_GE(fh->file_hdr_.num_pages, 3);  // page 0 = header, page 1,2 = data

    close_file(fh.get());
}

// ─── Test 3: 删除记录 ────────────────────────────────────────

TEST_F(RmTestBase, DeleteRecords) {
    auto fh = open_file("test_delete.txt", 8);
    char buf[8];
    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    // 插入 20 条
    for (int i = 0; i < 20; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        mock[rid] = std::string(buf, 8);
    }
    ASSERT_EQ(mock.size(), 20u);

    // 删除前 10 条
    int del_cnt = 0;
    for (auto it = mock.begin(); it != mock.end() && del_cnt < 10;) {
        Rid rid = it->first;
        ++it;
        fh->delete_record(rid, nullptr);
        mock.erase(rid);
        del_cnt++;
    }
    ASSERT_EQ(mock.size(), 10u);

    // 验证剩余记录
    check_equal(fh.get(), mock);

    close_file(fh.get());
}

// ─── Test 4: 更新记录 ────────────────────────────────────────

TEST_F(RmTestBase, UpdateRecords) {
    auto fh = open_file("test_update.txt", 8);
    char buf[8];
    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;
    std::vector<Rid> rids;

    for (int i = 0; i < 15; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        mock[rid] = std::string(buf, 8);
        rids.push_back(rid);
    }

    // 更新所有记录
    for (auto &rid : rids) {
        memset(buf, 0xFF, sizeof(buf));
        *(int *)buf = 9999;
        fh->update_record(rid, buf, nullptr);
        mock[rid] = std::string(buf, 8);
    }

    check_equal(fh.get(), mock);
    close_file(fh.get());
}

// ─── Test 5: 删除后重用空闲 slot ───────────────────────────

TEST_F(RmTestBase, ReuseSlotAfterDelete) {
    auto fh = open_file("test_reuse.txt", 8);
    char buf[8];
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 42;
    Rid rid1 = fh->insert_record(buf, nullptr);

    // 删除
    fh->delete_record(rid1, nullptr);
    ASSERT_FALSE(fh->is_record(rid1));

    // 重新插入，应该能重用该 slot
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 99;
    Rid rid2 = fh->insert_record(buf, nullptr);

    auto rec = fh->get_record(rid2, nullptr);
    ASSERT_EQ(99, *(int *)rec->data);

    close_file(fh.get());
}

// ─── Test 6: 页面满→删除→空闲链表回收 ─────────────────────

TEST_F(RmTestBase, FreePageListRecycle) {
    int record_size = 16;
    auto fh = open_file("test_freelist.txt", record_size);
    int n_per_page = fh->file_hdr_.num_records_per_page;
    char buf[16];

    // 填满一整页
    std::vector<Rid> rids;
    for (int i = 0; i < n_per_page; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        rids.push_back(fh->insert_record(buf, nullptr));
    }
    // 页面应该已满，空闲链表头应为 RM_NO_PAGE 或指向其他页
    ASSERT_EQ(fh->file_hdr_.first_free_page_no, RM_NO_PAGE);

    // 删除一条 → 页面变未满 → 应被链入空闲链表
    fh->delete_record(rids[0], nullptr);
    ASSERT_NE(fh->file_hdr_.first_free_page_no, RM_NO_PAGE);

    // 再次插入应该使用该空闲页
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 999;
    Rid new_rid = fh->insert_record(buf, nullptr);
    auto rec = fh->get_record(new_rid, nullptr);
    ASSERT_EQ(999, *(int *)rec->data);

    close_file(fh.get());
}

// ─── Test 7: RmScan 全表扫描 ────────────────────────────────

TEST_F(RmTestBase, ScanAllRecords) {
    auto fh = open_file("test_scan.txt", 8);
    char buf[8];
    std::set<int> inserted_vals;

    for (int i = 0; i < 50; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i * 10;
        fh->insert_record(buf, nullptr);
        inserted_vals.insert(i * 10);
    }

    // 扫描验证
    std::set<int> scanned_vals;
    for (RmScan scan(fh.get()); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), nullptr);
        scanned_vals.insert(*(int *)rec->data);
    }
    ASSERT_EQ(inserted_vals, scanned_vals);

    close_file(fh.get());
}

// ─── Test 8: 空表扫描 ───────────────────────────────────────

TEST_F(RmTestBase, ScanEmptyTable) {
    auto fh = open_file("test_empty_scan.txt", 8);
    RmScan scan(fh.get());
    ASSERT_TRUE(scan.is_end());
    close_file(fh.get());
}

// ─── Test 9: 大规模随机增删改 ──────────────────────────────

TEST_F(RmTestBase, RandomInsertDeleteUpdate) {
    srand(42);
    auto fh = open_file("test_random.txt", 32);
    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;
    char write_buf[32];

    for (int round = 0; round < 500; round++) {
        double insert_prob = 1. - mock.size() / 150.;
        double dice = rand() * 1. / RAND_MAX;
        if (mock.empty() || dice < insert_prob) {
            rand_buf(fh->file_hdr_.record_size, write_buf);
            Rid rid = fh->insert_record(write_buf, nullptr);
            mock[rid] = std::string(write_buf, fh->file_hdr_.record_size);
        } else {
            int rid_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int i = 0; i < rid_idx; i++) it++;
            auto rid = it->first;
            if (rand() % 2 == 0) {
                rand_buf(fh->file_hdr_.record_size, write_buf);
                fh->update_record(rid, write_buf, nullptr);
                mock[rid] = std::string(write_buf, fh->file_hdr_.record_size);
            } else {
                fh->delete_record(rid, nullptr);
                mock.erase(rid);
            }
        }
        // 每50轮重新打开文件验证持久化
        if (round % 50 == 0) {
            rm_manager_->close_file(fh.get());
            fh = rm_manager_->open_file("test_random.txt");
        }
        check_equal(fh.get(), mock);
    }

    close_file(fh.get());
}

// ─── Test 10: insert_record(Rid, buf) 指定位置插入 ──────────

TEST_F(RmTestBase, InsertAtSpecificRid) {
    auto fh = open_file("test_insert_rid.txt", 8);
    char buf[8];

    // 先插入一条让页面存在
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 1;
    Rid rid1 = fh->insert_record(buf, nullptr);

    // 在同一页面的另一个 slot 插入
    Rid target_rid{rid1.page_no, 2};
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 777;
    fh->insert_record(target_rid, buf);

    auto rec = fh->get_record(target_rid, nullptr);
    ASSERT_EQ(777, *(int *)rec->data);
    ASSERT_TRUE(fh->is_record(target_rid));

    close_file(fh.get());
}

// ═══════════════════════════════════════════════════════════════
//  B+ Tree Index 测试
// ═══════════════════════════════════════════════════════════════

class IxTestBase : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<IxManager> ix_manager_;
    int fd_ = -1;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), bpm_.get());

        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(TEST_DB_NAME));
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
    }

    void TearDown() override {
        if (chdir("..") < 0) {
            throw UnixError();
        }
    }

    // 创建 INT 类型单列索引
    std::unique_ptr<IxIndexHandle> open_index(const std::string &filename) {
        std::vector<ColMeta> cols;
        cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
        if (ix_manager_->exists(filename, cols)) {
            ix_manager_->destroy_index(filename, cols);
        }
        ix_manager_->create_index(filename, cols);
        return ix_manager_->open_index(filename, cols);
    }

    void close_index(IxIndexHandle *ih) { ix_manager_->close_index(ih); }
};

// ─── Test 11: B+树基本插入与点查 ───────────────────────────

TEST_F(IxTestBase, InsertAndGet) {
    auto ih = open_index("ix_basic");
    Transaction txn(0);

    // 插入 1..20
    for (int i = 1; i <= 20; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};  // mock rid
        ih->insert_entry(key, rid, &txn);
    }

    // 点查验证
    for (int i = 1; i <= 20; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        bool found = ih->get_value(key, &result, nullptr);
        ASSERT_TRUE(found);
        ASSERT_EQ(result.size(), 1u);
        ASSERT_EQ(result[0].page_no, 1);
        ASSERT_EQ(result[0].slot_no, i);
    }

    // 查不存在的 key
    char key[4];
    *(int *)key = 999;
    std::vector<Rid> result;
    ASSERT_FALSE(ih->get_value(key, &result, nullptr));

    close_index(ih.get());
}

// ─── Test 12: B+树递增序列插入（触发分裂）─────────────────

TEST_F(IxTestBase, InsertSequentialSplit) {
    auto ih = open_index("ix_seq_split");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 3;  // 足够触发多次分裂

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{i / 10, i % 10};
        ih->insert_entry(key, rid, &txn);
    }

    // 验证所有 key 可查
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Test 13: B+树逆序插入 ─────────────────────────────────

TEST_F(IxTestBase, InsertReverse) {
    auto ih = open_index("ix_reverse");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 2;

    for (int i = n; i >= 1; i--) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Test 14: B+树删除（触发合并/借用）────────────────────

TEST_F(IxTestBase, DeleteAndCoalesce) {
    auto ih = open_index("ix_delete");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 3;

    // 先插入
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 逐个删除
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);

        // 验证已删除的查不到
        std::vector<Rid> result;
        ASSERT_FALSE(ih->get_value(key, &result, nullptr));

        // 验证未删除的还在
        for (int j = i + 1; j <= std::min(i + 5, n); j++) {
            char k[4];
            *(int *)k = j;
            ASSERT_TRUE(ih->get_value(k, &result, nullptr));
        }
    }

    close_index(ih.get());
}

// ─── Test 15: B+树逆序删除 ─────────────────────────────────

TEST_F(IxTestBase, DeleteReverse) {
    auto ih = open_index("ix_del_reverse");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 2;

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    for (int i = n; i >= 1; i--) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
    }

    // 全删完后应全部查不到
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_FALSE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Test 16: B+树范围扫描 (IxScan) ────────────────────────

TEST_F(IxTestBase, RangeScan) {
    auto ih = open_index("ix_scan");
    Transaction txn(0);
    int n = 50;

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i * 2;  // 2,4,6,...,100
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 扫描 [10, 50] 范围
    char lo[4], hi[4];
    *(int *)lo = 10;
    *(int *)hi = 50;

    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);

    std::vector<int> scanned_keys;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        // 从 rid 反推 key（这里 rid.slot_no = i, key = i*2）
        scanned_keys.push_back(rid.slot_no * 2);
    }

    // 验证范围 [10,50) 内的偶数
    std::vector<int> expected;
    for (int k = 10; k < 50; k += 2) expected.push_back(k);
    ASSERT_EQ(scanned_keys, expected);

    close_index(ih.get());
}

// ─── Test 17: B+树随机插入 ──────────────────────────────────

TEST_F(IxTestBase, RandomInsert) {
    srand(123);
    auto ih = open_index("ix_random");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 4;
    std::set<int> inserted;

    // 随机插入不重复 key
    std::vector<int> keys;
    for (int i = 1; i <= n; i++) keys.push_back(i);
    std::shuffle(keys.begin(), keys.end(), std::mt19937(123));

    for (int k : keys) {
        char key[4];
        *(int *)key = k;
        Rid rid{1, k};
        ih->insert_entry(key, rid, &txn);
        inserted.insert(k);
    }

    // 全部验证
    for (int k : inserted) {
        char key[4];
        *(int *)key = k;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Test 18: B+树大规模删除导致根节点坍缩 ────────────────

TEST_F(IxTestBase, DeleteToRootCollapse) {
    auto ih = open_index("ix_collapse");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 3;

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 删除到只剩一个
    for (int i = 1; i < n; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
    }

    // 最后一条应该还在
    char key[4];
    *(int *)key = n;
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(key, &result, nullptr));

    // 删掉最后一条
    ih->delete_entry(key, &txn);
    ASSERT_FALSE(ih->get_value(key, &result, nullptr));

    close_index(ih.get());
}

// ─── Test 19: B+树插入重复 key ─────────────────────────────

TEST_F(IxTestBase, InsertDuplicateKey) {
    auto ih = open_index("ix_dup");
    Transaction txn(0);

    char key[4];
    *(int *)key = 42;

    // 插入相同 key 不同 rid
    Rid rid1{1, 0};
    Rid rid2{1, 1};
    ih->insert_entry(key, rid1, &txn);
    ih->insert_entry(key, rid2, &txn);

    // get_value 应返回两个 rid
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    ASSERT_EQ(result.size(), 2u);

    close_index(ih.get());
}

// ─── Test 20: Record + Index 联合测试 ───────────────────────

TEST_F(RmTestBase, RecordIndexIntegration) {
    // 1. 创建记录文件
    auto fh = open_file("test_integration.txt", 8);

    // 2. 创建索引
    IxManager ix_mgr(disk_manager_.get(), bpm_.get());
    std::vector<ColMeta> cols;
    cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
    std::string ix_name = ix_mgr.get_index_name("test_integration.txt", cols);
    if (ix_mgr.exists("test_integration.txt", cols)) {
        ix_mgr.destroy_index("test_integration.txt", cols);
    }
    ix_mgr.create_index("test_integration.txt", cols);
    auto ih = ix_mgr.open_index("test_integration.txt", cols);
    Transaction txn(0);

    char buf[8];
    std::unordered_map<int, Rid> key_to_rid;  // key -> record Rid

    // 3. 插入记录 + 索引
    for (int i = 1; i <= 30; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i * 100;
        Rid rid = fh->insert_record(buf, nullptr);

        char key[4];
        *(int *)key = i;
        ih->insert_entry(key, rid, &txn);
        key_to_rid[i] = rid;
    }

    // 4. 通过索引查找记录
    for (int i = 1; i <= 30; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
        ASSERT_EQ(result.size(), 1u);

        auto rec = fh->get_record(result[0], nullptr);
        ASSERT_EQ(i * 100, *(int *)rec->data);
    }

    // 5. 删除记录 + 索引
    for (int i = 1; i <= 10; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
        fh->delete_record(key_to_rid[i], nullptr);
    }

    // 6. 验证删除后状态
    for (int i = 1; i <= 10; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_FALSE(ih->get_value(key, &result, nullptr));
    }
    for (int i = 11; i <= 30; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    ix_mgr.close_index(ih.get());
    close_file(fh.get());
}
