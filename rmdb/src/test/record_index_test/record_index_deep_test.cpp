/* Record Manager & B+ Tree Index 深度单元测试 */

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

const std::string DEEP_TEST_DB_NAME = "DeepTest_db";
constexpr size_t DEEP_TEST_BUFFER_POOL_SIZE = 256;

// ─── 辅助工具 ────────────────────────────────────────────────

struct rid_hash_t {
    size_t operator()(const Rid &rid) const { return (rid.page_no << 16) | rid.slot_no; }
};

struct rid_equal_t {
    bool operator()(const Rid &x, const Rid &y) const { return x.page_no == y.page_no && x.slot_no == y.slot_no; }
};

void check_equal(const RmFileHandle *file_handle,
                 const std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> &mock) {
    for (auto &entry : mock) {
        Rid rid = entry.first;
        auto mock_buf = (char *)entry.second.c_str();
        auto rec = file_handle->get_record(rid, nullptr);
        ASSERT_EQ(0, memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size));
    }
    size_t num_records = 0;
    for (RmScan scan(file_handle); !scan.is_end(); scan.next()) {
        ASSERT_TRUE(mock.count(scan.rid()) > 0);
        auto rec = file_handle->get_record(scan.rid(), nullptr);
        ASSERT_EQ(0, memcmp(rec->data, mock.at(scan.rid()).c_str(), file_handle->file_hdr_.record_size));
        num_records++;
    }
    ASSERT_EQ(num_records, mock.size());
}

// ═══════════════════════════════════════════════════════════════
//  Record Manager 深度测试
// ═══════════════════════════════════════════════════════════════

class RmDeepTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(DEEP_TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());

        if (!disk_manager_->is_dir(DEEP_TEST_DB_NAME)) {
            disk_manager_->create_dir(DEEP_TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(DEEP_TEST_DB_NAME));
        if (chdir(DEEP_TEST_DB_NAME.c_str()) < 0) {
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

// ─── Rm-1: 多页压力插入（5页以上）──────────────────────────

TEST_F(RmDeepTest, MultiPageInsert) {
    int record_size = 8;
    auto fh = open_file("rm_multipage.txt", record_size);
    int n_per_page = fh->file_hdr_.num_records_per_page;
    int total = n_per_page * 5 + 3;  // 5页多3条

    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;
    char buf[8];
    for (int i = 0; i < total; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        mock[rid] = std::string(buf, 8);
    }

    ASSERT_EQ(mock.size(), (size_t)total);
    check_equal(fh.get(), mock);
    close_file(fh.get());
}

// ─── Rm-2: 删除全部再重插（空闲页回收验证）──────────────────

TEST_F(RmDeepTest, DeleteAllAndReinsert) {
    int record_size = 16;
    auto fh = open_file("rm_del_all.txt", record_size);
    int n_per_page = fh->file_hdr_.num_records_per_page;

    // 填满2页
    std::vector<Rid> rids;
    char buf[16];
    for (int i = 0; i < n_per_page * 2; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        rids.push_back(fh->insert_record(buf, nullptr));
    }

    // 全部删除
    for (auto &rid : rids) {
        fh->delete_record(rid, nullptr);
    }

    // 重新插入，应重用空闲页
    std::vector<Rid> rids2;
    for (int i = 0; i < n_per_page * 2; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i + 1000;
        rids2.push_back(fh->insert_record(buf, nullptr));
    }

    // 验证新数据
    for (int i = 0; i < n_per_page * 2; i++) {
        auto rec = fh->get_record(rids2[i], nullptr);
        ASSERT_EQ(i + 1000, *(int *)rec->data);
    }

    close_file(fh.get());
}

// ─── Rm-3: 交替插入删除（空闲链表反复回收）──────────────────

TEST_F(RmDeepTest, InterleavedInsertDelete) {
    auto fh = open_file("rm_interleaved.txt", 8);
    char buf[8];
    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    for (int round = 0; round < 200; round++) {
        // 插入5条
        for (int j = 0; j < 5; j++) {
            memset(buf, 0, sizeof(buf));
            *(int *)buf = round * 5 + j;
            Rid rid = fh->insert_record(buf, nullptr);
            mock[rid] = std::string(buf, 8);
        }
        // 随机删除2条
        int del_cnt = 0;
        for (auto it = mock.begin(); it != mock.end() && del_cnt < 2;) {
            Rid rid = it->first;
            ++it;
            fh->delete_record(rid, nullptr);
            mock.erase(rid);
            del_cnt++;
        }
    }

    check_equal(fh.get(), mock);
    close_file(fh.get());
}

// ─── Rm-4: 更新后扫描一致性 ────────────────────────────────

TEST_F(RmDeepTest, UpdateAndScanConsistency) {
    auto fh = open_file("rm_update_scan.txt", 8);
    char buf[8];
    std::set<int> expected_vals;

    for (int i = 0; i < 100; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        fh->insert_record(buf, nullptr);
        expected_vals.insert(i);
    }

    // 更新前50条的值
    std::set<int> updated_vals;
    int idx = 0;
    for (RmScan scan(fh.get()); !scan.is_end(); scan.next()) {
        if (idx >= 50) break;
        Rid rid = scan.rid();
        memset(buf, 0xFF, sizeof(buf));
        *(int *)buf = idx + 1000;
        fh->update_record(rid, buf, nullptr);
        updated_vals.insert(idx + 1000);
        expected_vals.erase(idx);
        expected_vals.insert(idx + 1000);
        idx++;
    }

    // 扫描验证
    std::set<int> scanned_vals;
    for (RmScan scan(fh.get()); !scan.is_end(); scan.next()) {
        auto rec = fh->get_record(scan.rid(), nullptr);
        scanned_vals.insert(*(int *)rec->data);
    }
    ASSERT_EQ(scanned_vals, expected_vals);

    close_file(fh.get());
}

// ─── Rm-5: 指定 Rid 插入后删除再重用 ──────────────────────

TEST_F(RmDeepTest, InsertAtRidReuse) {
    auto fh = open_file("rm_rid_reuse.txt", 8);
    char buf[8];

    // 先填一些记录让页面存在
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 1;
    Rid rid1 = fh->insert_record(buf, nullptr);

    // 在指定 slot 插入
    Rid target{rid1.page_no, 3};
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 777;
    fh->insert_record(target, buf);
    ASSERT_TRUE(fh->is_record(target));

    // 删除指定 slot
    fh->delete_record(target, nullptr);
    ASSERT_FALSE(fh->is_record(target));

    // 重用同一 slot
    memset(buf, 0, sizeof(buf));
    *(int *)buf = 888;
    fh->insert_record(target, buf);
    auto rec = fh->get_record(target, nullptr);
    ASSERT_EQ(888, *(int *)rec->data);

    close_file(fh.get());
}

// ─── Rm-6: 大记录尺寸（接近页面极限）────────────────────────

TEST_F(RmDeepTest, LargeRecordSize) {
    // 选取一个较大的 record_size，使每页只能放少量记录
    int record_size = 256;
    auto fh = open_file("rm_large_rec.txt", record_size);
    int n_per_page = fh->file_hdr_.num_records_per_page;
    ASSERT_GE(n_per_page, 1);

    char buf[256];
    std::vector<Rid> rids;
    for (int i = 0; i < n_per_page * 3; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        rids.push_back(fh->insert_record(buf, nullptr));
    }

    for (int i = 0; i < n_per_page * 3; i++) {
        auto rec = fh->get_record(rids[i], nullptr);
        ASSERT_EQ(i, *(int *)rec->data);
    }

    close_file(fh.get());
}

// ─── Rm-7: 全删后空表扫描 ──────────────────────────────────

TEST_F(RmDeepTest, ScanAfterDeleteAll) {
    auto fh = open_file("rm_scan_empty.txt", 8);
    char buf[8];
    std::vector<Rid> rids;

    for (int i = 0; i < 30; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        rids.push_back(fh->insert_record(buf, nullptr));
    }

    for (auto &rid : rids) {
        fh->delete_record(rid, nullptr);
    }

    RmScan scan(fh.get());
    ASSERT_TRUE(scan.is_end());

    close_file(fh.get());
}

// ─── Rm-8: 持久化验证（关闭重开后数据完整）─────────────────

TEST_F(RmDeepTest, PersistenceAfterReopen) {
    auto fh = open_file("rm_persist.txt", 8);
    char buf[8];
    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    for (int i = 0; i < 50; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        mock[rid] = std::string(buf, 8);
    }

    // 关闭并重新打开
    rm_manager_->close_file(fh.get());
    fh = rm_manager_->open_file("rm_persist.txt");

    check_equal(fh.get(), mock);
    close_file(fh.get());
}

// ═══════════════════════════════════════════════════════════════
//  B+ Tree Index 深度测试
// ═══════════════════════════════════════════════════════════════

class IxDeepTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<IxManager> ix_manager_;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(DEEP_TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        ix_manager_ = std::make_unique<IxManager>(disk_manager_.get(), bpm_.get());

        if (!disk_manager_->is_dir(DEEP_TEST_DB_NAME)) {
            disk_manager_->create_dir(DEEP_TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(DEEP_TEST_DB_NAME));
        if (chdir(DEEP_TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
    }

    void TearDown() override {
        if (chdir("..") < 0) {
            throw UnixError();
        }
    }

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

// ─── Ix-1: 边界值 key（0, 负数, INT_MAX）──────────────────

TEST_F(IxDeepTest, BoundaryKeys) {
    auto ih = open_index("ix_boundary");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;

    int keys[] = {0, -1, -2147483647, 2147483647, 1, -100, 100};
    int n = sizeof(keys) / sizeof(keys[0]);

    for (int i = 0; i < n; i++) {
        char key[4];
        *(int *)key = keys[i];
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    for (int i = 0; i < n; i++) {
        char key[4];
        *(int *)key = keys[i];
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
    }

    // 查不存在的边界
    char key[4];
    *(int *)key = -2147483648;  // INT_MIN
    std::vector<Rid> result;
    ASSERT_FALSE(ih->get_value(key, &result, nullptr));

    close_index(ih.get());
}

// ─── Ix-2: 大规模顺序插入（3层以上 B+ 树）─────────────────

TEST_F(IxDeepTest, LargeSequentialInsert) {
    auto ih = open_index("ix_large_seq");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * ih->file_hdr_->btree_order_ + 10;  // 足以产生3层

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{i / 100, i % 100};
        ih->insert_entry(key, rid, &txn);
    }

    // 全量验证
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Ix-3: 交替插入删除（反复分裂与合并）───────────────────

TEST_F(IxDeepTest, InterleavedInsertDelete) {
    auto ih = open_index("ix_interleaved");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;
    std::set<int> alive;

    // 初始插入
    int n = order * 3;
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
        alive.insert(i);
    }

    // 交替：每次删一批再插一批
    srand(777);
    for (int round = 0; round < 5; round++) {
        // 删一半
        std::vector<int> to_del(alive.begin(), alive.end());
        std::shuffle(to_del.begin(), to_del.end(), std::mt19937(777 + round));
        int del_n = to_del.size() / 2;
        for (int i = 0; i < del_n; i++) {
            char key[4];
            *(int *)key = to_del[i];
            ih->delete_entry(key, &txn);
            alive.erase(to_del[i]);
        }

        // 插一批新的
        for (int i = n + round * 50 + 1; i <= n + (round + 1) * 50; i++) {
            char key[4];
            *(int *)key = i;
            Rid rid{1, i};
            ih->insert_entry(key, rid, &txn);
            alive.insert(i);
        }

        // 验证所有存活 key
        for (int k : alive) {
            char key[4];
            *(int *)key = k;
            std::vector<Rid> result;
            ASSERT_TRUE(ih->get_value(key, &result, nullptr)) << "key=" << k << " round=" << round;
        }
    }

    close_index(ih.get());
}

// ─── Ix-4: 删除后重插同一 key（验证删除后空位可复用）─────────

TEST_F(IxDeepTest, DeleteAndReinsertSameKey) {
    auto ih = open_index("ix_del_reinsert");
    Transaction txn(0);

    for (int i = 1; i <= 20; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 删除 key=10
    char key10[4];
    *(int *)key10 = 10;
    ih->delete_entry(key10, &txn);
    std::vector<Rid> result;
    ASSERT_FALSE(ih->get_value(key10, &result, nullptr));

    // 重插 key=10 不同 rid
    Rid new_rid{2, 99};
    ih->insert_entry(key10, new_rid, &txn);
    ASSERT_TRUE(ih->get_value(key10, &result, nullptr));
    ASSERT_EQ(result.size(), 1u);
    ASSERT_EQ(result[0].page_no, 2);
    ASSERT_EQ(result[0].slot_no, 99);

    close_index(ih.get());
}

// ─── Ix-5: 多个重复 key（跨页分裂场景）─────────────────────

TEST_F(IxDeepTest, ManyDuplicateKeys) {
    auto ih = open_index("ix_many_dup");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;

    // 对 key=42 插入 order+5 个不同 rid，足以触发跨页分裂
    char key[4];
    *(int *)key = 42;
    for (int i = 0; i < order + 5; i++) {
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    ASSERT_EQ(result.size(), (size_t)(order + 5));

    // 再插入其他 key 确认树结构完整
    for (int k = 1; k <= 10; k++) {
        char kbuf[4];
        *(int *)kbuf = k;
        Rid rid{2, k};
        ih->insert_entry(kbuf, rid, &txn);
    }
    for (int k = 1; k <= 10; k++) {
        char kbuf[4];
        *(int *)kbuf = k;
        std::vector<Rid> r;
        ASSERT_TRUE(ih->get_value(kbuf, &r, nullptr));
    }

    close_index(ih.get());
}

// ─── Ix-6: 删除重复 key 中的部分 rid ──────────────────────

TEST_F(IxDeepTest, DeletePartialDuplicate) {
    auto ih = open_index("ix_del_dup");
    Transaction txn(0);

    char key[4];
    *(int *)key = 100;

    // 插入5个相同 key
    Rid rids[5] = {{1, 0}, {1, 1}, {1, 2}, {1, 3}, {1, 4}};
    for (int i = 0; i < 5; i++) {
        ih->insert_entry(key, rids[i], &txn);
    }

    // 验证5个
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    ASSERT_EQ(result.size(), 5u);

    // 删除其中3个（delete_entry 按 key 删除一个条目）
    for (int i = 0; i < 3; i++) {
        ih->delete_entry(key, &txn);
    }

    // 应剩2个
    result.clear();
    ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    ASSERT_EQ(result.size(), 2u);

    close_index(ih.get());
}

// ─── Ix-7: 范围扫描边界（单元素范围、空范围、全范围）────────

TEST_F(IxDeepTest, RangeScanBoundary) {
    auto ih = open_index("ix_scan_boundary");
    Transaction txn(0);

    for (int i = 1; i <= 100; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 1) 单元素范围 [5,6)
    {
        char lo[4], hi[4];
        *(int *)lo = 5;
        *(int *)hi = 6;
        Iid lower = ih->lower_bound(lo);
        Iid upper = ih->lower_bound(hi);
        int cnt = 0;
        for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
            cnt++;
        }
        ASSERT_EQ(cnt, 1);
    }

    // 2) 空范围 [50,50)
    {
        char lo[4], hi[4];
        *(int *)lo = 50;
        *(int *)hi = 50;
        Iid lower = ih->lower_bound(lo);
        Iid upper = ih->lower_bound(hi);
        int cnt = 0;
        for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
            cnt++;
        }
        ASSERT_EQ(cnt, 0);
    }

    // 3) 全范围 [1,101)
    {
        char lo[4], hi[4];
        *(int *)lo = 1;
        *(int *)hi = 101;
        Iid lower = ih->lower_bound(lo);
        Iid upper = ih->lower_bound(hi);
        int cnt = 0;
        for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
            cnt++;
        }
        ASSERT_EQ(cnt, 100);
    }

    // 4) 超出范围 [200,300)
    {
        char lo[4], hi[4];
        *(int *)lo = 200;
        *(int *)hi = 300;
        Iid lower = ih->lower_bound(lo);
        Iid upper = ih->lower_bound(hi);
        int cnt = 0;
        for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
            cnt++;
        }
        ASSERT_EQ(cnt, 0);
    }

    close_index(ih.get());
}

// ─── Ix-8: lower_bound / upper_bound 正确性 ────────────────

TEST_F(IxDeepTest, LowerUpperBoundCorrectness) {
    auto ih = open_index("ix_bound");
    Transaction txn(0);

    // 插入偶数 2,4,6,...,20
    for (int i = 1; i <= 10; i++) {
        char key[4];
        *(int *)key = i * 2;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // lower_bound(7) → 指向 8
    {
        char k[4];
        *(int *)k = 7;
        Iid iid = ih->lower_bound(k);
        Rid rid = ih->get_rid(iid);
        // rid.slot_no = i, key = i*2, 所以 key=8 → slot_no=4
        ASSERT_EQ(rid.slot_no, 4);
    }

    // lower_bound(8) → 指向 8
    {
        char k[4];
        *(int *)k = 8;
        Iid iid = ih->lower_bound(k);
        Rid rid = ih->get_rid(iid);
        ASSERT_EQ(rid.slot_no, 4);
    }

    // upper_bound(8) → 指向 10
    {
        char k[4];
        *(int *)k = 8;
        Iid iid = ih->upper_bound(k);
        Rid rid = ih->get_rid(iid);
        ASSERT_EQ(rid.slot_no, 5);
    }

    // lower_bound(1) → 指向 2
    {
        char k[4];
        *(int *)k = 1;
        Iid iid = ih->lower_bound(k);
        Rid rid = ih->get_rid(iid);
        ASSERT_EQ(rid.slot_no, 1);
    }

    close_index(ih.get());
}

// ─── Ix-9: 空索引点查与扫描 ────────────────────────────────

TEST_F(IxDeepTest, EmptyIndexOperations) {
    auto ih = open_index("ix_empty_ops");

    // 空索引点查
    char key[4];
    *(int *)key = 1;
    std::vector<Rid> result;
    ASSERT_FALSE(ih->get_value(key, &result, nullptr));

    // 空索引范围扫描
    char lo[4], hi[4];
    *(int *)lo = 1;
    *(int *)hi = 100;
    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);
    int cnt = 0;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        cnt++;
    }
    ASSERT_EQ(cnt, 0);

    close_index(ih.get());
}

// ─── Ix-10: 全删后重插（根坍缩后重建）─────────────────────

TEST_F(IxDeepTest, FullDeleteAndRebuild) {
    auto ih = open_index("ix_rebuild");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 2;

    // 插入
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 全删
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
    }

    // 确认空
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_FALSE(ih->get_value(key, &result, nullptr));
    }

    // 重插
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i + 1000;
        Rid rid{2, i};
        ih->insert_entry(key, rid, &txn);
    }

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i + 1000;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ─── Ix-11: 随机大规模删除（验证 redistribute 路径）────────

TEST_F(IxDeepTest, RandomLargeDelete) {
    srand(456);
    auto ih = open_index("ix_rand_del");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 5;

    // 顺序插入
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 随机删除 70%
    std::vector<int> keys;
    for (int i = 1; i <= n; i++) keys.push_back(i);
    std::shuffle(keys.begin(), keys.end(), std::mt19937(456));
    int del_n = n * 7 / 10;
    std::set<int> deleted;
    for (int i = 0; i < del_n; i++) {
        char key[4];
        *(int *)key = keys[i];
        ih->delete_entry(key, &txn);
        deleted.insert(keys[i]);
    }

    // 验证
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        bool found = ih->get_value(key, &result, nullptr);
        if (deleted.count(i)) {
            ASSERT_FALSE(found) << "key=" << i << " should be deleted";
        } else {
            ASSERT_TRUE(found) << "key=" << i << " should exist";
        }
    }

    close_index(ih.get());
}

// ─── Ix-12: 逆序插入后顺序扫描 ─────────────────────────────

TEST_F(IxDeepTest, ReverseInsertSequentialScan) {
    auto ih = open_index("ix_rev_scan");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 3;

    // 逆序插入
    for (int i = n; i >= 1; i--) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 顺序扫描应有序
    char lo[4], hi[4];
    *(int *)lo = 1;
    *(int *)hi = n + 1;
    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);

    int prev_key = 0;
    int cnt = 0;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        int cur_key = rid.slot_no;  // rid.slot_no == i == key
        ASSERT_GT(cur_key, prev_key) << "scan not in order at cnt=" << cnt;
        prev_key = cur_key;
        cnt++;
    }
    ASSERT_EQ(cnt, n);

    close_index(ih.get());
}

// ─── Ix-13: 重复 key 跨页分裂后范围扫描 ────────────────────

TEST_F(IxDeepTest, DuplicateKeyRangeScan) {
    auto ih = open_index("ix_dup_scan");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;

    // 插入 key=42 的 order+5 个重复
    char key42[4];
    *(int *)key42 = 42;
    for (int i = 0; i < order + 5; i++) {
        Rid rid{1, i};
        ih->insert_entry(key42, rid, &txn);
    }

    // 插入其他 key
    for (int k = 1; k <= 10; k++) {
        char kbuf[4];
        *(int *)kbuf = k;
        Rid rid{2, k};
        ih->insert_entry(kbuf, rid, &txn);
    }

    // 范围扫描 [1, 100)
    char lo[4], hi[4];
    *(int *)lo = 1;
    *(int *)hi = 100;
    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);

    int cnt = 0;
    int dup42_cnt = 0;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        cnt++;
        Rid rid = scan.rid();
        if (rid.page_no == 1) dup42_cnt++;
    }
    ASSERT_EQ(cnt, order + 5 + 10);
    ASSERT_EQ(dup42_cnt, order + 5);

    close_index(ih.get());
}

// ─── Ix-14: 删除到只剩根节点再插入 ─────────────────────────

TEST_F(IxDeepTest, DeleteToRootThenInsert) {
    auto ih = open_index("ix_root_ins");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;

    // 插入足够多以产生多层
    int n = order * 4;
    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 删到只剩1条
    for (int i = 1; i < n; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
    }

    // 验证最后一条
    char last_key[4];
    *(int *)last_key = n;
    std::vector<Rid> result;
    ASSERT_TRUE(ih->get_value(last_key, &result, nullptr));

    // 再插入一批，触发新的分裂
    for (int i = n + 1; i <= n + order * 2; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 全量验证
    for (int i = n; i <= n + order * 2; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> r;
        ASSERT_TRUE(ih->get_value(key, &r, nullptr)) << "key=" << i;
    }

    close_index(ih.get());
}

// ─── Ix-15: 随机插入 + 随机删除 + 范围扫描一致性 ──────────

TEST_F(IxDeepTest, RandomInsertDeleteWithScan) {
    srand(999);
    auto ih = open_index("ix_rand_scan");
    Transaction txn(0);
    int order = ih->file_hdr_->btree_order_;
    std::set<int> alive;

    // 随机插入 200 个
    std::vector<int> pool;
    for (int i = 1; i <= order * 5; i++) pool.push_back(i);
    std::shuffle(pool.begin(), pool.end(), std::mt19937(999));
    for (int i = 0; i < 200 && i < (int)pool.size(); i++) {
        char key[4];
        *(int *)key = pool[i];
        Rid rid{1, pool[i]};
        ih->insert_entry(key, rid, &txn);
        alive.insert(pool[i]);
    }

    // 随机删除 80 个
    std::vector<int> alive_vec(alive.begin(), alive.end());
    std::shuffle(alive_vec.begin(), alive_vec.end(), std::mt19937(1000));
    for (int i = 0; i < 80 && i < (int)alive_vec.size(); i++) {
        char key[4];
        *(int *)key = alive_vec[i];
        ih->delete_entry(key, &txn);
        alive.erase(alive_vec[i]);
    }

    // 范围扫描验证有序性
    char lo[4], hi[4];
    *(int *)lo = *alive.begin();
    *(int *)hi = *alive.rbegin() + 1;
    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);

    int prev_key = -1;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        int cur_key = rid.slot_no;
        ASSERT_GT(cur_key, prev_key);
        prev_key = cur_key;
    }

    close_index(ih.get());
}

// ─── Ix-16: 持久化验证（关闭重开索引后数据完整）────────────

TEST_F(IxDeepTest, IndexPersistence) {
    auto ih = open_index("ix_persist");
    Transaction txn(0);
    int n = ih->file_hdr_->btree_order_ * 2;

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        Rid rid{1, i};
        ih->insert_entry(key, rid, &txn);
    }

    // 关闭并重新打开
    ix_manager_->close_index(ih.get());
    std::vector<ColMeta> cols;
    cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
    ih = ix_manager_->open_index("ix_persist", cols);

    for (int i = 1; i <= n; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
    }

    close_index(ih.get());
}

// ═══════════════════════════════════════════════════════════════
//  Record + Index 联合深度测试
// ═══════════════════════════════════════════════════════════════

class IntegrationDeepTest : public ::testing::Test {
   protected:
    std::unique_ptr<DiskManager> disk_manager_;
    std::unique_ptr<BufferPoolManager> bpm_;
    std::unique_ptr<RmManager> rm_manager_;

    void SetUp() override {
        disk_manager_ = std::make_unique<DiskManager>();
        bpm_ = std::make_unique<BufferPoolManager>(DEEP_TEST_BUFFER_POOL_SIZE, disk_manager_.get());
        rm_manager_ = std::make_unique<RmManager>(disk_manager_.get(), bpm_.get());

        if (!disk_manager_->is_dir(DEEP_TEST_DB_NAME)) {
            disk_manager_->create_dir(DEEP_TEST_DB_NAME);
        }
        ASSERT_TRUE(disk_manager_->is_dir(DEEP_TEST_DB_NAME));
        if (chdir(DEEP_TEST_DB_NAME.c_str()) < 0) {
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

// ─── Int-1: 大规模联合增删 ─────────────────────────────────

TEST_F(IntegrationDeepTest, LargeScaleIntegration) {
    auto fh = open_file("int_large.txt", 8);
    IxManager ix_mgr(disk_manager_.get(), bpm_.get());
    std::vector<ColMeta> cols;
    cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
    if (ix_mgr.exists("int_large.txt", cols)) ix_mgr.destroy_index("int_large.txt", cols);
    ix_mgr.create_index("int_large.txt", cols);
    auto ih = ix_mgr.open_index("int_large.txt", cols);
    Transaction txn(0);

    char buf[8];
    std::unordered_map<int, Rid> key_to_rid;

    // 插入 200 条
    for (int i = 1; i <= 200; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i * 50;
        Rid rid = fh->insert_record(buf, nullptr);

        char key[4];
        *(int *)key = i;
        ih->insert_entry(key, rid, &txn);
        key_to_rid[i] = rid;
    }

    // 通过索引查找记录
    for (int i = 1; i <= 200; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
        ASSERT_EQ(result.size(), 1u);
        auto rec = fh->get_record(result[0], nullptr);
        ASSERT_EQ(i * 50, *(int *)rec->data);
    }

    // 删除前 100 条
    for (int i = 1; i <= 100; i++) {
        char key[4];
        *(int *)key = i;
        ih->delete_entry(key, &txn);
        fh->delete_record(key_to_rid[i], nullptr);
        key_to_rid.erase(i);
    }

    // 验证删除后状态
    for (int i = 1; i <= 100; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_FALSE(ih->get_value(key, &result, nullptr));
    }
    for (int i = 101; i <= 200; i++) {
        char key[4];
        *(int *)key = i;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
        auto rec = fh->get_record(result[0], nullptr);
        ASSERT_EQ(i * 50, *(int *)rec->data);
    }

    ix_mgr.close_index(ih.get());
    close_file(fh.get());
}

// ─── Int-2: 索引范围扫描 + 记录验证 ────────────────────────

TEST_F(IntegrationDeepTest, RangeScanWithRecordValidation) {
    auto fh = open_file("int_scan.txt", 8);
    IxManager ix_mgr(disk_manager_.get(), bpm_.get());
    std::vector<ColMeta> cols;
    cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
    if (ix_mgr.exists("int_scan.txt", cols)) ix_mgr.destroy_index("int_scan.txt", cols);
    ix_mgr.create_index("int_scan.txt", cols);
    auto ih = ix_mgr.open_index("int_scan.txt", cols);
    Transaction txn(0);

    char buf[8];
    std::unordered_map<int, Rid> key_to_rid;

    for (int i = 1; i <= 100; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i * 100;
        Rid rid = fh->insert_record(buf, nullptr);

        char key[4];
        *(int *)key = i;
        ih->insert_entry(key, rid, &txn);
        key_to_rid[i] = rid;
    }

    // 扫描 [20, 80) 的索引，验证对应记录
    char lo[4], hi[4];
    *(int *)lo = 20;
    *(int *)hi = 80;
    Iid lower = ih->lower_bound(lo);
    Iid upper = ih->lower_bound(hi);

    std::set<int> scanned_keys;
    for (IxScan scan(ih.get(), lower, upper, bpm_.get()); !scan.is_end(); scan.next()) {
        Rid rid = scan.rid();
        auto rec = fh->get_record(rid, nullptr);
        int val = *(int *)rec->data;
        int key = val / 100;
        ASSERT_GE(key, 20);
        ASSERT_LT(key, 80);
        scanned_keys.insert(key);
    }

    // 验证覆盖了 [20,80) 所有的 key
    ASSERT_EQ(scanned_keys.size(), 60u);
    for (int k = 20; k < 80; k++) {
        ASSERT_TRUE(scanned_keys.count(k) > 0) << "key=" << k << " missing from scan";
    }

    ix_mgr.close_index(ih.get());
    close_file(fh.get());
}

// ─── Int-3: 删全后重建索引 ─────────────────────────────────

TEST_F(IntegrationDeepTest, DeleteAllAndRebuildIndex) {
    auto fh = open_file("int_rebuild.txt", 8);
    IxManager ix_mgr(disk_manager_.get(), bpm_.get());
    std::vector<ColMeta> cols;
    cols.push_back(ColMeta{.tab_name = "", .name = "id", .type = TYPE_INT, .len = 4, .offset = 0, .index = false});
    if (ix_mgr.exists("int_rebuild.txt", cols)) ix_mgr.destroy_index("int_rebuild.txt", cols);
    ix_mgr.create_index("int_rebuild.txt", cols);
    auto ih = ix_mgr.open_index("int_rebuild.txt", cols);
    Transaction txn(0);

    char buf[8];
    std::vector<std::pair<int, Rid>> entries;

    // 插入
    for (int i = 1; i <= 50; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        char key[4];
        *(int *)key = i;
        ih->insert_entry(key, rid, &txn);
        entries.push_back({i, rid});
    }

    // 全删索引和记录
    for (auto &[k, rid] : entries) {
        char key[4];
        *(int *)key = k;
        ih->delete_entry(key, &txn);
        fh->delete_record(rid, nullptr);
    }

    // 重建
    entries.clear();
    for (int i = 100; i <= 180; i++) {
        memset(buf, 0, sizeof(buf));
        *(int *)buf = i;
        Rid rid = fh->insert_record(buf, nullptr);
        char key[4];
        *(int *)key = i;
        ih->insert_entry(key, rid, &txn);
        entries.push_back({i, rid});
    }

    // 验证
    for (auto &[k, rid] : entries) {
        char key[4];
        *(int *)key = k;
        std::vector<Rid> result;
        ASSERT_TRUE(ih->get_value(key, &result, nullptr));
        auto rec = fh->get_record(result[0], nullptr);
        ASSERT_EQ(k, *(int *)rec->data);
    }

    ix_mgr.close_index(ih.get());
    close_file(fh.get());
}
