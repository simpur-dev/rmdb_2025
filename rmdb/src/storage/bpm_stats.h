/* Buffer Pool Manager 性能统计
 * 用于优化前后对比：命中率、淘汰、I/O、锁竞争等指标
 * 默认编译即可启用；如需零开销可 #define BPM_STATS_DISABLED
 */
#pragma once

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdint>

struct BpmStats {
    // ─── 计数器 ───
    std::atomic<uint64_t> fetch_count{0};       // fetch_page 总调用次数
    std::atomic<uint64_t> hit_count{0};         // 命中（page_table_ 中已有）
    std::atomic<uint64_t> miss_count{0};        // 未命中，需要从磁盘加载
    std::atomic<uint64_t> evict_count{0};       // 淘汰次数（victim 选中）
    std::atomic<uint64_t> evict_dirty_count{0}; // 淘汰的脏页次数（需要回写）
    std::atomic<uint64_t> disk_read_count{0};   // disk_manager_->read_page 次数
    std::atomic<uint64_t> disk_write_count{0};  // disk_manager_->write_page 次数
    std::atomic<uint64_t> new_page_count{0};    // new_page 调用次数
    std::atomic<uint64_t> unpin_count{0};       // unpin_page 调用次数
    std::atomic<uint64_t> flush_count{0};       // flush_page 调用次数

    // ─── 累计耗时 (纳秒) ───
    std::atomic<uint64_t> fetch_total_ns{0};    // fetch_page 总耗时
    std::atomic<uint64_t> disk_read_ns{0};      // 磁盘读耗时
    std::atomic<uint64_t> disk_write_ns{0};     // 磁盘写耗时

    void reset() {
        fetch_count = hit_count = miss_count = 0;
        evict_count = evict_dirty_count = 0;
        disk_read_count = disk_write_count = 0;
        new_page_count = unpin_count = flush_count = 0;
        fetch_total_ns = disk_read_ns = disk_write_ns = 0;
    }

    double hit_ratio() const {
        uint64_t total = fetch_count.load();
        return total == 0 ? 0.0 : static_cast<double>(hit_count.load()) / total;
    }

    double avg_fetch_us() const {
        uint64_t total = fetch_count.load();
        return total == 0 ? 0.0 : static_cast<double>(fetch_total_ns.load()) / total / 1000.0;
    }

    double avg_disk_read_us() const {
        uint64_t total = disk_read_count.load();
        return total == 0 ? 0.0 : static_cast<double>(disk_read_ns.load()) / total / 1000.0;
    }

    double avg_disk_write_us() const {
        uint64_t total = disk_write_count.load();
        return total == 0 ? 0.0 : static_cast<double>(disk_write_ns.load()) / total / 1000.0;
    }

    void print(const char* label = "BPM Stats") const {
        printf("\n========== %s ==========\n", label);
        printf("fetch_page       : %lu\n", fetch_count.load());
        printf("  hit            : %lu  (%.2f%%)\n", hit_count.load(), hit_ratio() * 100);
        printf("  miss           : %lu\n", miss_count.load());
        printf("evict            : %lu  (dirty: %lu)\n", evict_count.load(), evict_dirty_count.load());
        printf("disk_read        : %lu  (avg %.2f us)\n", disk_read_count.load(), avg_disk_read_us());
        printf("disk_write       : %lu  (avg %.2f us)\n", disk_write_count.load(), avg_disk_write_us());
        printf("new_page         : %lu\n", new_page_count.load());
        printf("unpin_page       : %lu\n", unpin_count.load());
        printf("flush_page       : %lu\n", flush_count.load());
        printf("avg fetch_page   : %.2f us\n", avg_fetch_us());
        printf("====================================\n\n");
    }
};

// 计时辅助宏
#ifndef BPM_STATS_DISABLED
#define BPM_TIMER_START auto _bpm_t0 = std::chrono::steady_clock::now()
#define BPM_TIMER_NS    (std::chrono::duration_cast<std::chrono::nanoseconds>( \
                          std::chrono::steady_clock::now() - _bpm_t0).count())
#else
#define BPM_TIMER_START ((void)0)
#define BPM_TIMER_NS    (0)
#endif
