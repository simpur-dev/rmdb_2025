/**
 * Buffer Pool Benchmark
 *
 * 用法:
 *   cmake --build build --target bpm_bench -j$(nproc)
 *   ./build/bin/bpm_bench
 *
 * 测试场景:
 *   1. 顺序写入   — 衡量 new_page + unpin 吞吐
 *   2. 顺序读取   — 衡量 fetch_page 命中率与延迟
 *   3. 随机读取   — 衡量缓存淘汰策略效果
 *   4. 热点读取   — 80/20 zipf，衡量热页命中率
 *   5. 脏页淘汰   — 衡量 evict 脏页时的 I/O 代价
 *   6. 多线程并发  — 衡量锁竞争与并发吞吐
 */

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <functional>
#include <random>
#include <thread>
#include <vector>

#include "storage/buffer_pool_manager.h"
#include "storage/disk_manager.h"

// ─── 辅助工具 ───

static const std::string BENCH_DB = "bpm_bench_db";
static const std::string BENCH_FILE = "bench.dat";

struct StatsSnapshot {
    uint64_t fetch_count, hit_count, miss_count;
    uint64_t evict_count, evict_dirty_count;
    uint64_t disk_read_count, disk_write_count;
    uint64_t new_page_count, unpin_count, flush_count;
    uint64_t fetch_total_ns, disk_read_ns, disk_write_ns;

    double hit_ratio() const { return fetch_count == 0 ? 0.0 : (double)hit_count / fetch_count; }
    double avg_fetch_us() const { return fetch_count == 0 ? 0.0 : (double)fetch_total_ns / fetch_count / 1000.0; }
    double avg_disk_read_us() const { return disk_read_count == 0 ? 0.0 : (double)disk_read_ns / disk_read_count / 1000.0; }
    double avg_disk_write_us() const { return disk_write_count == 0 ? 0.0 : (double)disk_write_ns / disk_write_count / 1000.0; }

    static StatsSnapshot from(const BpmStats& s) {
        return {s.fetch_count, s.hit_count, s.miss_count,
                s.evict_count, s.evict_dirty_count,
                s.disk_read_count, s.disk_write_count,
                s.new_page_count, s.unpin_count, s.flush_count,
                s.fetch_total_ns, s.disk_read_ns, s.disk_write_ns};
    }
};

struct BenchResult {
    const char* name;
    double elapsed_ms;
    uint64_t ops;
    StatsSnapshot stats;
};

static void print_result(const BenchResult& r) {
    double tput = r.ops / (r.elapsed_ms / 1000.0);
    printf("\n─── %s ───\n", r.name);
    printf("  ops: %lu   time: %.1f ms   throughput: %.0f ops/s\n", r.ops, r.elapsed_ms, tput);
    printf("  fetch: %lu  hit: %lu (%.2f%%)  miss: %lu\n",
           r.stats.fetch_count, r.stats.hit_count, r.stats.hit_ratio()*100, r.stats.miss_count);
    printf("  evict: %lu (dirty: %lu)\n", r.stats.evict_count, r.stats.evict_dirty_count);
    printf("  disk_read: %lu (avg %.2f us)  disk_write: %lu (avg %.2f us)\n",
           r.stats.disk_read_count, r.stats.avg_disk_read_us(),
           r.stats.disk_write_count, r.stats.avg_disk_write_us());
    printf("  avg fetch_page: %.2f us\n", r.stats.avg_fetch_us());
}

class BenchEnv {
public:
    std::unique_ptr<DiskManager> dm;
    int fd = -1;

    void setup() {
        dm = std::make_unique<DiskManager>();
        if (!dm->is_dir(BENCH_DB)) dm->create_dir(BENCH_DB);
        if (chdir(BENCH_DB.c_str()) < 0) throw std::runtime_error("chdir failed");
        if (dm->is_file(BENCH_FILE)) dm->destroy_file(BENCH_FILE);
        dm->create_file(BENCH_FILE);
        fd = dm->open_file(BENCH_FILE);
    }

    void teardown() {
        dm->close_file(fd);
        fd = -1;
        if (chdir("..") < 0) throw std::runtime_error("chdir back failed");
    }
};

// ─── Benchmark 1: 顺序写入 ───
static BenchResult bench_sequential_write(DiskManager* dm, int fd, size_t pool_size, size_t num_pages) {
    BufferPoolManager bpm(pool_size, dm);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_pages; i++) {
        PageId pid{fd, INVALID_PAGE_ID};
        Page* p = bpm.new_page(&pid);
        if (p) {
            // 写点数据
            snprintf(p->get_data(), 64, "page-%zu", i);
            bpm.unpin_page(pid, true);
        }
    }
    bpm.flush_all_pages(fd);
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    return {"Sequential Write", ms, num_pages, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Benchmark 2: 顺序读取 ───
static BenchResult bench_sequential_read(DiskManager* dm, int fd, size_t pool_size, size_t num_pages) {
    BufferPoolManager bpm(pool_size, dm);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_pages; i++) {
        PageId pid{fd, static_cast<page_id_t>(i)};
        Page* p = bpm.fetch_page(pid);
        if (p) {
            bpm.unpin_page(pid, false);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    return {"Sequential Read", ms, num_pages, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Benchmark 3: 随机读取 ───
static BenchResult bench_random_read(DiskManager* dm, int fd, size_t pool_size,
                                     size_t num_pages, size_t num_ops) {
    BufferPoolManager bpm(pool_size, dm);
    std::mt19937 rng(42);
    std::uniform_int_distribution<page_id_t> dist(0, num_pages - 1);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_ops; i++) {
        PageId pid{fd, dist(rng)};
        Page* p = bpm.fetch_page(pid);
        if (p) {
            bpm.unpin_page(pid, false);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    return {"Random Read (uniform)", ms, num_ops, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Benchmark 4: 热点读取 (80/20) ───
static BenchResult bench_hotspot_read(DiskManager* dm, int fd, size_t pool_size,
                                      size_t num_pages, size_t num_ops) {
    BufferPoolManager bpm(pool_size, dm);
    std::mt19937 rng(123);

    // 80% 的访问集中在 20% 的热页上
    size_t hot_pages = std::max<size_t>(1, num_pages / 5);
    std::uniform_int_distribution<page_id_t> hot_dist(0, hot_pages - 1);
    std::uniform_int_distribution<page_id_t> cold_dist(0, num_pages - 1);
    std::uniform_int_distribution<int> ratio_dist(0, 99);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_ops; i++) {
        page_id_t pno = (ratio_dist(rng) < 80) ? hot_dist(rng) : cold_dist(rng);
        PageId pid{fd, pno};
        Page* p = bpm.fetch_page(pid);
        if (p) {
            bpm.unpin_page(pid, false);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    return {"Hotspot Read (80/20)", ms, num_ops, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Benchmark 5: 脏页淘汰压力 ───
static BenchResult bench_dirty_eviction(DiskManager* dm, int fd, size_t pool_size,
                                        size_t num_pages, size_t num_ops) {
    BufferPoolManager bpm(pool_size, dm);
    std::mt19937 rng(999);
    std::uniform_int_distribution<page_id_t> dist(0, num_pages - 1);

    auto t0 = std::chrono::steady_clock::now();
    for (size_t i = 0; i < num_ops; i++) {
        PageId pid{fd, dist(rng)};
        Page* p = bpm.fetch_page(pid);
        if (p) {
            // 总是标记为脏页
            snprintf(p->get_data(), 32, "dirty-%zu", i);
            bpm.unpin_page(pid, true);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    return {"Dirty Eviction Stress", ms, num_ops, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Benchmark 6: 多线程并发 ───
static BenchResult bench_concurrent(DiskManager* dm, int fd, size_t pool_size,
                                    size_t num_pages, size_t num_ops_per_thread,
                                    int num_threads) {
    BufferPoolManager bpm(pool_size, dm);

    auto worker = [&](int tid) {
        std::mt19937 rng(tid * 1000 + 7);
        std::uniform_int_distribution<page_id_t> dist(0, num_pages - 1);
        for (size_t i = 0; i < num_ops_per_thread; i++) {
            PageId pid{fd, dist(rng)};
            Page* p = bpm.fetch_page(pid);
            if (p) {
                bpm.unpin_page(pid, (i % 3 == 0));  // 1/3 标脏
            }
        }
    };

    auto t0 = std::chrono::steady_clock::now();
    std::vector<std::thread> threads;
    for (int t = 0; t < num_threads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) th.join();
    auto t1 = std::chrono::steady_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

    size_t total_ops = num_ops_per_thread * num_threads;
    return {"Concurrent Random R/W", ms, total_ops, StatsSnapshot::from(bpm.get_stats())};
}

// ─── Main ───

int main() {
    // 参数
    const size_t POOL_SIZE = 256;       // 缓冲池大小（帧数）
    const size_t TOTAL_PAGES = 4096;    // 磁盘上总页数
    const size_t READ_OPS = 50000;      // 读操作次数
    const size_t DIRTY_OPS = 50000;     // 脏页压力操作次数
    const size_t CONC_OPS = 10000;      // 每线程操作次数
    const int THREADS = 4;              // 并发线程数

    printf("╔═══════════════════════════════════════════╗\n");
    printf("║     Buffer Pool Manager Benchmark         ║\n");
    printf("╠═══════════════════════════════════════════╣\n");
    printf("║  pool_size   = %5zu frames                ║\n", POOL_SIZE);
    printf("║  total_pages = %5zu (on disk)             ║\n", TOTAL_PAGES);
    printf("║  page_size   = %5d bytes                 ║\n", PAGE_SIZE);
    printf("║  threads     = %5d                       ║\n", THREADS);
    printf("╚═══════════════════════════════════════════╝\n\n");

    BenchEnv env;
    env.setup();

    std::vector<BenchResult> results;

    // 1. 顺序写入（也为后续 bench 准备磁盘数据）
    results.push_back(bench_sequential_write(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES));

    // 2. 顺序读取
    results.push_back(bench_sequential_read(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES));

    // 3. 随机读取
    results.push_back(bench_random_read(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES, READ_OPS));

    // 4. 热点读取
    results.push_back(bench_hotspot_read(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES, READ_OPS));

    // 5. 脏页淘汰
    results.push_back(bench_dirty_eviction(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES, DIRTY_OPS));

    // 6. 多线程
    results.push_back(bench_concurrent(env.dm.get(), env.fd, POOL_SIZE, TOTAL_PAGES, CONC_OPS, THREADS));

    // ─── 汇总 ───
    printf("\n\n");
    printf("╔═══════════════════════════════════════════════════════════════════╗\n");
    printf("║                       SUMMARY                                   ║\n");
    printf("╠══════════════════════╦════════╦════════════╦══════════╦══════════╣\n");
    printf("║ Benchmark            ║ ops    ║ throughput ║ hit%%     ║ avg(us)  ║\n");
    printf("╠══════════════════════╬════════╬════════════╬══════════╬══════════╣\n");
    for (auto& r : results) {
        double tput = r.ops / (r.elapsed_ms / 1000.0);
        double hit = r.stats.hit_ratio() * 100;
        double avg = r.stats.avg_fetch_us();
        printf("║ %-20s ║ %6lu ║ %10.0f ║ %7.2f%% ║ %7.2f  ║\n",
               r.name, r.ops, tput, hit, avg);
    }
    printf("╚══════════════════════╩════════╩════════════╩══════════╩══════════╝\n");

    // 详细信息
    for (auto& r : results) {
        print_result(r);
    }

    env.teardown();
    return 0;
}
