#pragma once

#include <list>
#include <mutex>
#include <unordered_map>

#include "common/config.h"
#include "replacer/replacer.h"

class LRUKReplacer : public Replacer {
public:
    //explicit = 禁止隐式转换，只允许显示调用
    explicit LRUKReplacer(size_t num_pagers, size_t k = 2);
    ~LRUKReplacer() override;

    bool victim(frame_id_t *frame_id) override;
    void pin(frame_id_t frame_id) override;
    void unpin(frame_id_t frame_id) override;
    size_t Size() override;
    
private:
    struct FrameInfo {
        size_t access_count = 0;
        bool in_cache = false;    //在哪个队列：false=history, true=cache
    };

    std::mutex latch_;
    size_t max_size_;
    size_t k_;

    std::list<frame_id_t> history_list_;
    std::unorded_map<frame_id_t, std::list<frame_id_t>::iterator> history_hash_;

    std::list<frame_id_t> cache_list_;
    
}