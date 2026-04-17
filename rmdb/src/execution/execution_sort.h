/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once
#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    ColMeta cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    bool is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

    std::vector<std::unique_ptr<RmRecord>> buffer_;   // 缓存所有子记录
    std::vector<size_t> sorted_idx_;                  // 排序后的索引序列
    size_t pos_;                                      // 当前输出位置

    bool less_than(const RmRecord *a, const RmRecord *b) {
        const char *la = a->data + cols_.offset;
        const char *lb = b->data + cols_.offset;
        int cmp;
        if (cols_.type == TYPE_INT) {
            int x = *(int *)la, y = *(int *)lb;
            cmp = (x < y) ? -1 : ((x > y) ? 1 : 0);
        } else if (cols_.type == TYPE_FLOAT) {
            float x = *(float *)la, y = *(float *)lb;
            cmp = (x < y) ? -1 : ((x > y) ? 1 : 0);
        } else {
            cmp = memcmp(la, lb, cols_.len);
        }
        return is_desc_ ? (cmp > 0) : (cmp < 0);
    }
   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, TabCol sel_cols, bool is_desc) {
        prev_ = std::move(prev);
        cols_ = prev_->get_col_offset(sel_cols);
        is_desc_ = is_desc;
        tuple_num = 0;
        used_tuple.clear();
    }

    void beginTuple() override { 
        buffer_.clear();
        sorted_idx_.clear();
        for (prev_->beginTuple(); !prev_->is_end(); prev_->nextTuple()) {
            auto rec = prev_->Next();
            if (rec) buffer_.push_back(std::move(rec));
        }
        tuple_num = buffer_.size();
        sorted_idx_.resize(tuple_num);
        for (size_t i = 0; i < tuple_num; ++i) sorted_idx_[i] = i;
        std::sort(sorted_idx_.begin(), sorted_idx_.end(),
                [this](size_t a, size_t b) {
                    return less_than(buffer_[a].get(), buffer_[b].get());
                });
        pos_ = 0;
    }

    void nextTuple() override {
        if (pos_ < tuple_num) ++pos_;
    }

    std::unique_ptr<RmRecord> Next() override {
        if (pos_ >= tuple_num) return nullptr;
        auto &src = buffer_[sorted_idx_[pos_]];
        auto copy = std::make_unique<RmRecord>(src->size);
        memcpy(copy->data, src->data, src->size);
        return copy;
    }

    bool is_end() const override { return pos_ >= tuple_num; }
    size_t tupleLen() const override { return prev_->tupleLen(); }
    const std::vector<ColMeta> &cols() const override { return prev_->cols(); }
    std::string getType() override { return "SortExecutor"; }
    Rid &rid() override { return _abstract_rid; }
};