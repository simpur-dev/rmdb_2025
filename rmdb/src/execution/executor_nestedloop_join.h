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

class NestedLoopJoinExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    bool isend;

    bool check_join_conds(const RmRecord *left_rec, const RmRecord *right_rec) {
        for (auto &cond : fed_conds_) {
            auto left_col = get_col(left_->cols(), cond.lhs_col);
            char *lhs_data = left_rec->data + left_col->offset;

            char *rhs_data;
            ColType type = left_col->type;
            int len = left_col->len;

            if (cond.is_rhs_val) {
                rhs_data = cond.rhs_val.raw->data;
            } else {
                // rhs 可能在左表或右表
                auto &prev_cols = left_->cols();
                auto pos = std::find_if(prev_cols.begin(), prev_cols.end(), [&](const ColMeta &c) {
                    return c.tab_name == cond.rhs_col.tab_name && c.name == cond.rhs_col.col_name;
                });
                if (pos != prev_cols.end()) {
                    rhs_data = left_rec->data + pos->offset;
                } else {
                    auto right_col = get_col(right_->cols(), cond.rhs_col);
                    rhs_data = right_rec->data + right_col->offset;
                }
            }

            int cmp;
            if (type == TYPE_INT) {
                int l = *(int *)lhs_data, r = *(int *)rhs_data;
                cmp = (l < r) ? -1 : ((l > r) ? 1 : 0);
            } else if (type == TYPE_FLOAT) {
                float l = *(float *)lhs_data, r = *(float *)rhs_data;
                cmp = (l < r) ? -1 : ((l > r) ? 1 : 0);
            } else {
                cmp = memcmp(lhs_data, rhs_data, len);
            }

            bool ok;
            switch (cond.op) {
                case OP_EQ: ok = cmp == 0; break;
                case OP_NE: ok = cmp != 0; break;
                case OP_LT: ok = cmp < 0; break;
                case OP_GT: ok = cmp > 0; break;
                case OP_LE: ok = cmp <= 0; break;
                case OP_GE: ok = cmp >= 0; break;
                default: ok = false;
            }
            if (!ok) return false;
        }
        return true;
    }

   public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right, 
                            std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col : right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    void beginTuple() override {
        left_->beginTuple();
        if (left_->is_end()) {
            isend = true;
            return;
        }
        right_->beginTuple();
        // 找到第一对满足条件的组合
        while (!isend) {
            if (!right_->is_end()) {
                if (fed_conds_.empty() || check_join_conds(left_->Next().get(), right_->Next().get())) {
                    return;  // 找到匹配
                }
                right_->nextTuple();
            } else {
                left_->nextTuple();
                if (left_->is_end()) {
                    isend = true;
                    return;
                }
                right_->beginTuple();
            }
        }
    }

    void nextTuple() override {
        right_->nextTuple();
        while (!isend) {
            if (!right_->is_end()) {
                if (fed_conds_.empty() || check_join_conds(left_->Next().get(), right_->Next().get())) {
                    return;
                }
                right_->nextTuple();
            } else {
                left_->nextTuple();
                if (left_->is_end()) {
                    isend = true;
                    return;
                }
                right_->beginTuple();
            }
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (isend) return nullptr;
        auto left_rec = left_->Next();
        auto right_rec = right_->Next();
        if (!left_rec || !right_rec) return nullptr;
        // 拼接左右记录
        auto joined = std::make_unique<RmRecord>(len_);
        memcpy(joined->data, left_rec->data, left_->tupleLen());
        memcpy(joined->data + left_->tupleLen(), right_rec->data, right_->tupleLen());
        return joined;
    }

    bool is_end() const override { return isend; }
    size_t tupleLen() const override { return len_; }
    const std::vector<ColMeta> &cols() const override { return cols_; }
    std::string getType() override { return "NestedLoopJoinExecutor"; }
};