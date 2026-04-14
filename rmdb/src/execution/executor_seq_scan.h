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

class SeqScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

   public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    size_t tupleLen() const override {
        return len_;
    }

    const std::vector<ColMeta> &cols() const
    override {
        return cols_;
    }

    std::string getType() override {
        return "SeqScanExecutor";
    }


    void beginTuple() override {
        scan_ = std::make_unique<RmScan>(fh_);
        while (!scan_->is_end()) {
            if (fed_conds_.empty() || check_conditions(scan_->rid())) {
                rid_ = scan_->rid();
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        scan_->next();

        while (!scan_->is_end()) {
            if (fed_conds_.empty() || check_conditions(scan_->rid())) {
                rid_ = scan_->rid();
                return;
            }
            scan_->next();
        }
    }

    std::unique_ptr<RmRecord> Next() override {
        if (scan_->is_end()) {
            return nullptr;
        }
        rid_ = scan_->rid();

        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override {
        return scan_ == nullptr || scan_->is_end();
    }

    Rid &rid() override { 
        return rid_; 
    }

    ColMeta get_col_offset(const TabCol &target) override {
        for (auto &col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }

    bool check_conditions(const Rid &rid) {

        auto record = fh_->get_record(rid, context_);

        for (const auto &cond : fed_conds_) {
            if (!check_single_condition(record->data, cond)) {
                return false;
            }
        }
        return true;
    }
    
    bool check_single_condition(const char *record_data, const Condition &cond) {
        auto lhs_col = get_col_offset(cond.lhs_col);

        char *lhs_data = const_cast<char *>(record_data) + lhs_col.offset;

        ColType lhs_type = lhs_col.type;
        int lhs_len = lhs_col.len;

        if (cond.is_rhs_val) {
            char *rhs_data = cond.rhs_val.raw->data;
            return compare_values(lhs_data, rhs_data, lhs_type, lhs_len, cond.op);
        } else {
            auto rhs_col = get_col_offset(cond.rhs_col);
            char *rhs_data = const_cast<char *>(record_data) + rhs_col.offset;
            // 使用左侧类型进行比较（查询优化器保证两侧类型一致）
            return compare_values(lhs_data, rhs_data, lhs_type, lhs_len, cond.op);
        }
    }

    bool compare_values(const char *lhs, const char *rhs, ColType type, int len, CompOp op) {
        int cmp_result;

        if (type == TYPE_INT) {
            int l = *(int *)lhs;
            int r = *(int *)rhs;
            cmp_result = (l < r) ? -1 : ((l > r) ? 1 : 0);
        } else if (type == TYPE_FLOAT) {
            float l = *(float *)lhs;
            float r = *(float *)rhs;
            cmp_result = (l < r) ? -1 : ((l > r) ? 1 : 0);
        } else {
            cmp_result = memcmp(lhs, rhs, len);
        }

        switch (op) {
            case OP_EQ: return cmp_result == 0;
            case OP_NE: return cmp_result != 0;
            case OP_LT: return cmp_result < 0;
            case OP_GT: return cmp_result > 0;
            case OP_LE: return cmp_result <= 0;
            case OP_GE: return cmp_result >= 0;
            default: return false;
        }
    }
};