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

class IndexScanExecutor : public AbstractExecutor {
   private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_;
    std::unique_ptr<RecScan> scan_;

    SmManager *sm_manager_;

   public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names,
                    Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        index_col_names_ = index_col_names; 
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
            {OP_EQ, OP_EQ}, {OP_NE, OP_NE}, {OP_LT, OP_GT}, {OP_GT, OP_LT}, {OP_LE, OP_GE}, {OP_GE, OP_LE},
        };

        for (auto &cond : conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    void beginTuple() override {
        IxIndexHandle *ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_)).get();
        
        Iid lower, upper;
        
        char *lower_key = new char[index_meta_.col_tot_len];
        char *upper_key = new char[index_meta_.col_tot_len];
        memset(lower_key, 0, index_meta_.col_tot_len);
        memset(upper_key, 0, index_meta_.col_tot_len);
        
        for (auto &col : index_meta_.cols) {
            for (auto &cond : fed_conds_) {
                if (cond.is_rhs_val && cond.lhs_col.col_name == col.name) {
                    if (cond.op == OP_EQ) {
                        memcpy(lower_key + col.offset, cond.rhs_val.raw->data, col.len);
                        memcpy(upper_key + col.offset, cond.rhs_val.raw->data, col.len);
                    }
                    break;
                }
            }
        }
        
        lower = ih->lower_bound(lower_key);
        upper = ih->upper_bound(upper_key);
        
        delete[] lower_key;
        delete[] upper_key;
        
        scan_ = std::make_unique<IxScan>(ih, lower, upper, sm_manager_->get_bpm());
        
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (check_conditions(rec->data)) {
                return;
            }
            scan_->next();
        }
    }

    void nextTuple() override {
        scan_->next();
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
            if (check_conditions(rec->data)) {
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

    Rid &rid() override { return rid_; }

    size_t tupleLen() const override { return len_; }

    const std::vector<ColMeta> &cols() const override { return cols_; }

    std::string getType() override { return "IndexScanExecutor"; }

private:
    ColMeta get_col_offset(const TabCol &target) {
        for (auto &col : cols_) {
            if (col.tab_name == target.tab_name && col.name == target.col_name) {
                return col;
            }
        }
        throw ColumnNotFoundError(target.col_name);
    }

    bool check_conditions(const char *record_data) {
        for (const auto &cond : fed_conds_) {
            if (!check_single_condition(record_data, cond)) {
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
            return compare_values(lhs_data, rhs_data, lhs_type, lhs_len, cond.op);
        }
    }

    bool compare_values(const char *lhs, const char *rhs, ColType type, int len, CompOp op) {
        int cmp_result;

        if (type == TYPE_INT) {
            cmp_result = (*(int *)lhs < *(int *)rhs) ? -1 :
                         ((*(int *)lhs > *(int *)rhs) ? 1 : 0);
        } else if (type == TYPE_FLOAT) {
            cmp_result = (*(float *)lhs < *(float *)rhs) ? -1 :
                         ((*(float *)lhs > *(float *)rhs) ? 1 : 0);
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