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

class UpdateExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

   public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = set_clauses;
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = conds;
        rids_ = rids;
        context_ = context;
    }
    // 新版流程
    // for 每个 rid:
    // ① 读 rec (旧)
    // ② 拷贝出 new_rec，应用 set_clauses
    // ③ Pre-check 阶段（零副作用）：
    //     - 逐索引算 old_key / new_key
    //     - 字节相同 → 标记 不变，跳过
    //     - 字节不同 → 查重，撞别人才 throw
    // ④ Commit 阶段（前面全过才做）：
    //     - delete old_key (仅标记变了的)
    //     - update_record
    //     - insert new_key (仅标记变了的)
    std::unique_ptr<RmRecord> Next() override {
        for (auto &rid : rids_) {
            auto rec = fh_->get_record(rid, context_);

            RmRecord new_rec(fh_->get_file_hdr().record_size);
            memcpy(new_rec.data, rec->data, rec->size);
            for (auto &clause : set_clauses_) {
                auto col = tab_.get_col(clause.lhs.col_name);
                if (!clause.rhs.raw) {
                    clause.rhs.init_raw(col->len);
                }
                memcpy(new_rec.data + col->offset, clause.rhs.raw->data, col->len);
            }

            size_t n_idx = tab_.indexes.size();
            std::vector<std::unique_ptr<char[]>> old_keys(n_idx);
            std::vector<std::unique_ptr<char[]>> new_keys(n_idx);
            std::vector<bool> key_changed(n_idx, false);

            for (size_t i = 0; i < n_idx; ++i) {
                auto &index = tab_.indexes[i];
                old_keys[i] = std::make_unique<char[]>(index.col_tot_len);
                new_keys[i] = std::make_unique<char[]>(index.col_tot_len);

                int offset = 0;
                for (size_t k = 0; k < static_cast<size_t>(index.col_num); ++k) {
                    memcpy(old_keys[i].get() + offset, rec->data + index.cols[k].offset, index.cols[k].len);
                    memcpy(new_keys[i].get() + offset, new_rec.data + index.cols[k].offset, index.cols[k].len);
                    offset += index.cols[k].len;
                }

                if (memcmp(old_keys[i].get(), new_keys[i].get(), index.col_tot_len) == 0) {
                    continue;
                }

                key_changed[i] = true;

                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                std::vector<Rid> out;
                if (ih->get_value(new_keys[i].get(), &out, context_->txn_)) {
                    bool has_conflict = false;
                    for (auto &out_rid : out) {
                        if (out_rid.page_no != rid.page_no || out_rid.slot_no != rid.slot_no) {
                            has_conflict = true;
                            break;
                        }
                    }
                    if (has_conflict) {
                        throw IndexEntryDuplicateError();
                    }
                }
            }

            for (size_t i = 0; i < n_idx; ++i) {
                if (!key_changed[i]) {
                    continue;
                }
                auto &index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->delete_entry(old_keys[i].get(), context_->txn_);
            }

            fh_->update_record(rid, new_rec.data, context_);

            for (size_t i = 0; i < n_idx; ++i) {
                if (!key_changed[i]) {
                    continue;
                }
                auto &index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(
                    sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                ih->insert_entry(new_keys[i].get(), rid, context_->txn_);
            }
        }
        return nullptr;
    }
    //     for (auto &rid : rids_) {
    //         auto rec = fh_->get_record(rid, context_);
    //         for (auto &index : tab_.indexes) {
    //             auto ih = sm_manager_->ihs_.at(
    //                 sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
    //             char *key = new char[index.col_tot_len];
    //             int offset = 0;
    //             for (int i = 0; i < index.col_num; ++i) {
    //                 memcpy(key + offset, rec ->data + index.cols[i].offset, index.cols[i].len);
    //                 offset += index.cols[i].len;
    //             }

    //             ih->delete_entry(key, context_->txn_);
    //             delete[] key;
    //         }

    //         for (auto &clause : set_clauses_) {
    //             auto col = tab_.get_col(clause.lhs.col_name);
    //             if (!clause.rhs.raw) {
    //                 clause.rhs.init_raw(col->len);
    //             }
    //             memcpy(rec->data + col->offset, clause.rhs.raw->data, col->len);
    //         }
    //         fh_->update_record(rid, rec->data, context_);

    //         for (auto &index : tab_.indexes) {
    //             auto ih = sm_manager_->ihs_.at(
    //                 sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();

    //                 char *key = new char[index.col_tot_len];
    //                 int offset = 0;
    //                 for (int i = 0; i < index.col_num; ++i) {
    //                     memcpy(key + offset, rec->data + index.cols[i].offset, index.cols[i].len);
    //                     offset += index.cols[i].len;
    //                 }
    //                 ih->insert_entry(key, rid, context_->txn_);
    //                 delete[] key;
    //         }
    //     }
    //     return nullptr;
    // }
    // 旧版流程
    // for 每个 rid:
    // ① 遍历索引 → 直接 delete_entry(old_key)    ← 旧索引已经没了
    // ② 原地改 rec → update_record
    // ③ 遍历索引 → 直接 insert_entry(new_key)    ← 如果撞了就炸

    Rid &rid() override { return _abstract_rid; }
};