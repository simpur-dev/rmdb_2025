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

class InsertExecutor : public AbstractExecutor {
   private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_;                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

   public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<Value> values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
    };

    std::unique_ptr<RmRecord> Next() override {
        // 构造record buffer
        RmRecord rec(fh_->get_file_hdr().record_size);
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if (col.type != val.type) {
                throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

    //     // Insert into record file
    //     rid_ = fh_->insert_record(rec.data, context_);
        
    //     // Insert into index
    //     for(size_t i = 0; i < tab_.indexes.size(); ++i) {
    //         auto& index = tab_.indexes[i];
    //         auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
    //         char* key = new char[index.col_tot_len];
    //         int offset = 0;
    //         for(size_t i = 0; i < index.col_num; ++i) {
    //             memcpy(key + offset, rec.data + index.cols[i].offset, index.cols[i].len);
    //             offset += index.cols[i].len;
    //         }
    //         ih->insert_entry(key, rid_, context_->txn_);
    //     }
    //     return nullptr;
    //   }


//旧版
//    构造 record
//    │
//    ▼
// insert_record          ← ① 数据立刻落盘
//    │
//    ▼
// for 每个索引:
//     构造 key
//     insert_entry       ← ② 如果 B+ 树发现重复 key，这里抛异常
//                           但 ① 的数据已经落盘了！索引已经插进去的也还在！
// 题目三要求：插入重复键时，服务端输出 failure 且不能损坏数据。
// insert into grade values (2, 'B');  -- 假设建了 name 的唯一索引
// -- 旧版流程：
// -- ① insert_record → 把 (2, 'B') 写进堆文件 ✓
// -- ② id 索引 insert_entry → 成功 ✓
// -- ③ name 索引 insert_entry → 假设这里撞了，抛异常
// --   → 堆文件里多了一条 (2, 'B')
// --   → id 索引里多了一条 key=2 → rid
// --   数据库永久不一致！
        std::vector<std::unique_ptr<char[]>> keys(tab_.indexes.size());
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            keys[i] = std::make_unique<char[]>(index.col_tot_len);
            char *key = keys[i].get();

            int offset = 0;
            for (size_t k = 0; k < index.col_num; ++k) {
                memcpy(key + offset, rec.data + index.cols[k].offset, index.cols[k].len);
                offset += index.cols[k].len;
            }

            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            std::vector<Rid> out;
            if (ih->get_value(key, &out, context_->txn_)) {
                throw IndexEntryDuplicateError();
            }
        }

        rid_ = fh_->insert_record(rec.data, context_);
        for (size_t i = 0; i < tab_.indexes.size(); ++i) {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            ih->insert_entry(keys[i].get(), rid_, context_->txn_);
        }
        return nullptr;
    }
    //新版流程
//     构造 record
//    │
//    ▼
// for 每个索引:
//     构造 key (缓存)
//     get_value 查重      ← ① 只读操作，零副作用
//     若重复 → throw       ← 抛出时什么都没写，干净利落
//    │
//    ▼
// insert_record          ← ② 确认全过再写
//    │
//    ▼
// for 每个索引:
//     insert_entry       ← ③ 已经保证不会重复

    Rid &rid() override { return rid_; }
};