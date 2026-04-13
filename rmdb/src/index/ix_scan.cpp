/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

void IxScan::next() {
    assert(!is_end());

    if (current_page_ == nullptr) {
        current_page_ = bpm_->fetch_page(PageId{ih_->fd_, iid_.page_no});
        current_page_->latch_lock(false);
    }

    IxNodeHandle node(ih_->file_hdr_, current_page_);
    assert(node.is_leaf_page());
    assert(iid_.slot_no < node.get_size());

    iid_.slot_no++;
    if (iid_.page_no != ih_->file_hdr_->last_leaf_ && iid_.slot_no == node.get_size()) {
        iid_.slot_no = 0;
        page_id_t next_page_no = node.get_next_leaf();
        iid_.page_no = next_page_no;

        current_page_->latch_unlock(false);
        bpm_->unpin_page(current_page_->get_page_id(), false);

        current_page_ = bpm_->fetch_page(PageId{ih_->fd_, next_page_no});
        current_page_->latch_lock(false);
    }
}

Rid IxScan::rid() const {
    return ih_->get_rid(iid_);
}