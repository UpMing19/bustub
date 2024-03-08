//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <climits>
#include <cmath>
#include "common/config.h"
#include "common/exception.h"

namespace bustub {

LRUKNode::LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {
  history_.clear();
  is_evictable_ = false;
  left_ = nullptr;
  right_ = nullptr;
}

void LRUKNode::SetIsEvictable(bool f) { is_evictable_ = f; }
auto LRUKNode::GetIsEvictable() -> bool { return is_evictable_; }
auto LRUKNode::PushTime(size_t t) -> void {
  history_.push_back(t);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  full_k_head_ = new LRUKNode();
  full_k_tail_ = new LRUKNode();
  full_k_head_->right_ = full_k_tail_;
  full_k_tail_->left_ = full_k_head_;

  unfull_k_head_ = new LRUKNode();
  unfull_k_tail_ = new LRUKNode();
  unfull_k_head_->right_ = unfull_k_tail_;
  unfull_k_tail_->left_ = unfull_k_head_;

  full_k_node_store_.clear();
  unfull_k_node_store_.clear();
}

LRUKReplacer::~LRUKReplacer() {
  for (auto &it : full_k_node_store_) {
    delete (it.second);
  }
  for (auto &it : unfull_k_node_store_) {
    delete (it.second);
  }
  delete (unfull_k_head_);
  delete (unfull_k_tail_);
  delete (full_k_head_);
  delete (full_k_tail_);
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_++;

  if (!unfull_k_node_store_.empty()) {
    for (auto it = unfull_k_head_->right_; it != unfull_k_tail_; it = it->right_) {
      if (it->GetIsEvictable()) {
        *frame_id = it->GetFrameId();
        unfull_k_size_--;
        curr_size_--;
        RemoveNode(it);
        unfull_k_node_store_.erase(*frame_id);
        delete it;
        return true;
      }
    }
  }

  for (auto it = full_k_head_->right_; it != full_k_tail_; it = it->right_) {
    if (it->GetIsEvictable()) {
      *frame_id = it->GetFrameId();
      full_k_size_--;
      curr_size_--;
      RemoveNode(it);
      full_k_node_store_.erase(*frame_id);
      delete it;
      return true;
    }
  }
  return false;
}

auto LRUKReplacer::RemoveNode(LRUKNode *node) -> void {
  LRUKNode *left = node->left_;
  LRUKNode *right = node->right_;
  left->right_ = right;
  right->left_ = left;
}

auto LRUKReplacer::InsertNode(LRUKNode *node1, LRUKNode *node2) -> void {
  LRUKNode *nxt = node1->right_;
  node1->right_ = node2;
  node2->left_ = node1;
  node2->right_ = nxt;
  nxt->left_ = node2;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  if ((static_cast<size_t>(frame_id) > (replacer_size_))) {
    throw Exception("In function RecordAccess ::: frame_id > replacer_size_");
  }

  if (full_k_node_store_.count(frame_id) == 0 && unfull_k_node_store_.count(frame_id) == 0) {
    auto *node = new LRUKNode(k_, frame_id);
    // todo 新建的设置成ture 从某些Test中看，应该是这样的
    // node->SetIsEvictable(true);
    // curr_size_++;
    unfull_k_node_store_[frame_id] = node;
    InsertNode(unfull_k_tail_->left_, node);
  }

  if (unfull_k_node_store_.count(frame_id) != 0) {
    LRUKNode *node = unfull_k_node_store_[frame_id];
    node->PushTime(current_timestamp_);
    if (node->GetHistorySize() == k_) {
      unfull_k_size_--;
      full_k_size_++;
      RemoveNode(node);
      int flag = 1;
      for (auto it = full_k_head_->right_; it != full_k_tail_; it = it->right_) {
        if (it->history_.front() > node->history_.front()) {
          InsertNode(it->left_, node);
          flag = 0;
          break;
        }
      }
      if (flag != 0) {
        InsertNode(full_k_tail_->left_, node);
      }

      unfull_k_node_store_.erase(frame_id);
      full_k_node_store_[frame_id] = node;
    }
  } else if (full_k_node_store_.count(frame_id) != 0) {
    LRUKNode *node = full_k_node_store_[frame_id];
    node->PushTime(current_timestamp_);

    int flag = 1;
    for (auto it = node->right_; it != full_k_tail_; it = it->right_) {
      if (it->history_.front() > node->history_.front()) {
        RemoveNode(node);
        InsertNode(it->left_, node);
        flag = 0;
        break;
      }
    }
    if (flag != 0) {
      RemoveNode(node);
      InsertNode(full_k_tail_->left_, node);
    }
  }
}

void LRUKReplacer::SetEvictable(bustub::frame_id_t frame_id, bool set_evictable) {
  std::unique_lock<std::mutex> lock(latch_);
  current_timestamp_++;
  if ((static_cast<size_t>(frame_id) > (replacer_size_))) {
    throw Exception("异常LUR010");
  }

  if (full_k_node_store_.count(frame_id) == 0 && unfull_k_node_store_.count(frame_id) == 0) {
    return;
  }
  LRUKNode *node = nullptr;
  if (unfull_k_node_store_.count(frame_id) != 0) {
    node = unfull_k_node_store_[frame_id];
    if (node->GetIsEvictable() && !set_evictable) {
      curr_size_--;
      node->SetIsEvictable(set_evictable);
    } else if (!node->GetIsEvictable() && set_evictable) {
      curr_size_++;
      node->SetIsEvictable(set_evictable);
    }
  } else if (full_k_node_store_.count(frame_id) != 0) {
    node = full_k_node_store_[frame_id];
    if (node->GetIsEvictable() && !set_evictable) {
      curr_size_--;
      node->SetIsEvictable(set_evictable);
    } else if (!node->GetIsEvictable() && set_evictable) {
      curr_size_++;
      node->SetIsEvictable(set_evictable);
    }
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::unique_lock<std::mutex> lock(latch_);
  if (full_k_node_store_.count(frame_id) == 0 && unfull_k_node_store_.count(frame_id) == 0) {
    return;
  }
  if (unfull_k_node_store_.count(frame_id) != 0) {
    LRUKNode *node = unfull_k_node_store_[frame_id];
    if (!node->GetIsEvictable()) {
      throw Exception("不是候选键不可淘汰");
    }
    RemoveNode(node);
    unfull_k_node_store_.erase(frame_id);
    unfull_k_size_--;
    delete node;
  } else if (full_k_node_store_.count(frame_id) != 0) {
    LRUKNode *node = full_k_node_store_[frame_id];
    if (!node->GetIsEvictable()) {
      throw Exception("不可淘汰");
    }
    RemoveNode(node);
    full_k_node_store_.erase(frame_id);
    full_k_size_--;
    delete node;
  }
  curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::unique_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
