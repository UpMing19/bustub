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
};

void LRUKNode::SetIsEvictable(bool f) { is_evictable_ = f; };
auto LRUKNode::GetIsEvictable() -> bool { return is_evictable_; }
auto LRUKNode::GetKNum() -> size_t {
  if (history_.size() < k_) {
    return 0;
  }
  return history_.front();
}
void LRUKNode::PushTimeStampToList(size_t timestamp) {
  history_.push_back(timestamp);
  if (history_.size() > k_) {
    history_.pop_front();
  }
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  latch_.lock();
  current_timestamp_++;
  size_t ma = 0;
  for (const auto &it : node_store_) {
    frame_id_t id = it.first;
    LRUKNode node = it.second;
    if (!node.GetIsEvictable()) {
      continue;
    }
    size_t num = node.GetKNum();
    if (num == 0) {
      num = ULONG_MAX;
    } else {
      num = current_timestamp_ - num;
    }

    if (num > ma) {
      *frame_id = id;
      ma = num;
      // std::cout<<"id"<<id<<std::endl;
    }
  }
  if (ma != 0) {
    node_store_.erase(*frame_id);
    curr_size_--;
  }

  latch_.unlock();
  return ma != 0;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  latch_.lock();
  current_timestamp_++;
  if ((static_cast<size_t>(frame_id) > (replacer_size_))) {
    latch_.unlock();
    throw Exception("In function RecordAccess ::: frame_id > replacer_size_");
  }
  if (node_store_.count(frame_id) != 0U) {
  } else {
    if (curr_size_ == replacer_size_) {
      frame_id_t f;
      Evict(&f);
    }

    LRUKNode now = LRUKNode(k_, frame_id);
    now.SetIsEvictable(true);
    node_store_[frame_id] = now;
    curr_size_++;
  }
  LRUKNode &now = node_store_[frame_id];
  now.PushTimeStampToList(current_timestamp_);
  latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  latch_.lock();
  current_timestamp_++;
  if ((static_cast<size_t>(frame_id) > (replacer_size_))) {
    latch_.unlock();
    throw Exception("In function SetEvictable ::: frame_id > replacer_size_");
  }
  if (node_store_.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  LRUKNode &now = node_store_[frame_id];
  if (set_evictable && !now.GetIsEvictable()) {
    now.SetIsEvictable(set_evictable);
    curr_size_++;
  } else if (!set_evictable && now.GetIsEvictable()) {
    now.SetIsEvictable(set_evictable);
    curr_size_--;
  }
  latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  latch_.lock();
  current_timestamp_++;
  if (node_store_.count(frame_id) == 0) {
    latch_.unlock();
    return;
  }
  if (!node_store_[frame_id].GetIsEvictable()) {
    latch_.unlock();
    throw Exception("In function Remove ::: frame_id  not isEvictable");
  }
  curr_size_--;
  node_store_.erase(frame_id);
  latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
