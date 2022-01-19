//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <list>
#include <mutex>
#include "algorithm"
#include "common/config.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> lock_guard(mtx_);
  if (ls_.empty()) {
    return false;
  }

  *frame_id = ls_.back();
  ls_.pop_back();
  mp_.erase(*frame_id);

  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(mtx_);
  auto it = mp_.find(frame_id);
  if (it != mp_.end()) {
    ls_.erase(it->second);
    mp_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock_guard(mtx_);
  auto it = mp_.find(frame_id);
  if (it == mp_.end()) {
    ls_.emplace_front(frame_id);
    mp_[frame_id] = ls_.begin();
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock_guard(mtx_);
  return ls_.size();
}

}  // namespace bustub
