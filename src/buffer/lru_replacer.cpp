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
#include "algorithm"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mtx_.lock();

  if (ls_.empty()) {
    mtx_.unlock();
    return false;
  }

  *frame_id = ls_.front();
  ls_.pop_front();

  mtx_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx_.lock();

  auto it = std::find(ls_.begin(), ls_.end(), frame_id);
  if (it != ls_.end()) {
    ls_.erase(it);
  }

  mtx_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx_.lock();
  auto it = std::find(ls_.begin(), ls_.end(), frame_id);
  if (it != ls_.end()) {
    mtx_.unlock();
    return;
  }
  ls_.emplace_back(frame_id);
  mtx_.unlock();
}

size_t LRUReplacer::Size() {
  return ls_.size();
}

}  // namespace bustub
