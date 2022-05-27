//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <mutex>
#include <tuple>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  txn_id_t txn_id = txn->GetTransactionId();

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  /* see https://juejin.cn/post/7029372430397210632 */
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue *queue = &lock_table_[rid];
  queue->request_queue_.emplace_back(LockRequest(txn_id, LockMode::SHARED));

  while (queue->has_exclusive_) {
    queue->cv_.wait(lck);
  }

  ++queue->shared_cnt_;
  txn->GetSharedLockSet()->emplace(rid);
  queue->GetIter(txn_id)->granted_ = true;

  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  txn_id_t txn_id = txn->GetTransactionId();

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue *queue = &lock_table_[rid];
  queue->request_queue_.emplace_back(LockRequest(txn_id, LockMode::EXCLUSIVE));

  while (queue->has_exclusive_ || queue->shared_cnt_ > 0) {
    queue->cv_.wait(lck);
  }

  BUSTUB_ASSERT(queue->has_exclusive_ == false, "multiple writers");
  queue->has_exclusive_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  queue->GetIter(txn_id)->granted_ = true;

  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  txn_id_t txn_id = txn->GetTransactionId();

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCK_ON_SHRINKING);
    return false;
  }

  LockRequestQueue *queue = &lock_table_[rid];
  auto cur_req = queue->GetIter(txn_id);

  if (queue->upgrading_ != INVALID_TXN_ID) {
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
    return false;
  }

  BUSTUB_ASSERT(cur_req->lock_mode_ == LockMode::SHARED, "lock mode is not shared");
  --queue->shared_cnt_;
  queue->upgrading_ = txn_id;
  txn->GetSharedLockSet()->erase(rid);
  queue->GetIter(txn_id)->granted_ = false;

  while (queue->has_exclusive_ || queue->shared_cnt_ > 0) {
    queue->cv_.wait(lck);
  }

  BUSTUB_ASSERT(queue->has_exclusive_ == false, "multiple writers");
  queue->has_exclusive_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  queue->GetIter(txn_id)->granted_ = true;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  txn_id_t txn_id = txn->GetTransactionId();

  LockRequestQueue *queue = &lock_table_[rid];
  auto cur_req = queue->GetIter(txn_id);

  if (cur_req->lock_mode_ == LockMode::EXCLUSIVE) {
    queue->has_exclusive_ = false;
  } else {
    --queue->shared_cnt_;
  }

  if (txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  queue->cv_.notify_one();

  queue->request_queue_.erase(cur_req);
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

}  // namespace bustub
