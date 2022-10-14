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

#include <tuple>
#include <utility>
#include <vector>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

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
  }

  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }

  // see https://juejin.cn/post/7029372430397210632
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue *queue = &lock_table_[rid];
  queue->request_queue_.emplace_back(txn_id, LockMode::SHARED);

  if (queue->exclusive_ != INVALID_TXN_ID) {
    for (auto req = queue->request_queue_.begin(); req != queue->request_queue_.end(); ++req) {
      if (req->txn_id_ > txn_id) {
        /* we are older, just wound the victim.*/
        Transaction *victim_txn = TransactionManager::GetTransaction(req->txn_id_);
        if (victim_txn->GetState() == TransactionState::ABORTED) {
          continue;
        }

        if (req->lock_mode_ == LockMode::EXCLUSIVE) {
          victim_txn->SetState(TransactionState::ABORTED);
          req->valid_ = false;
          queue->exclusive_ = INVALID_TXN_ID;
        }
      }
    }

    while (queue->exclusive_ != INVALID_TXN_ID && txn->GetState() != TransactionState::ABORTED) {
      queue->cv_.wait(lck);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.erase(queue->GetIter(txn_id));
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  ++queue->shared_cnt_;
  txn->GetSharedLockSet()->emplace(rid);

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
    return false;
  }

  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }

  LockRequestQueue *queue = &lock_table_[rid];
  queue->request_queue_.emplace_back(txn_id, LockMode::EXCLUSIVE);

  if (queue->exclusive_ != INVALID_TXN_ID || queue->shared_cnt_ > 0) {
    for (auto req = queue->request_queue_.begin(); req != queue->request_queue_.end(); ++req) {
      if (req->txn_id_ > txn_id) {
        /* we are older, just wound the victim.*/
        Transaction *victim_txn = TransactionManager::GetTransaction(req->txn_id_);
        if (victim_txn->GetState() == TransactionState::ABORTED) {
          continue;
        }

        victim_txn->SetState(TransactionState::ABORTED);
        req->valid_ = false;
        if (req->lock_mode_ == LockMode::SHARED) {
          --queue->shared_cnt_;
        } else {
          queue->exclusive_ = INVALID_TXN_ID;
        }
      }
    }

    while ((queue->exclusive_ != INVALID_TXN_ID || queue->shared_cnt_ > 0) &&
           txn->GetState() != TransactionState::ABORTED) {
      queue->cv_.wait(lck);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.erase(queue->GetIter(txn_id));
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  queue->exclusive_ = txn_id;
  txn->GetExclusiveLockSet()->emplace(rid);

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
  }

  LockRequestQueue *queue = &lock_table_[rid];
  auto cur_req = queue->GetIter(txn_id);

  if (queue->upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn_id, AbortReason::UPGRADE_CONFLICT);
  }

  txn->GetSharedLockSet()->erase(rid);
  --queue->shared_cnt_;
  queue->upgrading_ = txn_id;
  cur_req->lock_mode_ = LockMode::EXCLUSIVE;

  if (queue->exclusive_ != INVALID_TXN_ID || queue->shared_cnt_ > 0) {
    for (auto req = queue->request_queue_.begin(); req != queue->request_queue_.end(); ++req) {
      if (req->txn_id_ > txn_id) {
        /* we are older, just wound the victim.*/
        Transaction *victim_txn = TransactionManager::GetTransaction(req->txn_id_);
        if (victim_txn->GetState() == TransactionState::ABORTED) {
          continue;
        }

        victim_txn->SetState(TransactionState::ABORTED);
        req->valid_ = false;
        if (req->lock_mode_ == LockMode::SHARED) {
          --queue->shared_cnt_;
        } else {
          queue->exclusive_ = INVALID_TXN_ID;
        }
      }
    }

    while ((queue->exclusive_ != INVALID_TXN_ID || queue->shared_cnt_ > 0) &&
           txn->GetState() != TransactionState::ABORTED) {
      queue->cv_.wait(lck);
    }
  }

  if (txn->GetState() == TransactionState::ABORTED) {
    queue->request_queue_.erase(queue->GetIter(txn_id));
    throw TransactionAbortException(txn_id, AbortReason::DEADLOCK);
  }

  txn->GetExclusiveLockSet()->emplace(rid);
  queue->exclusive_ = txn_id;
  queue->upgrading_ = INVALID_TXN_ID;

  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lck(latch_);
  txn_id_t txn_id = txn->GetTransactionId();
  LockRequestQueue *queue = &lock_table_[rid];
  auto cur_req = queue->GetIter(txn_id);

  BUSTUB_ASSERT(cur_req != queue->request_queue_.end(), "invariant violated!");

  if (cur_req->valid_) {
    if (cur_req->lock_mode_ == LockMode::EXCLUSIVE) {
      queue->exclusive_ = INVALID_TXN_ID;
    } else {
      --queue->shared_cnt_;
    }
  }
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  /* SHARED lock should be released immediately in READ_COMMITTED isolation level, do not enter SHRINKING phase.*/
  if ((cur_req->lock_mode_ != LockMode::SHARED || txn->GetIsolationLevel() != IsolationLevel::READ_COMMITTED) &&
      txn->GetState() == TransactionState::GROWING) {
    txn->SetState(TransactionState::SHRINKING);
  }

  queue->request_queue_.erase(cur_req);
  queue->cv_.notify_all();

  return true;
}

}  // namespace bustub
