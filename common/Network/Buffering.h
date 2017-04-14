/*
 * Copyright [2012-2015] DaSE@ECNU
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * /CLAIMS/common/Network/Buffering.h
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef COMMON_NETWORK_BUFFERING_H_
#define COMMON_NETWORK_BUFFERING_H_
#include <iostream>
#include <memory>
#include <unordered_map>
#include <vector>
#include <atomic>
#include <functional>
#include <thread>
#include <mutex>
#include "../../common/error_define.h"

#define LOCK_FREE

using std::vector;
using std::unordered_map;
using std::atomic;
using std::mutex;
using std::lock_guard;
using std::cout;
using std::endl;
using claims::common::rSuccess;
using claims::common::rFailure;

namespace claims {
namespace common {

template <typename T>
struct MailItem {
  MailItem(T body, MailItem* next) : body_(body), next_(next) {}
  T body_;
  MailItem* next_ = nullptr;
  ~MailItem() {}
  static MailItem<T> Gen(T body, MailItem* next) {
    return new MailItem(body, next);
  }
};

template <typename T>
class MailBox {
 public:
  MailBox() {
#ifdef LOCK_FREE
    stack_tail_.store(nullptr);
#else
    stack_tail_ = nullptr;
#endif
  }

  // multiply-thread safety
  void Enqueue(const T& item) {
    MailItem<T>* new_item = new MailItem<T>(item, nullptr);
    MailItem<T>* expected;
#ifdef LOCK_FREE
    do {
      expected = stack_tail_.load();
      new_item->next_ = expected;
    } while (!stack_tail_.compare_exchange_strong(expected, new_item));
    count_.fetch_add(1);
// cout << "enqueue=> current stack_tail:" << stack_tail_ << endl;
#else
    lock_guard<mutex> guard(lock_);
    new_item->next_ = stack_tail_;
    stack_tail_ = new_item;
    count_++;
#endif
  }
  // single-thread safety
  bool Dequeue(T* item) {
    auto head = cache_head_;
    // cache is not empty
    if (head != nullptr) {
      cache_head_ = head->next_;
      *item = head->body_;
      delete head;
      count_.fetch_sub(1);
      return true;
    }
    // cache is empty, fetch from stack_tail
    MailItem<T>* new_cache;
#ifdef LOCK_FREE
    do {
      new_cache = stack_tail_.load();
      // cout << "dequeue=> new_cache:" << new_cache << endl;
      if (new_cache == nullptr) {
        return false;
      }
    } while (!stack_tail_.compare_exchange_strong(new_cache, nullptr));
#else
    lock_guard<mutex> guard(lock_);
    if (stack_tail_ == nullptr) return false;
    new_cache = stack_tail_;
#endif

    // fill cache by reversing fetched items
    while (new_cache != nullptr) {
      auto next = new_cache->next_;
      new_cache->next_ = cache_head_;
      cache_head_ = new_cache;
      new_cache = next;
    }
    return Dequeue(item);
  }
  bool Top(T*& item) {
    auto head = cache_head_;
    // cache is not empty
    if (head != nullptr) {
      item = head;
      return true;
    }
    // cache is empty, fetch from stack_tail
    MailItem<T>* new_cache;
#ifdef LOCK_FREE
    do {
      new_cache = stack_tail_.load();
      // cout << "dequeue=> new_cache:" << new_cache << endl;
      if (new_cache == nullptr) {
        return false;
      }
    } while (!stack_tail_.compare_exchange_strong(new_cache, nullptr));
#else
    lock_guard<mutex> guard(lock_);
    if (stack_tail_ == nullptr) return false;
    new_cache = stack_tail_;
#endif

    // fill cache by reversing fetched items
    while (new_cache != nullptr) {
      auto next = new_cache->next_;
      new_cache->next_ = cache_head_;
      cache_head_ = new_cache;
      new_cache = next;
    }
    return Dequeue(item);
  }

  // return count of mailbox
  unsigned GetCount() { return count_.load(); }

 private:
#ifdef LOCK_FREE
  atomic<unsigned> count_;
  atomic<MailItem<T>*> stack_tail_;
#else
  unsigned count_ = 0;
  mutex lock_;
  MailItem<T>* stack_tail_ = nullptr;
#endif
  MailItem<T>* cache_head_ = nullptr;
};

template <typename V>
class ConcurrentMap {
 public:
  static const unsigned kChunkSize = 1009;
  explicit ConcurrentMap(unsigned chunk_size = kChunkSize)
      : chunk_size_(chunk_size) {
    chunk_list_.resize(chunk_size_);
    chunk_mutex_list_.resize(chunk_size_);
  }
  V* Get(uint64_t key) {
    auto index = key % chunk_size_;
    lock_guard<mutex> guard(chunk_mutex_list_[index]);
    auto value = chunk_list_[index].find(key);
    if (value == chunk_list_[index].end())
      return nullptr;
    else
      return &(*value);
  }
  void Set(uint64_t key, const V& val) {
    auto index = key % chunk_size_;
    lock_guard<mutex> guard(chunk_mutex_list_[index]);
    chunk_list_[index][key] = val;
  }
  void Delete(uint64_t key) {
    auto index = key % chunk_size_;
    lock_guard<mutex> guard(chunk_mutex_list_[index]);
    chunk_list_[index].erase(key);
  }

 private:
  unsigned chunk_size_;
  vector<unordered_map<uint64_t, V>> chunk_list_;
  vector<mutex> chunk_mutex_list_;
};

}  // namespace common
}  // namespace claims

#endif  //  COMMON_NETWORK_BUFFERING_H_
