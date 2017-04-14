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
 * /CLAIMS/common/Network/Buffering.cpp
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include <vector>
#include "../../common/Network/Buffering.h"

namespace claims {
namespace common {
/*
void A::f() {}

template <typename T>
void MailBox<T>::Enqueue(const T& item) {
  Mailtem<T>* new_item = new Mailtem<T>(item, nullptr);
  Mailtem<T>* expected, val;
  do {
    expected = stack_tail_.load();
    new_item->next_ = expected;
    val = new_item;
  } while (!stack_tail_.compare_exchange_strong(expected, val));
  count_.fetch_add(1);
}

template <typename T>
bool MailBox<T>::Dequeue(T* item) {
  auto head = cache_head_;
  // cache is not empty
  if (head != nullptr) {
    cache_head_ = head->next_;
    item = head->body_;
    delete head;
    count_.fetch_sub(1);
    return true;
  }
  // cache is empty, fetch from stack_tail
  Mailtem<T>* new_cache;
  do {
    new_cache = stack_tail_.load();
    if (new_cache == nullptr) return false;
  } while (!stack_tail_.compare_exchange_strong(new_cache, nullptr));

  // fill cache by reversing fetched items
  while (new_cache != nullptr) {
    auto next = new_cache->next_;
    new_cache->next_ = next;
    cache_head_ = new_cache;
    new_cache = next;
  }
  return Dequeue(item);
}

template <typename V>
V* ConcurrentMap<V>::Get(uint64_t key) {
  auto index = key % chunk_size_;
  lock_guard<mutex> guard(chunk_mutex_list_[index]);
  auto value = chunk_list_[index].find(key);
  if (value == chunk_list_[index].end())
    return nullptr;
  else
    return &(*value);
}
template <typename V>
void ConcurrentMap<V>::Set(uint64_t key, const V& val) {
  auto index = key % chunk_size_;
  lock_guard<mutex> guard(chunk_mutex_list_[index]);
  chunk_list_[index][key] = val;
}

template <typename V>
void ConcurrentMap<V>::Delete(uint64_t key) {
  auto index = key % chunk_size_;
  lock_guard<mutex> guard(chunk_mutex_list_[index]);
  chunk_list_[index].erase(key);
}*/

}  // namespace common
}  // namespace claims
