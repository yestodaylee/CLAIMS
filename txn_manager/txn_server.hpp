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
 * /txn/txn_server.hpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef TXN_SERVER_HPP_
#define TXN_SERVER_HPP_
#include <vector>
#include <iostream>
#include <functional>
#include <algorithm>
#include <memory>
#include <map>
#include <unordered_map>
#include <set>
#include <utility>
#include <unordered_map>
#include <mutex>
#include <time.h>
#include <stdlib.h>
#include <chrono>
#include <sys/time.h>
#include "unistd.h"
#include "stdlib.h"
#include "caf/all.hpp"
#include "caf/io/all.hpp"
#include "../txn_manager/txn.hpp"
#include "../utility/Timer.h"
//#include "txn_log.hpp"

namespace claims {
namespace txn {
using std::cin;
using std::cout;
using std::endl;
using std::vector;
using std::string;
using std::map;
using std::unordered_map;
using std::pair;
using std::to_string;
using std::function;
using std::sort;
using std::atomic;
using std::mutex;
using std::lock_guard;
using std::chrono::seconds;
using std::chrono::milliseconds;
using std::make_shared;
using std::shared_ptr;
using std::atomic;
// UInt64 txn_id;
class TimeStamp {
 public:
  static UInt64 Init(UInt64 ts) { now_.store(ts); }
  static UInt64 Gen() { return now_.load(); }
  static UInt64 GenAdd() { return now_.fetch_add(1); }
  static UInt64 TSLow(UInt64 ts, int num) { return ts % num; }
  static UInt64 TSHigh(UInt64 ts, int num) { return ts / num; }

 private:
  static atomic<UInt64> now_;
};

class QueryTracker : public caf::event_based_actor {
 public:
  static RetCode Init() {
    tracker_ = caf::spawn<QueryTracker>();
    return rSuccess;
  }
  static RetCode Begin(UInt64 ts) {
    caf::anon_send(tracker_, BeginAtom::value, ts);
  }
  static RetCode Commit(UInt64 ts) {
    caf::anon_send(tracker_, CommitAtom::value, ts);
  }
  caf::behavior make_behavior() override;

 private:
  static caf::actor tracker_;
  static set<UInt64> active_querys_;
};

class CheckpointTracker : public caf::event_based_actor {
 public:
  static RetCode Init() {
    tracker_ = caf::spawn<CheckpointTracker>();
    return rSuccess;
  }
  caf::behavior make_behavior() override;

 private:
  static caf::actor tracker_;
};

class TxnCore : public caf::event_based_actor {
 public:
  UInt64 core_id_;
  map<UInt64, TxnBin> txnbin_list_;
  caf::behavior make_behavior() override;
  TxnCore(int coreId) : core_id_(coreId) {}
};

class TxnServer : public caf::event_based_actor {
 public:
  static bool active_;
  static int port_;
  static int concurrency_;
  static caf::actor proxy_;
  static vector<caf::actor> cores_;
  static unordered_map<UInt64, atomic<UInt64>> pos_list_;
  static unordered_map<UInt64, Checkpoint> cp_list_;
  // static unordered_map<UInt64, atomic<UInt64>> CountList;
  /**************** User APIs ***************/
  static RetCode Init(int concurrency = kConcurrency, int port = kTxnPort);
  static RetCode LoadCPList(UInt64 ts,
                            const unordered_map<UInt64, UInt64>& his_cp_list,
                            const unordered_map<UInt64, UInt64>& rt_cp_list);
  static RetCode LoadPos(const unordered_map<UInt64, UInt64>& pos_list);
  static int GetCoreID(UInt64 ts) { return ts % concurrency_; }
  static string ToString();
  /**************** System APIs ***************/
 private:
  caf::behavior make_behavior() override;
  static unordered_map<UInt64, UInt64> GetHisCPList(
      UInt64 ts, const vector<UInt64>& parts);
  static unordered_map<UInt64, UInt64> GetRtCPList(UInt64 ts,
                                                   const vector<UInt64>& parts);
  static inline Strip AtomicMalloc(UInt64 part, UInt64 TupleSize,
                                   UInt64 TupleCount);
};
}
}

#endif  //  TXN_SERVER_HPP_
