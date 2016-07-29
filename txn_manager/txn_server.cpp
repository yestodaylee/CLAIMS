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
 * /txn/txn_server.cpp
 *
 *  Created on: 2016年4月10日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include "txn_server.hpp"

#include "caf/all.hpp"
#include "txn_log.hpp"

using caf::aout;
namespace claims {
namespace txn {
// using claims::common::rSuccess;
// using claims::common::rLinkTmTimeout;
// using claims::common::rLinkTmFail;
// using claims::common::rBeginIngestTxnFail;
// using claims::common::rBeginQueryFail;
// using claims::common::rBeginCheckpointFail;
// using claims::common::rCommitIngestTxnFail;
// using claims::common::rAbortIngestTxnFail;
// using claims::common::rCommitCheckpointFail;

/***************/
atomic<UInt64> TimeStamp::now_;
caf::actor QueryTracker::tracker_;
set<UInt64> QueryTracker::active_querys_;

/**************/

int TxnServer::port_ = kTxnPort;
int TxnServer::concurrency_ = kConcurrency;
caf::actor TxnServer::proxy_;
vector<caf::actor> TxnServer::cores_;
bool TxnServer::active_ = false;

unordered_map<UInt64, atomic<UInt64>> TxnServer::pos_list_;
unordered_map<UInt64, Checkpoint> TxnServer::cp_list_;
caf::actor test;

caf::behavior QueryTracker::make_behavior() {
  this->delayed_send(this, seconds(3), TimerAtom::value);
  return {[](BeginAtom, UInt64 ts) { active_querys_.insert(ts); },
          [](CommitAtom, UInt64 ts) { active_querys_.erase(ts); },
          [this](TimerAtom) {
            /**
             * TODO broadcast all components what minimum timestamp
             *   is still alive.
             */
            this->delayed_send(this, seconds(3), TimerAtom::value);
          }};
}

caf::behavior CheckpointTracker::make_behavior() {}

caf::behavior TxnCore::make_behavior() {
  // this->delayed_send(this, seconds(kGCTime + CoreId), GCAtom::value);
  return {
      [this](IngestAtom, shared_ptr<Ingest> ingest) -> caf::message {
        RetCode ret = rSuccess;
        // cout << "begin ingestion" << endl << ingest->ToString() << endl;
        auto id = TxnBin::GetTxnBinID(ingest->ts_, TxnServer::concurrency_);
        auto pos = TxnBin::GetTxnBinPos(ingest->ts_, TxnServer::concurrency_);
        txnbin_list_[id].SetTxn(pos, ingest->strip_list_);
        return caf::make_message(ret, *ingest);
      },
      [this](CommitIngestAtom, const UInt64 ts) -> caf::message {
        // cout << "commit:" << ts << endl;
        RetCode ret = rSuccess;
        auto id = TxnBin::GetTxnBinID(ts, TxnServer::concurrency_);
        auto pos = TxnBin::GetTxnBinPos(ts, TxnServer::concurrency_);
        txnbin_list_[id].CommitTxn(pos);
        return caf::make_message(rSuccess);
      },
      [this](AbortIngestAtom, const UInt64 ts) -> caf::message {
        cout << "abort:" << ts << endl;
        RetCode ret = rSuccess;
        auto id = TxnBin::GetTxnBinID(ts, TxnServer::concurrency_);
        auto pos = TxnBin::GetTxnBinPos(ts, TxnServer::concurrency_);
        txnbin_list_[id].AbortTxn(pos);
        return caf::make_message(rSuccess);
      },
      [](QueryAtom, const QueryReq& request, shared_ptr<Query> query) {

      },
      [](MergeAtom, const QueryReq& request, shared_ptr<Query> query) {

      },
      [](MergeAtom, shared_ptr<Checkpoint> cp) {

      },
      [](CheckpointAtom, shared_ptr<Checkpoint> cp) {
        /*        for (auto i = 0; i < size_; i++)
                  if (commit_[i]) {
                    for (auto& strip : strip_list_[i])
                      if (strip.part_ == cp->part_ && strip.pos_ >=
           cp->logic_cp_)
                        cp->commit_strip_list_.push_back(
                            PStrip(strip.pos_, strip.offset_));
                  } else if (abort_[i]) {
                    for (auto& strip : strip_list_[i])
                      if (strip.part_ == cp->part_ && strip.pos_ >=
           cp->logic_cp_)
                        cp->abort_strip_list_.push_back(
                            PStrip(strip.pos_, strip.offset_));
                  }
                if (core_id_ != TxnServer::cores_.size() - 1)
                  this->forward_to(TxnServer::cores_[core_id_ + 1]);
                else {
                  current_message() = caf::make_message(MergeAtom::value, cp);
                  this->forward_to(TxnServer::cores_[TxnServer::SelectCoreId()]);
                }*/
      },
      [](GCAtom) {
        /*        auto size_old = size_;
                auto pos = 0;
                for (auto i = 0; i < size_; i++)
                  if (!TxnServer::IsStripListGarbage(strip_list_[i])) {
                    txn_index_[txn_index_[i]] = pos;
                    commit_[pos] = commit_[i];
                    abort_[pos] = abort_[i];
                    strip_list_[pos] = strip_list_[i];
                    ++pos;
                  }
                size_ = pos;
                cout << "core:" << core_id_ << ",gc:" << size_old << "=>" << pos
                     << endl;*/
        // this->delayed_send(this, seconds(kGCTime), GCAtom::value);
      },
      caf::others >>
          [&]() { cout << "core:" << core_id_ << " unkown message" << endl; }};
}

caf::behavior TxnServer::make_behavior() {
  try {
    caf::io::publish(proxy_, port_, nullptr, true);
    cout << "txn server bind to port:" << port_ << " success" << endl;
  } catch (...) {
    cout << "txn server bind to port:" << port_ << " fail" << endl;
  }
  // this->delayed_send(this, seconds(3), CheckpointAtom::value);
  // this->delayed_send(this, seconds(3));
  return {[this](IngestAtom, const FixTupleIngestReq& request) {
            auto ts = TimeStamp::GenAdd();
            auto ingest = make_shared<Ingest>(request.content_, ts);
            for (auto& part : ingest->strip_list_)
              ingest->InsertStrip(AtomicMalloc(part.first, part.second.first,
                                               part.second.second));
            current_message() = caf::make_message(IngestAtom::value, ingest);
            forward_to(cores_[GetCoreID(ts)]);
          },
          [this](CommitIngestAtom,
                 const UInt64 ts) { forward_to(cores_[GetCoreID(ts)]); },
          [this](AbortIngestAtom,
                 const UInt64 ts) { forward_to(cores_[GetCoreID(ts)]); },
          [this](QueryAtom, const QueryReq& request) {
            auto ts = TimeStamp::Gen();
            auto query =
                make_shared<Query>(ts, GetHisCPList(ts, request.part_list_),
                                   GetRtCPList(ts, request.part_list_));
            current_message() =
                caf::make_message(QueryAtom::value, request, query);
            forward_to(cores_[GetCoreID(ts)]);
          },
          [this](CommitCPAtom, UInt64 ts, UInt64 part, UInt64 his_cp,
                 UInt64 rt_cp) -> caf::message {
            cp_list_[part].SetHisCP(ts, his_cp);
            cp_list_[part].SetRtCP(ts, rt_cp);
            return caf::make_message(OkAtom::value);
          },
          caf::others >> [this]() {
                           cout << "server unkown message:"
                                << to_string(current_message()) << endl;
                         }};
}

RetCode TxnServer::Init(int concurrency, int port) {
  active_ = true;
  concurrency_ = concurrency;
  port_ = port;
  proxy_ = caf::spawn<TxnServer>();
  for (auto i = 0; i < concurrency_; i++)
    cores_.push_back(caf::spawn<TxnCore, caf::detached>(i));
  CAFSerConfig();
  // RecoveryCheckpoint();
  // RecoveryFromTxnLog();
  // srand((unsigned)time(NULL));
  return 0;
}

Strip TxnServer::AtomicMalloc(UInt64 part, UInt64 TupleSize,
                              UInt64 TupleCount) {
  Strip strip;
  strip.part_ = part;
  if (TupleSize * TupleCount == 0) return strip;
  do {
    strip.pos_ = pos_list_[part].load();
    strip.offset_ = 0;
    UInt64 block_pos = strip.pos_ % kBlockSize;
    UInt64 remain_count = TupleCount;
    int count = 0;
    while (remain_count > 0) {
      // 求出一个块内可以存放的最多元组数
      UInt64 use_count = (kBlockSize - block_pos - kTailSize) / TupleSize;
      if (use_count > remain_count) use_count = remain_count;

      //使用块内可用区域
      remain_count -= use_count;
      strip.offset_ += use_count * TupleSize;
      block_pos += use_count * TupleSize;
      //将不可利用的空间也分配
      if (kBlockSize - block_pos - kTailSize < TupleSize) {
        strip.offset_ += kBlockSize - block_pos;
        block_pos = 0;
      }
    }
  } while (!pos_list_[part].compare_exchange_weak(strip.pos_,
                                                  strip.pos_ + strip.offset_));
  return strip;
}

unordered_map<UInt64, UInt64> TxnServer::GetHisCPList(
    UInt64 ts, const vector<UInt64>& parts) {
  unordered_map<UInt64, UInt64> cps;
  for (auto& part : parts) cps[part] = cp_list_[part].GetHisCP(ts);
  return cps;
}

unordered_map<UInt64, UInt64> TxnServer::GetRtCPList(
    UInt64 ts, const vector<UInt64>& parts) {
  unordered_map<UInt64, UInt64> cps;
  for (auto& part : parts) cps[part] = cp_list_[part].GetRtCP(ts);
  return cps;
}

RetCode TxnServer::LoadCPList(UInt64 ts,
                              const unordered_map<UInt64, UInt64>& his_cp_list,
                              const unordered_map<UInt64, UInt64>& rt_cp_list) {
  for (auto& cp : his_cp_list) cp_list_[cp.first].SetHisCP(ts, cp.second);
  for (auto& cp : rt_cp_list) cp_list_[cp.first].SetRtCP(ts, cp.second);
  return rSuccess;
}

RetCode TxnServer::LoadPos(const unordered_map<UInt64, UInt64>& pos_list) {
  for (auto& pos : pos_list) pos_list_[pos.first].store(pos.second);
  return rSuccess;
}

string TxnServer::ToString() {}
}
}
