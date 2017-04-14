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
 * /CLAIMS/common/Network/EpollServer.h
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef COMMON_NETWORK_BROKER_H_
#define COMMON_NETWORK_BROKER_H_

#include <stdlib.h>
#include <sys/time.h>
#include <string>
#include <iostream>
#include <memory>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <functional>
#include "../common/ids.h"
#include "../common/error_define.h"
#include "../common/Network/Network.h"
#include "../common/Network/Epoller.h"
#include "../common/Network/Buffering.h"
using std::cout;
using std::endl;
using std::vector;
using std::queue;
using std::unordered_map;
using std::unordered_set;
using std::make_shared;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::function;
// using NodeID;
using claims::common::rSuccess;
using claims::common::rFailure;
using claims::common::Epoller;
using claims::common::Socket;
using claims::common::MailBox;
using claims::common::Timer;
namespace claims {
namespace common {

class Broker : public Epoller {
 public:
  Broker() {}
  explicit Broker(NodeID node_id) : node_id_(node_id) {}
  RetCode Public(int port, bool is_blocking);
  RetCode Connect(string ip, int port, bool is_blocking);
  RetCode SetTimeout(unsigned ms);
  vector<NodeID> GetNodeList() const;
  virtual ~Broker() {}
  virtual RetCode OnSendToNode(NodeID node_id, int fd) = 0;
  virtual RetCode OnRecvFromNode(NodeID node_id, int fd) = 0;
  virtual RetCode OnTimeout(int timer_fd) = 0;
  static const int kTimeout = 1000;

 protected:
  RetCode OnRead(int fd) override;
  RetCode OnWrite(int fd) override;
  RetCode OnError(int fd, int event) override;
  void EnterNode(NodeID node_id, int fd);
  void LeaveNode(NodeID node_id, int fd);
  bool IsNodeExist(NodeID node_id) const;
  bool IsFdExist(int fd) const;

  NodeID node_id_ = -1;
  static const int kServerID = -1;
  Socket socket_;
  unordered_map<NodeID, int> node_fd_list_;
  unordered_map<int, NodeID> fd_node_list_;
  unordered_set<NodeID> suspend_node_list_;
  Timer timer_;
  bool is_server_ = false;
};

struct TaskPacket {
  TaskPacket(NodeID desc, void* payload, uint64_t size)
      : desc_(desc), payload_(payload), size_(size) {}
  NodeID desc_;
  void* payload_;
  uint64_t size_;
  uint64_t cur_ = 0;
};

struct JobPacket {
  JobPacket() {}
  JobPacket(uint64_t id, const vector<TaskPacket>& task_list,
            function<RetCode()> callback)
      : id_(id), task_list_(task_list), callback_(callback) {}
  void CommitTask() {
    if (++ack_ct_ == task_list_.size()) callback_();
  }
  uint64_t id_;
  vector<TaskPacket> task_list_;
  function<RetCode()> callback_;
  int ack_ct_ = 0;
};

class BatchBroker : public Broker {
 public:
  BatchBroker() {}
  explicit BatchBroker(NodeID node_id) : Broker(node_id) {}
  virtual ~BatchBroker() {}
  void AddJob(const JobPacket& packet);
  uint64_t send_bytes_ = 0;
  uint64_t produce_bytes_ = 0;
  uint64_t produce_ct_ = 0;

 protected:
  RetCode OnTimeout(int timer_fd) override;
  void TrySuspendSend(NodeID node_id);
  void TryWokeupSend(NodeID node_id);
  void Schedule();
  MailBox<JobPacket> mailbox_;
  unordered_map<NodeID, queue<TaskPacket> > task_queue_list_;
  unordered_map<uint64_t, JobPacket> run_job_list_;
};

}  // namespace common
}  // namespace claims
#endif  //  COMMON_NETWORK_BROKER_H_
