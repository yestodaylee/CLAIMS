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
 * /CLAIMS/loader/master_loader_network.cpp
 *
 *  Created on: 2017年3月23日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include "../loader/master_loader_broker.h"
namespace claims {
namespace loader {

RetCode MasterLoaderBroker::PublicForIngest(int port) {
  return Public(port, true);
}

RetCode MasterLoaderBroker::OnSendToNode(NodeID node_id, int fd) {
  // send  task packet to each node.
  while (task_queue_list_[node_id].size() > 0) {
    auto& task = task_queue_list_[node_id].front();
    // if (job_ack_list_)
    auto res = Socket::Write(fd, task.payload_ + task.cur_,
                                        task.size_ - task.cur_);
    if (res.status_ == Socket::Result::kFinish) {
      // finished to send a packet
      task_queue_list_[node_id].pop();
      TrySuspendSend(node_id);
    } else if (res.status_ == Socket::Result::kRemain) {
      // kernel buffer is full
      task.cur_ += res.finished_bytes_;
      break;
    } else if (res.status_ == Socket::Result::kClose) {
      LeaveNode(node_id, fd);
      break;
    }
  }
  return rSuccess;
}

RetCode MasterLoaderBroker::OnRecvFromNode(NodeID node_id, int fd) {
  auto res = Socket::Read(fd, ack_buf_ + ack_buf_remain_cur_,
                                     kAckBufSize - ack_buf_remain_cur_);
  if (res.status_ == Socket::Result::kClose) {
    LeaveNode(node_id, fd);
    return rFailure;
  }
  int pos = 0;
  int max_range = res.finished_bytes_ + ack_buf_remain_cur_;
  int max_reable_range = max_range - max_range % sizeof(uint64_t);
  ack_buf_remain_cur_ = max_range - max_reable_range;
  while ((pos + sizeof(uint64_t)) <= max_reable_range) {
    auto id = *reinterpret_cast<uint64_t*>(ack_buf_ + pos);
    pos += sizeof(uint64_t);
    cout << "[ack]:" << id << endl;
    if (run_job_list_.find(id) != run_job_list_.end())
      run_job_list_[id].CommitTask();
  }
  for (auto i = 0; i + max_reable_range < max_range; i++)
    ack_buf_[i] = ack_buf_[i + max_reable_range];
  return rSuccess;
}

}  // namespace loader
}  // namespace claims
