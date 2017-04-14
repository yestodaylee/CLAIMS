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
 * /CLAIMS/loader/slave_loader_network.cpp
 *
 *  Created on: 2017年3月23日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include <string>
#include "../loader/slave_loader_broker.h"
#include "../txn_manager/wa_log_impl.h"
using claims::txn::BinaryValueLog;
namespace claims {
namespace loader {

RetCode SlaveLoaderBroker::ConnectForIngest(string ip, int port) {
  return Connect(ip, port, false);
}

RetCode SlaveLoaderBroker::OnSendToNode(NodeID node_id, int fd) {
  while (task_queue_list_[node_id].size() > 0) {
    auto task = task_queue_list_[node_id].front();

    auto res = Socket::Write(fd, task.payload_, task.size_);
    if (res.status_ == Socket::Result::kFinish) {
      // finished to send a packet
      task_queue_list_[node_id].pop();
    } else if (res.status_ == Socket::Result::kClose) {
      LeaveNode(node_id, fd);
    }
  }
  TrySuspendSend(node_id);
}

RetCode SlaveLoaderBroker::OnRecvFromNode(NodeID node_id, int fd) {
  return rSuccess;
}

}  // namespace loader
}  // namespace claims
