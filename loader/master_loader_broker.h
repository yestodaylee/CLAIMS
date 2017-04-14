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
 * /CLAIMS/loader/master_loader_network.h
 *
 *  Created on: 2017年3月23日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef LOADER_MASTER_LOADER_BROKER_H_
#define LOADER_MASTER_LOADER_BROKER_H_
#include <stdlib.h>
#include <sys/time.h>
#include <string>
#include <iostream>
#include <memory>
#include <vector>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include "../common/ids.h"
#include "../common/error_define.h"
#include "../common/Network/Network.h"
#include "../common/Network/Epoller.h"
#include "../common/Network/Buffering.h"
#include "../common/Network/Broker.h"
using std::cout;
using std::endl;
using std::vector;
using std::unordered_map;
using std::unordered_set;
using std::make_shared;
using std::shared_ptr;
using std::dynamic_pointer_cast;
using std::queue;
using claims::common::rSuccess;
using claims::common::rFailure;
using claims::common::Epoller;
using claims::common::Socket;
using claims::common::MailBox;
using claims::common::BatchBroker;
using claims::common::TaskPacket;
using claims::common::JobPacket;

namespace claims {
namespace loader {
class MasterLoaderBroker : public BatchBroker {
 public:
  explicit MasterLoaderBroker(NodeID node_id) : BatchBroker(node_id) {}
  RetCode PublicForIngest(int port);
  RetCode OnSendToNode(NodeID node_id, int fd) override;
  RetCode OnRecvFromNode(NodeID node_id, int fd) override;
  static const int kAckBufSize = 128;

 private:
  unordered_map<uint64_t, int> job_ack_list_;
  char ack_buf_[kAckBufSize];
  int ack_buf_remain_cur_ = 0;
};

}  // namespace loader
}  // namespace claims

#endif  //  LOADER_MASTER_LOADER_BROKER_H_
