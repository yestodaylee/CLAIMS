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
 * /CLAIMS/loader/slave_loader_network.h
 *
 *  Created on: 2017年3月23日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef LOADER_SLAVE_LOADER_BROKER_H_
#define LOADER_SLAVE_LOADER_BROKER_H_
#include <stdlib.h>
#include <string>
#include <iostream>
#include <memory>
#include <vector>
#include <unordered_map>
#include "../common/error_define.h"
#include "../common/Network/Network.h"
#include "../common/Network/Epoller.h"
#include "../common/Network/Buffering.h"
#include "../common/Network/Broker.h"
#include "../loader/load_packet.h"
using std::cout;
using std::endl;
using std::vector;
using std::unordered_map;
using claims::common::rSuccess;
using claims::common::rFailure;
using claims::common::Epoller;
using claims::common::Socket;
using claims::common::MailBox;
using claims::common::BatchBroker;
using claims::loader::LoadPacket;
using claims::common::JobPacket;
using claims::common::TaskPacket;
namespace claims {
namespace loader {

class SlaveLoaderBroker : public BatchBroker {
 public:
  explicit SlaveLoaderBroker(NodeID node_id) : BatchBroker(node_id) {}
  RetCode ConnectForIngest(string ip, int port);
  RetCode OnSendToNode(NodeID node_id, int fd) override;
  RetCode OnRecvFromNode(NodeID node_id, int fd) override;
};

}  // namespace loader
}  // namespace claims

#endif  //  LOADER_SLAVE_LOADER_BROKER_H_
