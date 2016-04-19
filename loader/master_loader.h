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
 * /Claims/loader/master_loader.h
 *
 *  Created on: Apr 7, 2016
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#ifndef LOADER_MASTER_LOADER_H_
#define LOADER_MASTER_LOADER_H_
#include <string>
#include <vector>
#include "../common/error_define.h"
#include "caf/all.hpp"

using caf::behavior;
using caf::event_based_actor;

namespace claims {
namespace loader {

using std::string;
using std::vector;

class MasterLoader {
 public:
  struct NetAddr {
    NetAddr(string ip, int port) : ip_(ip), port_(port) {}
    string ip_;
    int port_;
  };

  struct IngestionRequest {
    string table_name_;
    string col_sep_;
    string row_sep_;
    string tuples_;
  };

 public:
  MasterLoader();
  ~MasterLoader();

  RetCode ConnectWithSlaves();

  RetCode Ingest();

 private:
  string GetMessage();

  RetCode GetRequestFromMessage(const string& message, IngestionRequest* req);

  RetCode GetSlaveNetAddr();
  RetCode SetSocketWithSlaves();
  RetCode GetSocketFdConnectedWithSlave(string ip, int port, int* connected_fd);
  bool CheckValidity();
  void DistributeSubIngestion();

  static behavior ReceiveSlaveReg(event_based_actor* self,
                                  MasterLoader* mloader);

 public:
  static void* StartMasterLoader(void* arg);

 private:
  string master_loader_ip;
  int master_loader_port;
  vector<NetAddr> slave_addrs_;
  vector<int> slave_sockets_;
};

} /* namespace loader */
} /* namespace claims */

#endif  // LOADER_MASTER_LOADER_H_