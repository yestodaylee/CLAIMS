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
 * /CLAIMS/common/Network/Epoller.h
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef COMMON_NETWORK_EPOLLER_H_
#define COMMON_NETWORK_EPOLLER_H_
#include <sys/epoll.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <iostream>
#include <functional>
#include <thread>
#include "../common/error_define.h"
#include "../../common/Network/Network.h"
using std::cout;
using std::endl;
using std::thread;
using claims::common::rSuccess;
using claims::common::rFailure;
using EpollEvent = struct epoll_event;

namespace claims {
namespace common {
class Epoller {
 public:
  Epoller() {}
  RetCode Init();
  RetCode Run();
  // never call blocking function in
  //  "OnRead","OnWrite","OnIdle"
  virtual RetCode OnRead(int fd) = 0;
  virtual RetCode OnWrite(int fd) = 0;
  virtual RetCode OnError(int fd, int event) = 0;
  virtual ~Epoller();
  static const int kEpollSize = 10240;
  static const int kReadEvent = EPOLLIN;
  static const int kWriteEvent = EPOLLOUT;
  static void EventLoop(Epoller* epoller);

 protected:
  RetCode Register(int fd, int event);
  RetCode Switch(int fd, int event);
  RetCode Release(int fd);
  RetCode Forward(Epoller* epoller, int fd, int event);

 private:
  int epfd_ = -1;
  EpollEvent* alive_event_ = nullptr;
  Timer timer_;
};
}  // namespace common
}  // namespace claims

#endif  //  COMMON_NETWORK_EPOLLER_H_
