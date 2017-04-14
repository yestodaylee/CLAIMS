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
 * /CLAIMS/common/Network/Epoller.cpp
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#include "../../common/Network/Epoller.h"

namespace claims {
namespace common {

RetCode Epoller::Init() {
  epfd_ = epoll_create(kEpollSize);
  alive_event_ =
      static_cast<EpollEvent*>(malloc(sizeof(EpollEvent) * kEpollSize));
  if (epfd_ >= 3 && alive_event_ != nullptr)
    return rSuccess;
  else
    return rFailure;
}

RetCode Epoller::Register(int fd, int event) {
  EpollEvent epoll_event;
  epoll_event.events = event;
  epoll_event.data.fd = fd;
  assert(epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &epoll_event) == 0);
  return rSuccess;
}

RetCode Epoller::Switch(int fd, int event) {
  EpollEvent epoll_event;
  epoll_event.events = event;
  epoll_event.data.fd = fd;
  assert(epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &epoll_event) == 0);
  return rSuccess;
}

RetCode Epoller::Release(int fd) {
  if (epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) >= 0)
    return rSuccess;
  else
    return rFailure;
}

RetCode Epoller::Forward(Epoller* epoller, int fd, int event) {
  return epoller->Register(fd, event);
}

RetCode Epoller::Run() {
  thread t(&Epoller::EventLoop, this);
  t.detach();
  return rSuccess;
}

Epoller::~Epoller() {
  if (epfd_ > 3) close(epfd_);
  free(alive_event_);
}

void Epoller::EventLoop(Epoller* epoller) {
  while (true) {
    int event_cnt =
        epoll_wait(epoller->epfd_, epoller->alive_event_, kEpollSize, -1);
    if (event_cnt < 0) {
      cout << "<event loop fail>!!!" << endl;
      continue;
    }
    for (auto i = 0; i < event_cnt; i++) {
      auto event = epoller->alive_event_[i].events;
      // printf("loop event: %X!!!!!\n", event);
      auto fd = epoller->alive_event_[i].data.fd;
      if ((event & ~(kReadEvent | kWriteEvent))) {
        // cout << "release!!!" << event << endl;
        // cout << (event & ~(kReadEvent | kWriteEvent)) << endl;
        epoller->OnError(fd, event);
        continue;
      } else if (event & kReadEvent) {
        // cout << "loop read!!!" << endl;
        epoller->OnRead(fd);
      } else if (event & kWriteEvent) {
        // cout << "loop write!!!" << endl;
        epoller->OnWrite(fd);
      }
    }
  }
}
}  // namespace common
}  // namespace claims
