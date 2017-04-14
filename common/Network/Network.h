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
 * /CLAIMS/common/Network/SocketHandle.h
 *
 *  Created on: 2017年3月18日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#ifndef COMMON_NETWORK_NETWORK_H_
#define COMMON_NETWORK_NETWORK_H_
#include <sys/socket.h>
#include <sys/timerfd.h>
#include <time.h>
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include <functional>
#include <thread>
#include <utility>
#include "../common/error_define.h"
using std::cout;
using std::endl;
using std::string;
using std::pair;
namespace claims {
namespace common {

class Timer {
 public:
  RetCode SetTimeout(unsigned ms);
  unsigned GetTimeout() const { return timeout_; }
  int GetFd() const { return fd_; }
  ~Timer();
  int fd_ = -1;
  unsigned timeout_ = 0;
};

class SocketS {
 public:
  /*  SocketS();
    RetCode Public(int port);
    RetCode Connect(string ip, int port);
    int Accept();
    int GetFd() const { return fd_; }
    int GetPublicPort() const { return public_port_; }
    string GetRemoteIP() const { return remote_ip_; }
    int GetRemotePort() const { return remote_port_; }
    ~SocketS() {
      cout << "free socket" << endl;
      if (fd_ > 0) close(fd_);
    }*/

  int fd_ = -1;

  int public_port_;
  string remote_ip_;
  // int remote_port_;
  /*  struct Result {
      Result(int status, int bytes, int ct)
          : status_(status), finished_bytes_(bytes), ct_(ct) {}
      int status_;
      int finished_bytes_;
      int ct_ = 0;
      static const int kFinish = 1;
      static const int kRemain = 2;
      static const int kClose = 3;
    };*/
};

class Socket {
 public:
  Socket();
  RetCode Public(int port);
  RetCode Connect(string ip, int port);
  int Accept();
  int GetFd() const { return fd_; }
  int GetPublicPort() const { return public_port_; }
  string GetRemoteIP() const { return remote_ip_; }
  int GetRemotePort() const { return remote_port_; }
  ~Socket() {
    if (fd_ > 0) close(fd_);
  }

  int fd_ = -1;

  int public_port_;
  string remote_ip_;
  int remote_port_;
  struct Result {
    Result(int status, int bytes, int ct)
        : status_(status), finished_bytes_(bytes), ct_(ct) {}
    int status_;
    int finished_bytes_;
    int ct_ = 0;
    static const int kFinish = 1;
    static const int kRemain = 2;
    static const int kClose = 3;
  };
  RetCode SetNonBlocking() { Socket::SetNonBlocking(fd_); }
  static RetCode SetNonBlocking(int fd);
  static Result Write(int fd, void* buf, int need_bytes);
  static Result Read(int fd, void* buf, int need_bytes);
  // static Result Write(int fd, void* buf, int need_bytes);
  // static Result Read(int fd, void* buf, int need_bytes);
};
}  // namespace common
}  // namespace claims

#endif  //  COMMON_NETWORK_NETWORK_H_
