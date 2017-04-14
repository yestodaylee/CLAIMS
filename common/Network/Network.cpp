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
 * /CLAIMS/common/Network/Sockert.cpp
 *
 *  Created on: 2017年3月18日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include <string>
#include "../../common/Network/Network.h"
namespace claims {
namespace common {

RetCode Timer::SetTimeout(unsigned ms) {
  if (fd_ == -1) fd_ = timerfd_create(CLOCK_REALTIME, 0);
  if (fd_ == -1) return rFailure;
  struct timespec interval;
  interval.tv_sec = ms / 1000;
  interval.tv_nsec = (ms % 1000) * 1000 * 1000;
  struct itimerspec timeout_arg;
  timeout_arg.it_interval = interval;
  timeout_arg.it_value = interval;
  if (timerfd_settime(fd_, TFD_TIMER_ABSTIME, &timeout_arg, nullptr) == -1)
    return rFailure;
  timeout_ = ms;
  return rSuccess;
}

Timer::~Timer() {
  if (fd_ != -1) close(fd_);
}
/*******************************/
/*SocketS::SocketS() { cout << "init sockets..." << endl; }*/
/*
RetCode SocketS::Public(int port) {
  cout << "try to public port " << port << endl;
  public_port_ = port;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(public_port_);
  if (bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    cout << "<binding fd:" << fd_ << " failed>" << endl;
    return rFailure;
  }
  if (listen(fd_, 10) < 0) {
    cout << "<listen fd:" << fd_ << ">" << endl;
    return rFailure;
  }
  cout << "success public at port:" << port << endl;
  return rSuccess;
}
RetCode SocketS::Connect(string ip, int port) {
  remote_ip_ = ip;
  remote_port_ = port;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(remote_ip_.c_str());
  addr.sin_port = htons(remote_port_);
  if (connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) <
      0) {
    cout << "<connect fd:" << fd_ << " failed>" << endl;
    return rFailure;
  }
  return rSuccess;
}

RetCode SocketS::SetNonBlocking(int fd) {
  int flag = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flag | O_NONBLOCK);
  return rSuccess;
}

int SocketS::Accept() {
  struct sockaddr_in addr;
  socklen_t len;
  return accept(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len);
}*/

/*********************************/
Socket::Socket() { fd_ = socket(AF_INET, SOCK_STREAM, 0); }
RetCode Socket::Public(int port) {
  // cout << ">> socket try to public port " << port << endl;
  public_port_ = port;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(public_port_);
  if (bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
    cout << "<binding fd:" << fd_ << " failed>" << endl;
    return rFailure;
  }
  if (listen(fd_, 10) < 0) {
    cout << "<listen fd:" << fd_ << ">" << endl;
    return rFailure;
  }
  cout << ">> success public at port:" << port << endl;
  return rSuccess;
}
RetCode Socket::Connect(string ip, int port) {
  remote_ip_ = ip;
  remote_port_ = port;
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(remote_ip_.c_str());
  addr.sin_port = htons(remote_port_);
  // cout << ">> socket try to connect to <" << ip << "," << port << ">" <<
  // endl;
  if (connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) <
      0) {
    cout << "<connect fd:" << fd_ << " failed>" << endl;
    return rFailure;
  }
  cout << ">> socket success to connect to <" << ip << "," << port << ">"
       << endl;
  return rSuccess;
}

RetCode Socket::SetNonBlocking(int fd) {
  int flag = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flag | O_NONBLOCK);
  return rSuccess;
}

int Socket::Accept() {
  struct sockaddr_in addr;
  socklen_t len;
  return accept(fd_, reinterpret_cast<struct sockaddr*>(&addr), &len);
}

Socket::Result Socket::Write(int fd, void* buf, int need_bytes) {
  int finished_bytes = 0;
  int ct = 0;
  while (need_bytes > 0) {
    ++ct;
    // cout << "sending @  ";
    auto send_bytes = write(fd, buf, need_bytes);
    // auto send_bytes = recv(fd, buf, need_bytes, MSG_DONTWAIT);
    // cout << "times " << ct << endl;
    if (send_bytes == 0) return Result(Result::kClose, finished_bytes, ct);
    if (send_bytes < 0 && errno == EAGAIN) {
      // cout << "write remain " << need_bytes << " bytes" << endl;
      return Result(Result::kRemain, finished_bytes, ct);
    }
    finished_bytes += send_bytes;
    buf += send_bytes;
    need_bytes -= send_bytes;
  }
  return Result(Result::kFinish, finished_bytes, ct);
}

Socket::Result Socket::Read(int fd, void* buf, int need_bytes) {
  int finished_bytes = 0;
  int ct = 0;
  // cout << "$$$$$$$ non block read begin " << need_bytes << "$$$$$$$" << endl;
  while (need_bytes > 0) {
    ++ct;
    auto recv_bytes = read(fd, buf, need_bytes);
    // cout << "read " << recv_bytes << " bytes in non_blocking_read" << endl;
    if (recv_bytes == 0) return Result(Result::kClose, finished_bytes, ct);
    if (recv_bytes < 0 && errno == EAGAIN) {
      // cout << "read remain " << need_bytes << " bytes" << endl;
      return Result(Result::kRemain, finished_bytes, ct);
    }
    finished_bytes += recv_bytes;
    buf += recv_bytes;
    need_bytes -= recv_bytes;
  }
  // cout << endl;
  return Result(Result::kFinish, finished_bytes, ct);
}
/*
Socket::Result Socket::Write(int fd, void* buf, int need_bytes) {
  int finished_bytes = 0;
  int ct = 0;
  while (need_bytes > 0) {
    ++ct;
    auto send_bytes = write(fd, buf, need_bytes);
    if (send_bytes == 0) return Result(Result::kClose, finished_bytes, ct);
    if (send_bytes < 0 && errno == EAGAIN) usleep(100);
    finished_bytes += send_bytes;
    buf += send_bytes;
    need_bytes -= send_bytes;
  }
  return Result(Result::kFinish, finished_bytes, ct);
}

Socket::Result Socket::Read(int fd, void* buf, int need_bytes) {
  int finished_bytes = 0;
  int ct = 0;
  while (need_bytes > 0) {
    ++ct;
    auto send_bytes = read(fd, buf, need_bytes);
    if (send_bytes == 0) return Result(Result::kClose, finished_bytes, ct);
    if (send_bytes < 0 && errno == EAGAIN) usleep(100);
    finished_bytes += send_bytes;
    buf += send_bytes;
    need_bytes -= send_bytes;
  }
  return Result(Result::kFinish, finished_bytes, ct);
}
*/

}  // namespace common
}  // namespace claims
