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
 * /CLAIMS/common/Network/network_Test.cpp
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */
#include <sys/time.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../../common/Network/Network.h"
#include "../../common/Network/Buffering.h"
#include "../../common/Network/Epoller.h"
#include "../../common/Network/Broker.h"
using std::vector;
using std::cout;
using std::endl;
using std::thread;
using std::mutex;
using std::lock_guard;
using claims::common::MailBox;
using claims::common::Epoller;
using claims::common::Socket;
using claims::common::BatchBroker;
using claims::common::JobPacket;
using claims::common::TaskPacket;
// #define QUEUE_BENCH
#define EPOLL_TEST
const int BUFSIZE = 4 * 1024;
// char send_buf[BUFSIZE];
char recv_buf[BUFSIZE];
char ack_buf[BUFSIZE] = {'7'};
class Server : public BatchBroker {
 public:
  Server(NodeID node_id, int port) : BatchBroker(node_id), ct_(0) {
    Public(port, true);
  }
  RetCode OnSendToNode(NodeID node_id, int fd) override {
    /*    cout << "try to send when queue size is "
             << node_queue_list_[node_id].size() << endl;*/
    while (task_queue_list_[node_id].size() > 0) {
      auto& task = task_queue_list_[node_id].front();
      auto res = Socket::Write(fd, task.payload_ + task.cur_,
                                           task.size_ - task.cur_);
      send_bytes_ += res.finished_bytes_;
      if (res.status_ == Socket::Result::kRemain | task.cur_ > 0)
        cout << "send to node[" << node_id << "]  " << task.cur_ << " => "
             << task.cur_ + res.finished_bytes_ << endl;
      if (res.status_ == Socket::Result::kFinish) {
        free(task.payload_);
        task_queue_list_[node_id].pop();
        TrySuspendSend(node_id);
      } else if (res.status_ == Socket::Result::kRemain) {
        // cout << "update "<< &node_queue_list_[node_id].front()<<
        task.cur_ += res.finished_bytes_;

        break;
      } else if (res.status_ == Socket::Result::kClose) {
        LeaveNode(node_id, fd);
        break;
      }
    }

    return rSuccess;
  }
  RetCode OnRecvFromNode(NodeID node_id, int fd) override {
    // cout << " recv ";
    auto res = Socket::Read(fd, ack_buf, sizeof(int) + 1);
    /*   cout << "recv:" << res.finished_bytes_ << " bytes" << endl;
       cout << "ack:" << *reinterpret_cast<int*>(ack_buf) << endl << endl;*/

    if (res.status_ == Socket::Result::kClose) {
      LeaveNode(node_id, fd);
      return rFailure;
    }

    return rSuccess;
  }
  NodeID node_id_;

  int ct_;
};

class Client : public BatchBroker {
  int ct = 0;

 public:
  Client(NodeID node_id, string ip, int port) : BatchBroker(node_id) {
    Connect(ip, port, false);
    Register(socket_.GetFd(), kReadEvent);
    EnterNode(kServerID, socket_.GetFd());
  }

  RetCode OnSendToNode(NodeID node_id, int fd) override {
    while (task_queue_list_[node_id].size() > 0) {
      /*      cout << "try to send ack to " << node_id << ", "
                 << (socket_.GetFd() == fd) << endl;*/
      auto task = task_queue_list_[node_id].front();

      auto res = Socket::Write(fd, task.payload_, task.size_);

      // cout << "send " << res.finished_bytes_ << " bytes" << endl;
      if (res.status_ == Socket::Result::kRemain) {
        break;
      } else if (res.status_ == Socket::Result::kClose) {
        LeaveNode(node_id, fd);
        break;
      }
      // free(task.payload_);
      task_queue_list_[node_id].pop();
    }
    TrySuspendSend(node_id);
    return rSuccess;
  }

  RetCode OnRecvFromNode(NodeID node_id, int fd) override {
    auto res = Socket::Read(fd, recv_buf, BUFSIZE);
    if (res.status_ == Socket::Result::kClose) {
      LeaveNode(node_id, fd);
      return rFailure;
    }
    auto ack = *reinterpret_cast<int*>(recv_buf);
    // cout << " recv " << res.finished_bytes_ << " bytes" << endl;
    /* cout << "send ack:" << ack << "," << *(ack_ + ack % 12) << "," << ack %
       12
          << "," << (ack_ + ack % 12) << endl;*/

    vector<TaskPacket> task_list = {
        TaskPacket(node_id, ack_ + ack % 12, sizeof(int))};
    JobPacket jobpacket(ack, task_list, []() -> RetCode { return 1; });
    AddJob(jobpacket);
    return rSuccess;
  }
  int ack_[12] = {10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0, -1};
};
uint64_t produce_bytes = 0;
int main(int argc, char* argv[]) {
  vector<string> cmd;
  for (auto i = 1; i < argc; i++) cmd.push_back(string(argv[i]));

  uint64_t count = 0;
  struct timeval start, end;
  gettimeofday(&start, nullptr);

#ifdef QUEUE_BENCH
  vector<thread> tasks;
  for (auto i = 0; i < thread_ct; i++) tasks.push_back(thread(task_euqueue));
  for (auto& task : tasks) task.join();
  count = task_ct * thread_ct;
#endif

#ifdef EPOLL_TEST
  if (cmd[0] == "server") {
    Server server(stoi(cmd[1]), stoi(cmd[2]));
    int ct = 0;
    sleep(3);
    while (true) {
      if (ct % 1000 != 0) {
        JobPacket job;
        auto node_list = server.GetNodeList();
        // cout << "alive node number is " << node_list.size() << endl;

        vector<TaskPacket> task_list;
        for (auto& node : node_list) {
          int* buf = (int*)malloc(BUFSIZE);
          server.produce_bytes_ += BUFSIZE;
          buf[0] = ct;
          task_list.push_back(TaskPacket(node, buf, BUFSIZE));
        }
        auto call_back = []() -> RetCode { return 1; };
        server.AddJob(JobPacket(ct, task_list, call_back));

      } else
        usleep(1000 * 20);
      ++ct;
    }
  } else if (cmd[0] == "client") {
    Client client(stoi(cmd[1]), cmd[2], stoi(cmd[3]));
    while (true) sleep(1);
  }
#endif

  gettimeofday(&end, nullptr);

  double start_msec = start.tv_sec * 1000 + start.tv_usec / 1000;
  double end_msec = end.tv_sec * 1000 + end.tv_usec / 1000;
  cout << "total time in second: " << (end_msec - start_msec) / 1000.0 << endl;
  cout << "throughput:" << (count / 1000) / (end_msec - start_msec)
       << " M per second" << endl;
  // tasks.push_back(thread(task_dequeue));
}
