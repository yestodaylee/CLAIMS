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
 * /CLAIMS/common/Network/EpollServer.cpp
 *
 *  Created on: 2017年3月17日
 *      Author: imdb
 *		   Email:
 *
 * Description:
 *
 */

#include <error.h>
#include <string>
#include <vector>
#include "../common/Network/Broker.h"
using std::hash;

namespace claims {
namespace common {

RetCode Broker::Public(int port, bool is_blocking) {
  assert(Init() == rSuccess);
  assert(SetTimeout(kTimeout) == rSuccess);
  assert(Run() == rSuccess);
  assert(socket_.Public(port) == rSuccess);
  if (is_blocking) assert(Socket::SetNonBlocking(socket_.GetFd()) == rSuccess);
  assert(Register(socket_.GetFd(), kReadEvent) == rSuccess);
  is_server_ = true;
  return rSuccess;
}

RetCode Broker::Connect(string ip, int port, bool is_blocking) {
  assert(Init() == rSuccess);
  assert(SetTimeout(kTimeout) == rSuccess);
  assert(Run() == rSuccess);
  if (socket_.Connect(ip, port) == rFailure) {
    cout << "connect fail" << endl;
    return rFailure;
  }
  cout << "broker connect to <" << ip << "," << port << ">" << endl;
  /*  auto res =
        Socket::NonBlockingWrite(socket_.GetFd(), &node_id_, sizeof(node_id_));
    if (res.status_ != Socket::Result::kFinish) return rFailure;*/
  // cout << "try to connect" << endl;
  write(socket_.GetFd(), &node_id_, sizeof(node_id_));
  if (is_blocking) assert(Socket::SetNonBlocking(socket_.GetFd()) == rSuccess);
  return rSuccess;
}

RetCode Broker::SetTimeout(unsigned ms) {
  if (timer_.SetTimeout(ms) == rFailure) return rFailure;
  Register(timer_.GetFd(), kReadEvent);
  return rSuccess;
}

vector<NodeID> Broker::GetNodeList() const {
  vector<NodeID> node_list;
  for (auto& node_fd : node_fd_list_) node_list.push_back(node_fd.first);
  return node_list;
}

RetCode Broker::OnRead(int fd) {
  // new connection
  if (is_server_ && fd == socket_.GetFd()) {
    auto con_fd = socket_.Accept();
    Socket::SetNonBlocking(con_fd);
    return Register(con_fd, kReadEvent);
  }
  // timeout
  if (fd == timer_.GetFd()) return OnTimeout(fd);

  // register node_id
  if (is_server_ && !IsFdExist(fd)) {
    // cout << "new node try register" << endl;
    NodeID node_id;
    /*
        auto res = Socket::NonBlockingRead(fd, &node_id, sizeof(NodeID));
        if (res.status_ == Socket::Result::kFinish) {
          cout << "register " << node_id << " success" << endl;
          EnterNode(node_id, fd);
        } else {
          cout << "register fail" << endl;
        }
    */
    read(fd, &node_id, sizeof(node_id));
    EnterNode(node_id, fd);
    return rSuccess;
  }
  // receive data from node_id
  if (is_server_) {
    // cout << "1" << endl;
    return OnRecvFromNode(fd_node_list_[fd], fd);
  } else {
    // cout << "2" << endl;
    return OnRecvFromNode(kServerID, fd);
  }
}

RetCode Broker::OnWrite(int fd) {
  // cout << "3" << endl;
  NodeID node_id = fd_node_list_[fd];
  return OnSendToNode(node_id, fd);
}

RetCode Broker::OnError(int fd, int event) {
  cout << "error!!!" << event << endl;
  if (fd != socket_.GetFd()) LeaveNode(fd_node_list_[fd], fd);
  return rSuccess;
}

void Broker::EnterNode(NodeID node_id, int fd) {
  node_fd_list_[node_id] = fd;
  fd_node_list_[fd] = node_id;
  suspend_node_list_.insert(node_id);
  cout << "Node[" << node_id << "] register broker ..." << endl;
}

void Broker::LeaveNode(NodeID node_id, int fd) {
  // assert(false);
  node_fd_list_.erase(node_id);
  fd_node_list_.erase(fd);
  suspend_node_list_.erase(node_id);
  Release(fd);  // release epoll register
  close(fd);    // close fd
  cout << "Node[" << node_id << "] leave broker ..." << endl;
}

bool Broker::IsNodeExist(NodeID node_id) const {
  return node_fd_list_.find(node_id) != node_fd_list_.end();
}

bool Broker::IsFdExist(int fd) const {
  return fd_node_list_.find(fd) != fd_node_list_.end();
}

void BatchBroker::Schedule() {
  JobPacket job;
  while (mailbox_.Dequeue(&job)) {
    run_job_list_[job.id_] = job;
    for (auto& task : job.task_list_) task_queue_list_[task.desc_].push(task);
  }
  /*  for (auto& node_queue : node_queue_list_) {
      cout << "Node[" << node_queue.first << "] size:" <<
    node_queue.second.size()
           << endl;
    }*/
}

void BatchBroker::AddJob(const JobPacket& packet) { mailbox_.Enqueue(packet); }

RetCode BatchBroker::OnTimeout(int timer_fd) {
  uint64_t times;
  read(timer_fd, &times, sizeof(times));
  // cout << "before schedule" << endl;
  Schedule();
  for (auto& node_fd : node_fd_list_) TryWokeupSend(node_fd.first);
  // cout << "after schedule" << endl;
  /*  cout << "[having send:] " << send_bytes_ / (1024.0 * 1024.0) << " MB" <<
    endl;
    cout << "[having prod:] " << produce_bytes_ / (1024.0 * 1024.0) << " MB"
         << endl;
    cout << "[having prod:] " << produce_ct_ << " counts" << endl;*/
  return rSuccess;
}

void BatchBroker::TrySuspendSend(NodeID node_id) {
  // cout << "try to suspenSend" << endl;
  if (task_queue_list_[node_id].size() == 0) {
    Switch(node_fd_list_[node_id], kReadEvent);
    suspend_node_list_.insert(node_id);
  }
}

void BatchBroker::TryWokeupSend(NodeID node_id) {
  if (node_fd_list_.find(node_id) != node_fd_list_.end() &&
      suspend_node_list_.find(node_id) != suspend_node_list_.end() &&
      task_queue_list_[node_id].size() > 0) {
    // cout << "woke up send to node " << node_id << endl;
    Switch(node_fd_list_[node_id], kReadEvent | kWriteEvent);
    suspend_node_list_.erase(node_id);
  }
}

}  // namespace common
}  // namespace claims
