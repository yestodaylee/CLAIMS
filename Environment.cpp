/*
 * Environment.cpp
 *
 *  Created on: Aug 10, 2013
 *      Author: wangli
 */

#include "Environment.h"
#include <assert.h>
#include <libconfig.h++>

#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#undef GLOG_NO_ABBREVIATED_SEVERITIES
#include <map>
#include <utility>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>  // NOLINT
#include "caf/all.hpp"
#include "common/Message.h"
#include "exec_tracker/stmt_exec_tracker.h"
#include "exec_tracker/segment_exec_tracker.h"
#include "node_manager/base_node.h"
#include "loader/load_packet.h"
#include "loader/master_loader.h"
#include "loader/slave_loader.h"
#include "./Debug.h"
#include "./Config.h"
#include "common/ids.h"
#include "common/Logging.h"
#include "common/TypePromotionMap.h"
#include "common/TypeCast.h"
#include "common/error_define.h"
#include "codegen/CodeGenerator.h"
#include "common/expression/data_type_oper.h"
#include "common/expression/expr_type_cast.h"
#include "common/expression/type_conversion_matrix.h"
// #define DEBUG_MODE
#include "catalog/catalog.h"
#include "txn_manager/txn_server.hpp"
#include "txn_manager/txn_client.hpp"
#include "txn_manager/txn_log.hpp"
#include "txn_manager/txn.hpp"

using caf::announce;
using claims::BaseNode;
using claims::catalog::Catalog;
using claims::common::InitAggAvgDivide;
using claims::common::InitOperatorFunc;
using claims::common::InitTypeCastFunc;
using claims::common::InitTypeConversionMatrix;
using claims::common::rSuccess;
using claims::loader::LoadPacket;
using claims::loader::MasterLoader;
using claims::loader::SlaveLoader;
using claims::txn::TxnServer;
using claims::txn::TxnClient;
using claims::txn::LogServer;
using claims::txn::LogClient;
using claims::txn::GetGlobalPartId;
using claims::NodeAddr;
using claims::NodeSegmentID;
using claims::StmtExecTracker;

Environment* Environment::_instance = 0;

Environment::Environment(bool ismaster) : ismaster_(ismaster) {
  _instance = this;
  Config::getInstance();
  CodeGenerator::getInstance();
  logging_ = new EnvironmentLogging();
  readConfigFile();
  initializeExpressionSystem();
  portManager = PortManager::getInstance();

  catalog_ = claims::catalog::Catalog::getInstance();
  logging_->log("restore the catalog ...");
  if (rSuccess != catalog_->restoreCatalog()) {
    LOG(ERROR) << "failed to restore catalog" << std::endl;
    cerr << "ERROR: restore catalog failed" << endl;
  }
  stmt_exec_tracker_ = new StmtExecTracker();
  seg_exec_tracker_ = new SegmentExecTracker();

  if (true == g_thread_pool_used) {
    logging_->log("Initializing the ThreadPool...");
    if (false == initializeThreadPool()) {
      logging_->elog("initialize ThreadPool failed");
      assert(false && "can't initialize thread pool");
    }
  }

  logging_->log("Initializing the loader...");
  if (!InitLoader()) {
    LOG(ERROR) << "failed to initialize loader";
  }

  /**
   * TODO:
   * DO something in AdaptiveEndPoint such that the construction function does
          not return until the connection is completed. If so, the following
         sleep() dose not needed.
          This is done in Aug.18 by Li :)
   */
  AnnounceCafMessage();
  /*Before initializing Resource Manager, the instance ip and port should be
   * decided.*/
  logging_->log("Initializing the ResourceManager...");
  initializeResourceManager();
  // should after above
  InitMembership();

  logging_->log("Initializing the Storage...");
  initializeStorage();

  logging_->log("Initializing the BufferManager...");
  initializeBufferManager();

  logging_->log("Initializing txn manager");
  if (!InitTxnManager()) LOG(ERROR) << "failed to initialize txn manager";

  logging_->log("Initializing txn log server");
  if (!InitTxnLog()) LOG(ERROR) << "failed to initialize txn log";

  logging_->log("Initializing the ExecutorMaster...");
  iteratorExecutorMaster = new IteratorExecutorMaster();

  logging_->log("Initializing the ExecutorSlave...");
  iteratorExecutorSlave = new IteratorExecutorSlave();

  exchangeTracker = new ExchangeTracker();
  expander_tracker_ = ExpanderTracker::getInstance();
#ifndef DEBUG_MODE
  if (ismaster) {
    initializeClientListener();
  }
#endif
}

Environment::~Environment() {
  _instance = 0;
  delete expander_tracker_;
  delete logging_;
  delete portManager;
  delete catalog_;
  if (ismaster_) {
    delete iteratorExecutorMaster;
    delete resourceManagerMaster_;
    delete blockManagerMaster_;
#ifndef DEBUG_MODE
    destoryClientListener();
#endif
  }
  delete iteratorExecutorSlave;
  delete exchangeTracker;
  delete resourceManagerSlave_;
  delete blockManager_;
  delete bufferManager_;
}
Environment* Environment::getInstance(bool ismaster) {
  if (_instance == 0) {
    new Environment(ismaster);
  }
  return _instance;
}
std::string Environment::getIp() { return ip; }
unsigned Environment::getPort() { return port; }

ThreadPool* Environment::getThreadPool() const { return thread_pool_; }

void Environment::readConfigFile() {
  libconfig::Config cfg;
  cfg.readFile(Config::config_file.c_str());
  ip = (const char*)cfg.lookup("ip");
}

void Environment::AnnounceCafMessage() {
  announce<NodeAddress>("NodeAddress", &NodeAddress::ip, &NodeAddress::port);
  announce<ProjectionID>("ProjectionID", &ProjectionID::table_id,
                         &ProjectionID::projection_off);
  announce<PartitionID>("PartitionID", &PartitionID::projection_id,
                        &PartitionID::partition_off);

  announce<LoadPacket>("LoadPacket", &LoadPacket::txn_id_,
                       &LoadPacket::global_part_id_, &LoadPacket::pos_,
                       &LoadPacket::offset_, &LoadPacket::data_length_,
                       &LoadPacket::data_buffer_, &LoadPacket::socket_fd_);
  announce<StorageBudgetMessage>(
      "StorageBudgetMessage", &StorageBudgetMessage::nodeid,
      &StorageBudgetMessage::memory_budget, &StorageBudgetMessage::disk_budget);
  announce<ProjectionID>("ProjectionID", &ProjectionID::table_id,
                         &ProjectionID::projection_off);
  announce<PartitionID>("PartitionID", &PartitionID::projection_id,
                        &PartitionID::partition_off);
  announce<ExchangeID>("ExchangeID", &ExchangeID::exchange_id,
                       &ExchangeID::partition_offset);
  announce<BaseNode>("BaseNode", &BaseNode::node_id_to_addr_);
  announce<NodeSegmentID>("NodeSegmentID", &NodeSegmentID::first,
                          &NodeSegmentID::second);
}
void Environment::initializeStorage() {
  if (ismaster_) {
    blockManagerMaster_ = BlockManagerMaster::getInstance();
    blockManagerMaster_->initialize();
  }
  /*both master and slave node run the BlockManager.*/
  //    BlockManagerId *bmid=new BlockManagerId();
  //    string
  // actorname="blockManagerWorkerActor_"+bmid->blockManagerId;
  //    cout<<actorname.c_str()<<endl;
  //    BlockManager::BlockManagerWorkerActor
  //*blockManagerWorkerActor=new
  // BlockManager::BlockManagerWorkerActor(endpoint,framework_storage,actorname.c_str());

  blockManager_ = BlockManager::getInstance();
  blockManager_->initialize();
}

void Environment::initializeResourceManager() {
  if (ismaster_) {
    resourceManagerMaster_ = new ResourceManagerMaster();
    DLOG(INFO) << "ResourceManagerMaster instanced ";
  }
  resourceManagerSlave_ = new InstanceResourceManager();
  // DLOG(INFO) << "resourceManagerSlave instanced ";
  //  nodeid = resourceManagerSlave_->Register();
}
void Environment::InitMembership() {
  if (ismaster_) {
    master_node_ = MasterNode::GetInstance();
  }
  slave_node_ = SlaveNode::GetInstance();
  slave_node_->RegisterToMaster();
  nodeid = slave_node_->get_node_id();
}

bool Environment::InitLoader() {
  if (Config::is_master_loader) {
    LOG(INFO) << "I'm master loader. Oyeah";
    master_loader_ = new MasterLoader();
    std::thread master_thread(&MasterLoader::StartMasterLoader, nullptr);
    master_thread.detach();
    DLOG(INFO) << "started thread as master loader";

    //    TxnServer::Init(6);
  }

  usleep(10000);
  DLOG(INFO) << "starting create thread as slave loader";
  slave_loader_ = new SlaveLoader();
  std::thread slave_thread(&SlaveLoader::StartSlaveLoader, nullptr);
  slave_thread.detach();

  return true;
}

bool Environment::InitTxnManager() {
  if (Config::enable_txn_server) {
    LOG(INFO) << "I'm txn manager server";
    TxnServer::Init(Config::txn_server_cores, Config::txn_server_port);
    auto cat = Catalog::getInstance();
    auto table_count = cat->getNumberOfTable();
    // cout << "table count:" << table_count << endl;
    for (auto table_id : cat->getAllTableIDs()) {
      auto table = cat->getTable(table_id);
      if (NULL == table) {
        cout << " No table whose id is:" << table_id << endl;
        assert(false);
      }
      auto proj_count = table->getNumberOfProjection();
      // cout << "proj_count:" << proj_count << endl;
      for (auto proj_id = 0; proj_id < proj_count; proj_id++) {
        auto proj = table->getProjectoin(proj_id);
        if (NULL == proj) {
          cout << "No projection whose id is:" << proj_id
               << " in table:" << table->getTableName() << endl;
          assert(false);
        }
        auto part = proj->getPartitioner();
        auto part_count = part->getNumberOfPartitions();
        // cout << "part_count:" << part_count << endl;
        for (auto part_id = 0; part_id < part_count; part_id++) {
          auto global_part_id = GetGlobalPartId(table_id, proj_id, part_id);
          //   cout << global_part_id << endl;
          TxnServer::pos_list_[global_part_id] =
              TxnServer::logic_cp_list_[global_part_id] =
                  TxnServer::phy_cp_list_[global_part_id] =
                      part->getPartitionBlocks(part_id) * 64 * 1024;
        }
      }
    }

    cout << "*******pos_list*******" << endl;
    for (auto& pos : TxnServer::pos_list_)
      cout << "partition[" << pos.first << "] => " << pos.second << endl;
  }
  TxnClient::Init(Config::txn_server_ip, Config::txn_server_port);
  return true;
}

bool Environment::InitTxnLog() {
  if (Config::enable_txn_log) {
    LOG(INFO) << "I'm txn log server";
    LogServer::Init(Config::txn_log_path);
  }
  return true;
}

void Environment::initializeBufferManager() {
  bufferManager_ = BufferManager::getInstance();
}

void Environment::initializeIndexManager() {
  indexManager_ = IndexManager::getInstance();
}

ExchangeTracker* Environment::getExchangeTracker() { return exchangeTracker; }
ResourceManagerMaster* Environment::getResourceManagerMaster() {
  return resourceManagerMaster_;
}
InstanceResourceManager* Environment::getResourceManagerSlave() {
  return resourceManagerSlave_;
}
NodeID Environment::getNodeID() const { return nodeid; }
claims::catalog::Catalog* Environment::getCatalog() const { return catalog_; }

void Environment::initializeClientListener() {
  listener_ = new ClientListener(Config::client_listener_port);
  listener_->configure();
}

void Environment::initializeExpressionSystem() {
  InitTypeConversionMatrix();
  InitOperatorFunc();
  InitAggAvgDivide();
  InitTypeCastFunc();
}

void Environment::destoryClientListener() {
  listener_->shutdown();
  delete listener_;
}

bool Environment::initializeThreadPool() {
  thread_pool_ = new ThreadPool();
  return thread_pool_->Init(Config::thread_pool_init_thread_num);
}

IteratorExecutorSlave* Environment::getIteratorExecutorSlave() const {
  return iteratorExecutorSlave;
}
