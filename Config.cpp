/*
 * Config.cpp
 *
 *  Created on: Apr 4, 2014
 *      Author: wangli
 */

#include "Config.h"
#include "Debug.h"
#include <stdlib.h>
#include <libconfig.h++>
#include <unistd.h>
#include <iosfwd>
#include <iostream>
#include <string>
#include <sstream>
using namespace std;
//#define DEBUG_Config

string gete() {
  char *p = getenv("CLAIMS_HOME");
  stringstream sp;
  sp << string(p).c_str() << "/conf/config";
  return sp.str();
  //	return "/home/imdb/config/wangli/config";
}
string get_default_logfile_name() {
  char *p = getenv("CLAIMS_HOME");
  stringstream sp;
  sp << string(p).c_str() << "/log/claims.log";
  return sp.str();
}
std::string Config::config_file;
Config *Config::instance_ = 0;

/**
 * This parameter specifies the maximum degrees of parallelism
 * for each expander.
 */
int Config::max_degree_of_parallelism;

/*
 * This parameter specifies the data path to search for data file,
 * e.g., T0G0P0,...
 */
std::string Config::data_dir;

/* this parameter control whether expander tracker change the degree
 *  of parallelism in each expander based on the workload.*/
bool Config::enable_expander_adaptivity;

/*
 * This parameter sets the frequency, at which expander tracker keeps
 * track of expander's workload and adaptively change the degree of
 * parallelism if needed.
 */
int Config::expander_adaptivity_check_frequency;

/**
 * This parameter sets the initial degree of parallelism for each segment.
 */
int Config::initial_degree_of_parallelism;

/**
 * THis parameter determines whether the data transmission in data exchanges
 * is pipelined.
 */
bool Config::pipelined_exchange;

int Config::scan_batch;

std::string Config::logfile;

std::string Config::hdfs_master_ip;

int Config::hdfs_master_port;

bool Config::master;

bool Config::local_disk_mode;

/* the port of the ClientListener in the master */
int Config::client_listener_port;

bool Config::enable_codegen;

std::string Config::catalog_file;

int Config::thread_pool_init_thread_num;

int Config::load_thread_num;

bool Config::is_master_loader;
std::string Config::master_loader_ip;
int Config::master_loader_port;


bool Config::enable_txn_server;
int Config::txn_server_cores;
std::string Config::txn_server_ip;
int Config::txn_server_port;

bool Config::enable_txn_log;
std::string Config::txn_log_path;


Config *Config::getInstance() {
  if (instance_ == 0) {
    instance_ = new Config();
  }
  return instance_;
}

Config::Config() { initialize(); }

Config::~Config() {
  // TODO Auto-generated destructor stub
}

void Config::initialize() {
  if (config_file.empty()) config_file = gete().c_str();

  /**
   * open configure file, which path is specified in CONFIG.
   */
  cfg.readFile(config_file.c_str());

  /*
   * The following lines set the search attribute name and default value for
   * each parameter.
   */

  data_dir = getString("data", "/home/imdb/data/");

  max_degree_of_parallelism = getInt("max_degree_of_parallelism", 4);

  expander_adaptivity_check_frequency =
      getInt("expander_adaptivity_check_frequency", 1000);

  enable_expander_adaptivity = getBoolean("enable_expander_adaptivity", false);

  initial_degree_of_parallelism = getInt("initial_degree_of_parallelism", 1);

  scan_batch = getInt("scan_batch", 10);

  hdfs_master_ip = getString("hdfs_master_ip", "10.11.1.192");

  hdfs_master_port = getInt("hdfs_master_port", 9000);

  logfile = getString("log", get_default_logfile_name().c_str());

  master = getBoolean("master", true);

  local_disk_mode = getBoolean("local_disk_mode", false);

  pipelined_exchange = getBoolean("pipelined_exchange", true);

  client_listener_port = getInt("client_listener_port", 10001);

  catalog_file = getString("catalog_file", data_dir + "CATALOG");

  enable_codegen = getBoolean("enable_codegen", true);

  thread_pool_init_thread_num = getInt("thread_pool_init_thread_num", 100);

  load_thread_num = getInt("load_thread_num", sysconf(_SC_NPROCESSORS_CONF));

  is_master_loader = getBoolean("is_master_loader", true);

  master_loader_ip = getString("master_loader_ip", "10.11.1.193");

  master_loader_port = getInt("master_loader_port", 9001);

  // txn manager
  enable_txn_server = getBoolean("txn_server", true);

  txn_server_cores = getInt("txn_server_cores", 4);

  txn_server_ip = getString("txn_server_ip", "127.0.0.1");

  txn_server_port = getInt("txn_server_port", 9100);

  // txn log
  enable_txn_log = getBoolean("txn_log", true);

  txn_log_path = getString("txn_log_path", ".");

#ifdef DEBUG_Config
  print_configure();
#endif
}

std::string Config::getString(std::string attribute_name,
                              std::string default_value) {
  std::string ret;
  try {
    ret = (const char *)cfg.lookup(attribute_name.c_str());
  } catch (libconfig::SettingNotFoundException &e) {
    ret = default_value;
  }
  return ret;
}

int Config::getInt(std::string attribute_name, int default_value) {
  int ret;
  try {
    ret = cfg.lookup(attribute_name.c_str());
  } catch (libconfig::SettingNotFoundException &e) {
    ret = default_value;
  }
  return ret;
}

bool Config::getBoolean(std::string attribute_name, bool defalut_value) {
  bool ret;
  try {
    ret = ((int)cfg.lookup(attribute_name.c_str())) == 1;
  } catch (libconfig::SettingNotFoundException &e) {
    ret = defalut_value;
  }
  return ret;
}

void Config::print_configure() const {
  std::cout << "configure file :" << config_file << std::endl;
  std::cout << "The configure is as follows." << std::endl;
  std::cout << "data:" << data_dir << std::endl;
  std::cout << "max_degree_of_parallelism:" << max_degree_of_parallelism
            << std::endl;
  std::cout << "expander_adaptivity_check_frequency:"
            << expander_adaptivity_check_frequency << std::endl;
  std::cout << "enable_expander_adaptivity:" << enable_expander_adaptivity
            << std::endl;
  std::cout << "initial_degree_of_parallelism:" << initial_degree_of_parallelism
            << std::endl;
  std::cout << "hdfs master ip:" << hdfs_master_ip << std::endl;
  std::cout << "hdfs_master_port:" << hdfs_master_port << std::endl;

  std::cout << "log:" << logfile << std::endl;
  std::cout << "master:" << master << std::endl;
  std::cout << "local disk mode:" << local_disk_mode << std::endl;
  std::cout << "client_lisener_port:" << client_listener_port << std::endl;
  std::cout << "catalog_file:" << catalog_file << std::endl;
  std::cout << "codegen:" << enable_codegen << std::endl;
  std::cout << "load_thread_num:" << load_thread_num << std::endl;

  std::cout << "enable_txn_serverr:" << enable_txn_server << std::endl;
  std::cout << "txn_server_cores:" << txn_server_cores << std::endl;
  std::cout << "txn_server_ip:" << txn_server_ip << std::endl;
  std::cout << "txn_server_port:" << txn_server_port << std::endl;

  std::cout << "enable_txn_log:" << enable_txn_log << std::endl;
  std::cout << "txn_log_path:" << txn_log_path << std::endl;
}

void Config::setConfigFile(std::string file_name) { config_file = file_name; }
