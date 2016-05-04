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
 * /CLAIMS/txn_manager/ser_test.cpp
 *
 *  Created on: 2016年5月4日
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */
#include <iostream>
#include <sstream>
#include <string>
#include "txn.hpp"
using std::cout;
using std::cin;
using std::endl;
using std::string;
using std::stringstream;
using claims::txn::Query;
/**
 *  Transform T type c++ object to string
 */
template<typename T>
string Serialize(T obj) {
  stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << obj;
  return ss.str();
}

/**
 * Transform string to T type c++ object
 */
template<typename T>
T Derialize(string obj) {
  T ret;
  stringstream ss(obj);
  boost::archive::text_iarchive ia(ss);
  ia >> ret;
  return ret;
}
int main() {
  cout << "hello world" << endl;
  Query query1, query2;
  query1.cp_list_ = {{1, 2}, {2, 10}};
  query1.snapshot_ = {{1, {{3,1},{4,1}}}, {2, {{3,1},{4,1}}}};
  string tmp = Serialize<Query>(query1);
  cout << tmp << endl;
  query2 = Derialize<Query>(tmp);
  cout << query2.ToString() << endl;
}





