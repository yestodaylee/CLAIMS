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
 * /Claims/loader/single_file_connector.h
 *
 *  Created on: Oct 21, 2015
 *      Author: yukai
 *		   Email: yukai2014@gmail.com
 *
 * Description:
 *
 */

#ifndef LOADER_SINGLE_FILE_CONNECTOR_H_
#define LOADER_SINGLE_FILE_CONNECTOR_H_
#include <assert.h>
#include <atomic>
#include <string>
#include "./file_connector.h"
#include "../common/file_handle/file_handle_imp_factory.h"
#include "../common/file_handle/file_handle_imp.h"
#include "../common/memory_handle.h"
#include "../common/rename.h"
#include "../utility/lock.h"
#include "../utility/lock_guard.h"

namespace claims {
namespace loader {

using std::string;
using claims::common::FileHandleImp;
using claims::common::FilePlatform;
using claims::common::rSuccess;
using claims::utility::LockGuard;
using std::atomic;

class SingleFileConnector {
  NO_COPY_AND_ASSIGN(SingleFileConnector);

 public:
  SingleFileConnector(FilePlatform platform, string file_name)
      : platform_(platform), file_name_(file_name), is_closed(true) {
    imp_ = common::FileHandleImpFactory::Instance().CreateFileHandleImp(
        platform_, file_name_);
  }
  ~SingleFileConnector() { DELETE_PTR(imp_); }

  RetCode Open(common::FileOpenFlag open_flag);
  RetCode Close();

  RetCode AtomicFlush(const void* source, unsigned length);

  /*RetCode AtomicFlush(const void* source, unsigned length,
                      function<void()> lock_func, function<void()> unlock_func,
                      bool overwrite = false) {
    if (overwrite)
      return imp_->AtomicOverWrite(source, length, lock_func, unlock_func);
    else
      return imp_->AtomicAppend(source, length, lock_func, unlock_func);
  }*/

  bool CanAccess() { return imp_->CanAccess(file_name_); }

  RetCode Delete();
  /**
   * @brief Method description: load total file into memory
   * @param buffer: set buffer point to a new memory allocated by this method,
   * @param length: set to the length of file, also the length of new memory
   * @return  rSuccess if succeed.
   * @details   (additional) this method will modify buffer, set to a new memory
   */
  RetCode LoadTotalFile(void*& buffer, uint64_t* length) {
    assert(common::FileOpenFlag::kReadFile == open_flag_ &&
           "open mode must be read ");
    LockGuard<Lock> guard(write_lock_);
    return imp_->ReadTotalFile(buffer, length);
  }

  /**
   * @brief Method description: load a part of file into memory pointed by
   * buffer
   * @param buffer: point to the memory where file is load
   * @param start: the position from which to read
   * @param length: the length of data need to read
   * @return  rSuccess if OK
   * @details   (additional)
   */
  RetCode LoadFile(void* buffer, int64_t start, uint64_t length) {
    assert(common::FileOpenFlag::kReadFile == open_flag_ &&
           "open mode must be read ");
    return imp_->PRead(buffer, length, start);
  }

 private:
  string file_name_;
  common::FilePlatform platform_;
  common::FileHandleImp* imp_;
  common::FileOpenFlag open_flag_ = -1;

  Lock write_lock_;

  atomic<int> ref_;
  bool is_closed;
  SpineLock open_close_lcok_;
};

} /* namespace loader */
} /* namespace claims */

#endif  // LOADER_SINGLE_FILE_CONNECTOR_H_
