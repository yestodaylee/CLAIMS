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
 * /CLAIMS/common/data_type.cpp
 *
 *  Created on: Oct 31, 2015
 *      Author: imdb
 *		   Email: 
 * 
 * Description:
 *
 */

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <glog/logging.h>
#include <limits.h>
#include <float.h>
#include <string>
#include <sstream>
#include "./data_type.h"
#include "./hash.h"
#include "../utility/string_process.h"
#include "./types/NValue.hpp"
using std::cout;
using std::cin;
using std::endl;
using boost::gregorian::date_duration;
using boost::gregorian::from_undelimited_string;
using boost::gregorian::from_string;
using boost::posix_time::duration_from_string;
using boost::gregorian::date;
using boost::posix_time::ptime;
using boost::posix_time::time_duration;
using boost::posix_time::time_from_string;
using boost::posix_time::neg_infin;
using boost::hash_value;
using boost::hash_combine;
using decimal::NValue;
using decimal::ExportSerializeOutput;

/**
 * The function template to add increment to target
 * @param target
 * @param increment
 */
template <typename T>
void AddFuc(void* target, void* increment) {
  *static_cast<T*>(target) += *static_cast<T*>(increment);
}
template <>
void AddFuc<char>(void* target, void* increment) {
  LOG(WARNING) << "add for string type is not supported ! " << endl;
}
template <>
void AddFuc<NValue>(void* target, void* increment) {
  *static_cast<NValue*>(target) = static_cast<NValue*>(target)->op_add(
          *static_cast<NValue*>(increment));
}
template <>
void AddFuc<date>(void* target, void* increment) {
  *static_cast<date*>(target) =
      *static_cast<date*>(target) + *static_cast<date_duration*>(increment);
}
template <>
void AddFuc<ptime>(void* target, void* increment) {
  *static_cast<ptime*>(target) =
      *static_cast<ptime*>(target) + *static_cast<time_duration*>(increment);
}
template <>
void AddFuc<time_duration>(void* target, void* increment) {
  *static_cast<time_duration*>(target) = *static_cast<time_duration*>(target) +
      *static_cast<time_duration*>(increment);
}
/**
 * The function template to multiple increment up to target
 * @param target
 * @param increment
 */
template <typename T>
void MultiplyFuc(void* target, void* increment) {
  *static_cast<T*>(target) =
      (*static_cast<T*>(target)) * (*static_cast<T*>(increment));
}
template <>
void MultiplyFuc<NValue>(void* target, void* increment) {
  *static_cast<NValue*>(target) = static_cast<NValue*>(target)->op_multiply(
      *static_cast<NValue*>(increment));
}
template <>
void MultiplyFuc<char>(void* target, void* increment) {
  LOG(WARNING) << "The multiply for string type is not supported !" << endl;
}
template <>
void MultiplyFuc<date>(void* target, void* increment) {
  LOG(WARNING) << "The multiply for date type is not supported !" << endl;
}
template <>
void MultiplyFuc<ptime>(void* target, void* increment) {
  LOG(WARNING) << "The multiply for datetime type is not supported !" << endl;
}
template <>
void MultiplyFuc<time_duration>(void* target, void* increment) {
  LOG(WARNING) << "The multiply for time type is not supported !" << endl;
}

/**
 * The function template for minimum,
 * and target will be assigned to the minimum one
 * @param target
 * @param increment
 */
template <typename T>
void MinFuc(void* target, void* increment) {
  if (*static_cast<T*>(target) > *static_cast<T*>(increment))
    *static_cast<T*>(target) = *static_cast<T*>(increment);
}
template <>
void MinFuc<char>(void* target, void* increment) {
  if (strcmp(static_cast<char*>(target), static_cast<char*>(increment)) > 0)
    strcpy(static_cast<char*>(target), static_cast<char*>(increment));
}
template <>
void MinFuc<NValue>(void* target, void* increment) {
  *static_cast<NValue*>(target) =
      static_cast<NValue*>(target)->op_min(*static_cast<NValue*>(increment));
}

/**
 * The function template for minimum,
 * and target will be assigned to the minimum one
 * @param target
 * @param increment
 */
template <typename T>
void MaxFuc(void* target, void* increment) {
  if (*static_cast<T*>(target) < *static_cast<T*>(increment))
    *static_cast<T*>(target) = *static_cast<T*>(increment);
}
template <>
void MaxFuc<char>(void* target, void* increment) {
  if (strcmp(static_cast<char*>(target), static_cast<char*>(increment)) < 0)
    strcpy(static_cast<char*>(target), static_cast<char*>(increment));
}
template <>
void MaxFuc<NValue>(void* target, void* increment) {
  *static_cast<NValue*>(target) =
      static_cast<NValue*>(target)->op_max(*static_cast<NValue*>(increment));
}
/**
 * The function to assign vaue from src to desc
 * @param src
 * @param desc
 */
template <typename T>
void AssignFuc(const void* src, void* desc) {
  *static_cast<T*>(desc) = *static_cast<T*>(const_cast<void*>(src));
}
template <>
void AssignFuc<char>(const void*  src, void*  desc) {
  strcpy(static_cast<char*>(const_cast<void*>(desc)),
         static_cast<char*>(const_cast<void*>(src)));
}
/**
 * add "1" up to target
 * @param target
 * @param increment
 */
template <typename T>
void IncreaseByOneFuc(void* target, void* increment) {
  (*static_cast<T*>(target))++;
}
template <>
void IncreaseByOneFuc<char>(void* target, void* increment) {
  LOG(WARNING) << "The ++ for string type is not supported!" << endl;
}
template <>
void IncreaseByOneFuc<NValue>(void* target, void* increment) {
  NValue nv1 = NValue::getDecimalValueFromString("1");
  *static_cast<NValue*>(target) = static_cast<NValue*>(target)->op_add(nv1);
}
template <>
void IncreaseByOneFuc<date>(void* target, void* increment) { }
template <>
void IncreaseByOneFuc<ptime>(void* target, void* increment) { }
template <>
void IncreaseByOneFuc<time_duration>(void* target, void* increment) { }
/**
 * The function template to compare a and b
 * @param a
 * @param b
 * @return a equals b or not (bool)
 */
template<typename T>
bool EqualFuc(const void *  a, const void *  b) {
  return *static_cast<T*>(const_cast<void*>(a)) ==
      *static_cast<T*>(const_cast<void*>(b));
}
template<>
bool EqualFuc<char>(const void * a, const void *  b) {
  return strcmp(static_cast<char*>(const_cast<void*>(a)),
                static_cast<char*>(const_cast<void*>(b)));
}
template<>
bool EqualFuc<NValue>(const void *   a, const void * b) {
  return static_cast<NValue*>(const_cast<void*>(a))->op_equals(
      *static_cast<NValue*>(const_cast<void*>(b)));
}
/**
 * The function template to compare a and b
 * @param a
 * @param b
 * @return a is less than b or not
 */
template<typename T>
bool LessFuc(const void*  a, const void*  b) {
  return *static_cast<T*>(const_cast<void*>(a)) <
      *static_cast<T*>(const_cast<void*>(b));
}
template<>
bool LessFuc<char>(const void* a, const void* b) {
  return strcmp(static_cast<char*>(const_cast<void*>(a)),
                static_cast<char*>(const_cast<void*>(b))) < 0;
}
template<>
bool LessFuc<NValue>(const void* a, const void* b) {
  if (static_cast<NValue*>(const_cast<void*>(a))->op_equals(
      *static_cast<NValue*>(const_cast<void*>(b))))
    return false;
  NValue tmp = static_cast<NValue*>(const_cast<void*>(a))->op_min(
      *static_cast<NValue*>(const_cast<void*>(b)));
  if (tmp.op_equals(*static_cast<NValue*>(const_cast<void*>(a))))
    return true;
  return false;
}

/**
 * The function template to compare a and b
 * @param a
 * @param b
 * @return a is greater than b or not
 */
template<typename T>
bool GreaterFuc(const void* a, const void* b) {
  return *static_cast<T*>(const_cast<void*>(a)) >
    *static_cast<T*>(const_cast<void*>(b));
}
template<>
bool GreaterFuc<char>(const void* a, const void* b) {
  return strcmp(static_cast<char*>(const_cast<void*>(a)),
                static_cast<char*>(const_cast<void*>(b))) > 0;
}
template<>
bool GreaterFuc<NValue>(const void* a, const void* b) {
  if (static_cast<NValue*>(const_cast<void*>(a))->op_equals(
      *static_cast<NValue*>(const_cast<void*>(b))))
    return false;
  NValue tmp = static_cast<NValue*>(const_cast<void*>(a))->op_min(
      *static_cast<NValue*>(const_cast<void*>(b)));
  if (tmp.op_equals(*static_cast<NValue*>(const_cast<void*>(a))))
    return false;
  return true;
}

/**
 * The function template to compare a and b
 * @param a
 * @param b
 * @return a - b
 */
template<typename T>
int CompareFuc(const void *a, const void * b) {
  if (*static_cast<T*>(const_cast<void*>(a)) <
      *static_cast<T*>(const_cast<void*>(b)))
    return -1;
  else if (*static_cast<T*>(const_cast<void*>(a)) >
    *static_cast<T*>(const_cast<void*>(b)))
    return 1;
  return 0;
}
template<>
int CompareFuc<char>(const void *a, const void *b) {
  return strcmp(static_cast<char*>(const_cast<void*>(a)),
                static_cast<char*>(const_cast<void*>(b)));
}
template<>
int CompareFuc<NValue>(const void *a, const void *b) {
  if (static_cast<NValue*>(const_cast<void*>(a))->op_equals(
      *static_cast<NValue*>(const_cast<void*>(b))))
    return 0;
  else  if (LessFuc<NValue>(a, b))
    return -1;
  return 1;
}
/**
 * get partition value ( hash value) for a key value
 * @param key: The key to get the hash value
 * @param mod: If mod is not 0 and partition function value is nullptr,
 *             the hash value will mode mod
 * @param partition_function if partition function is given, partition function will evaluate
 *             the hash value
 * @return the hash value
 */
template<typename T>
unsigned GetPartitionValueFuc(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc) {
  if (partition_fuc != nullptr) {
    return partition_fuc->get_partition_value(
        *static_cast<T*>(const_cast<void*>(key)));
  } else {
    if (mod != 0)
      return hash_value(*static_cast<T*>(const_cast<void*>(key))) % mod;
    else
      return hash_value(*static_cast<T*>(const_cast<void*>(key)));
  }
  return 0;
}
template<>
unsigned GetPartitionValueFuc<unsigned int>(
    const void* key, unsigned long mod,
    PartitionFunction* partition_fuc) {
if (partition_fuc != nullptr) {
  return partition_fuc->get_partition_value(
      *static_cast<short*>(const_cast<void*>(key)));
} else {
  if (mod != 0)
    return hash_value(*static_cast<short*>(const_cast<void*>(key))) % mod;
  else
    return hash_value(*static_cast<short*>(const_cast<void*>(key)));
}
return 0;
}
template<>
unsigned GetPartitionValueFuc<char>(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc) {
  if (partition_fuc != nullptr) {
      cout<<"The hash function for string type is not implement yet!"<<endl;
      assert(false);
      return 0;
  } else {
    if (mod != 0)
      return hash_value(string(*static_cast<char*>(const_cast<void*>
          (key))))%mod;
    else
      return hash_value(string(*static_cast<char*>(const_cast<void*>(key))));
  }
  return 0;
}
template<>
unsigned GetPartitionValueFuc<date>(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc) {
  if (partition_fuc != nullptr) {
      cout<<"The hash function for date type is not implement yet!"<<endl;
      assert(false);
      return 0;
  } else {
      if (mod != 0)
        return hash_value(static_cast<date*>(
            const_cast<void*>(key))->julian_day()) % mod;
      else
        return hash_value(static_cast<date*>(
            const_cast<void*>(key))->julian_day());
  }
  return 0;
}
template<>
unsigned GetPartitionValueFuc<time_duration>(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc) {
  if (partition_fuc != nullptr) {
      cout << "The hash function for time type is not implement yet!" << endl;
      assert(false);
      return 0;
  } else {
    if ( mod != 0 )
      return hash_value(static_cast<time_duration*>(
          const_cast<void*>(key))->total_nanoseconds())
          % mod;
    else
      return hash_value(static_cast<time_duration*>(
          const_cast<void*>(key))->total_nanoseconds());
  }
  return 0;
}
template<>
unsigned GetPartitionValueFuc<ptime>(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc) {
  if (partition_fuc != nullptr) {
      cout << "The hash function for date time type is not implement yet!"
          << endl;
      assert(false);
      return 0;
  } else {
     if (mod != 0)
       return hash_value(to_simple_string(
           *static_cast<ptime*>(const_cast<void*>(key)))) % mod;
     else
       return hash_value(to_simple_string(
           *static_cast<ptime*>(const_cast<void*>(key))));
  }
  return 0;
}
template<>
unsigned GetPartitionValueFuc<NValue>(
      const void* key, unsigned long mod,
      PartitionFunction* partition_fuc ) {
  unsigned long ul1 =  *(unsigned long*)(((NValue*)key)->m_data);
  unsigned long ul2 =  *(unsigned long*)(((NValue*)key)->m_data+8);
  if (partition_fuc != nullptr) {
    return partition_fuc->get_partition_value(ul1+ul2);
  } else {
    hash_combine(ul1, ul2);
      if (mod != 0)
        return ul1 % mod;
      else
        return ul1;
  }
  return 0;
}

/**
 * The function to init a Operate instance
 * @param operate
 */
template<typename T>
inline void InitOperate(Operate * operate) {
  operate->Add = AddFuc<T>;
  operate->Assign = AssignFuc<T>;
  operate->Compare = CompareFuc<T>;
  operate->Equal = EqualFuc<T>;
  operate->GetPartitionValue = GetPartitionValueFuc<T>;
  operate->Greater = GreaterFuc<T>;
  operate->IncreaseByOne = IncreaseByOneFuc<T>;
  operate->Less = LessFuc<T>;
  operate->Max = MaxFuc<T>;
  operate->Min = MinFuc<T>;
  operate->Multiply = MultiplyFuc<T>;
}

/**
 * The constructor function for Operate
 * accroding to differen data type, InitOperate will
 * assign corresponding  functions to Operate's function pointer
 * like add, min, max , etc.
 * @param _type
 * @param _nullable
 * @param _digit_len
 */
Operate::Operate(data_type _type = t_int, bool _nullable = true,
                          unsigned int _digit_len = 12) {
  this->type = _type;
  this->nullable = _nullable;
  this->digit_len = _digit_len;
  switch(this->type){
    case t_int: InitOperate<int>(this); break;
    case t_float: InitOperate<float>(this); break;
    case t_double: InitOperate<double>(this); break;
    case t_string: InitOperate<char>(this); break;
    case t_u_long: InitOperate<unsigned long>(this); break;
    case t_date:   InitOperate<date>(this); break;
    case t_time:    InitOperate<time_duration>(this); break;
    case t_datetime: InitOperate<ptime>(this); break;
    case t_decimal:  InitOperate<NValue>(this); break;
    case t_smallInt: InitOperate<short>(this); break;
    case t_u_smallInt: InitOperate<unsigned int>(this); break;
    case t_boolean:   InitOperate<int>(this); break;
  }
}
