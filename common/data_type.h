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
 * /CLAIMS/common/data_type.h
 *
 *  Created on: Oct 31, 2015
 *      Author: imdb
 *       Email:
 *
 * Description:
 *
 */

#ifndef COMMON_DATATYPE_H_
#define COMMON_DATATYPE_H_
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
#include <map>
#include "./hash.h"
#include "../utility/string_process.h"
#include "./types/NValue.hpp"
using std::cout;
using std::cin;
using std::endl;
using std::map;
using std::string;
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

typedef void (*fun)(void*, void*);
#define DATA_TYPE_NUMBER 20
/**
 * enum: data_type
 * descrition: the different types of data value
 */
enum data_type {
  t_smallInt = 0,     //!< t_smallInt
  t_int,          //!< t_int
  t_u_long,       //!< t_u_long
  t_float,        //!< t_float
  t_double,       //!< t_double
  t_string,       //!< t_string
  t_date,         //!< t_date
  t_time,         //!< t_time
  t_datetime,     //!< t_datetime
  t_decimal,      //!< t_decimal
  t_boolean,      //!< t_boolean
  t_u_smallInt,   //!< t_u_smallInt
  t_date_day,     //!< t_date_day
  t_date_week,    //!< t_date_week
  t_date_month,   //!< t_date_month
  t_date_year,    //!< t_date_year
  t_date_quarter  //!< t_date_quarter
};
static map<data_type,unsigned int> data_type_size_dic_abcx = {
    {t_int, sizeof(int)},
    {t_float, sizeof(float)},
    {t_double, sizeof(double)},
    {t_u_long, sizeof(unsigned long)},
    {t_string, 0},
    {t_date, sizeof(date)},
    {t_time, sizeof(time_duration)},
    {t_datetime, sizeof(ptime)},
    {t_decimal, 16},
    {t_smallInt, sizeof(short)},
    {t_u_smallInt, sizeof(unsigned short)},
    {t_boolean, sizeof(int)}
};
/**
 * NULL value for all date type
 */
#define NULL_SMALL_INT SHRT_MAX
#define NULL_INT INT_MAX
#define NULL_U_LONG ULONG_LONG_MAX
#define NULL_FLOAT FLT_MAX   // const transfor to int 2139095039
#define NULL_DOUBLE DBL_MAX  // const transfor to int -1
#define NULL_STRING '7'
#define NULL_DATE neg_infin  // is_neg_infinity() return true
#define NULL_TIME neg_infin
#define NULL_DATETIME neg_infin
#define NULL_DECIMAL nvalue_null
#define NULL_U_SMALL_INT USHRT_MAX
#define NULL_BOOLEAN 2
static NValue nvalue_null = NValue::getDecimalValueFromString(
    "99999999999999999999999999.999999999999");  // null value for decimal
/**
 * the number of bytes that are aligned between any two
 *  adjacent data types
 */
#define allign_bytes 4
#define byte_align(size) (((size - 1) / allign_bytes + 1) * allign_bytes)

/**
 * @brief Class description: The encapsulation of operate function
 *                           for all different  data types;
 */
class Operate {
 public:
  explicit Operate(data_type _type = t_int, bool _nullable = true,
                   unsigned int _digit_len = 12);
  virtual ~Operate() {}
  inline virtual string ToString(void* value) = 0;
  inline virtual void ToValue(void* target, const char* str) = 0;
  inline virtual bool SetNull(void* value) = 0;
  inline virtual bool IsNull(void* value) const = 0;
  virtual Operate* DuplicateOperator() const = 0;
  fun GetADDFunction() { return Add; }
  fun GetMINFunction() { return Min; }
  fun GetMAXFunction() { return Max; }
  fun GetIncreateByOneFunction() {return IncreaseByOne; }
 /*
  *  fun GetAVGFunction() {return IncreateByOneFunction;}
  *   */
  bool nullable;
  data_type type;
  unsigned int digit_len;
  void (*Assign)(const void* src, void* desc);
  bool (*Equal)(const void*  a, const void* b);
  bool (*Less)(const void*  a, const void*  b);
  bool (*Greater)(const void* a, const void*  b);
  void (*Add)(void* target, void* increment);
  void (*Min)(void* target, void* increment);
  void (*Max)(void* target, void* increment);
  void (*Multiply)(void* target, void* increment);
  void (*IncreaseByOne)(void* target, void* increment);
  int  (*Compare)(const void* a, const void* b);
  unsigned (*GetPartitionValue)( const void* key, unsigned long,
      PartitionFunction* partition_function);
};

/**
 *  The Operate for int type
 */
class OperateInt : public Operate {
 public:
  explicit OperateInt(bool _nullable) : Operate(t_int, _nullable) { }
  inline string ToString(void* value) {
    string ret;
    if (this->nullable == true &&
        (*static_cast<int*>(value)) == static_cast<int>(NULL_INT)) {
      ret = "NULL";
    } else {
      ostringstream ss;
      ss << *static_cast<int*>(value);
      ret = ss.str();
    }
    return ret;
  }
  inline void ToValue(void* target, const char* str) {
    if (strcmp(str, "") == 0 && this->nullable == true)
      *static_cast<int*>(target) = NULL_INT;
    else
      *static_cast<int*>(target) = atoi(str);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<int*>(value) = NULL_INT;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<int*>(value) == NULL_INT)
      return true;
    return false;
  }
  Operate* DuplicateOperator() const {
    return new OperateInt(this->nullable);
  }
};

/**
 * The operate for bool type
 */
class OperateBool : public Operate {
 public:
  explicit OperateBool(bool _nullable) : Operate(t_boolean, _nullable) { }
  inline string ToString(void* value) {
    if (this->nullable == true && *static_cast<int*>(value) == NULL_BOOLEAN) {
      return "NULL";
    } else {
      ostringstream ss;
      if (*static_cast<int*>(value) == 0)
        return "FALSE";
      else
        return "TRUE";
    }
  }
  inline void ToValue(void* target, const char* str) {
    string f = "FALSE";
    string t = "TRUE";
    string n = "NULL";
    if ((strcmp(str, n.c_str()) == 0) && this->nullable == true) {
      *static_cast<int*>(target) = NULL_BOOLEAN;
    } else if (strcmp(f.c_str(), str) == 0) {
      *static_cast<int*>(target) = 0;
    } else {
      *static_cast<int*>(target) = 1;
    }
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<int*>(value) = NULL_BOOLEAN;
    return true;
  }

  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<int*>(value) == NULL_SMALL_INT)
      return true;
    return false;
  }
  Operate* DuplicateOperator() const { return new OperateBool(this->nullable); }
};
/**
 * Operate for type float
 */
class OperateFloat : public Operate {
 public:
  explicit OperateFloat(bool _nullable = true) : Operate(t_float, _nullable) {}
  inline string ToString(void* value) {
    string ret;
    if (this->nullable == true && *static_cast<float*>(value) == NULL_FLOAT) {
      ret = "NULL";
    } else {
      ostringstream ss;
      ss << *static_cast<float*>(value);
      ret = ss.str();
    }
    return ret;
  }
  inline void ToValue(void* target, const char* str) {
    if (strcmp(str, "") == 0 && this->nullable == true)
      *static_cast<float*>(target) = NULL_FLOAT;
    else
      *static_cast<float*>(target) = atof(str);
  }
  Operate* DuplicateOperator() const {
    return new OperateFloat(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<float*>(value) = NULL_FLOAT;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<float*>(value) == NULL_FLOAT)
      return true;
    return false;
  }
};
/**
 * Operate for double type
 */
class OperateDouble : public Operate {
 public:
  explicit OperateDouble(bool _nullable = true): Operate(t_double, _nullable) {}
  inline string ToString(void* value) {
    string ret;
    if (this->nullable == true && *static_cast<double*>(value) == NULL_DOUBLE) {
      ret = "NULL";
    } else {
      ostringstream ss;
      ss << *static_cast<double*>(value);
      ret = ss.str();
    }
    return ret;
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true)
      *static_cast<double*>(target) = NULL_DOUBLE;
    else
      *static_cast<double*>(target) = atof(str);
  }
  Operate* DuplicateOperator() const {
    return new OperateDouble(this->nullable);
  }

  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<double*>(value) = NULL_DOUBLE;
    return true;
  }

  inline bool IsNull(void* value) const {
    if (this->nullable == true &&
        *static_cast<int*>(value) == static_cast<int>(NULL_DOUBLE))
      return true;
    return false;
  }
};

/**
 * The Operate for small int type
 */

class OperateSmallInt : public Operate {
 public:
  explicit OperateSmallInt(bool _nullable = true)
      : Operate(t_smallInt, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true && *static_cast<short*>(value) == NULL_SMALL_INT) {
      return "NULL";
    } else {
      ostringstream ss;
      ss << *static_cast<short*>(value);
      string ret = ss.str();
      return ret;
    }
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) &&
        this->nullable == true)  // modified by Li Wang in Sep.10th
      *static_cast<short*>(target) = NULL_SMALL_INT;
    else
      *static_cast<short*>(target) = static_cast<short>(atoi(str));
  }
  Operate* DuplicateOperator() const {
    return new OperateSmallInt(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<short*>(value) = NULL_SMALL_INT;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<short*>(value) == NULL_SMALL_INT)
      return true;
    return false;
  }
};
/**
 * Operate for unsigned small int
 */
class OperateUSmallInt : public Operate {
 public:
  explicit OperateUSmallInt(bool _nullable = true)
      : Operate(t_smallInt, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true &&
        *static_cast<unsigned short*>(value) == NULL_U_SMALL_INT)
      return "NULL";
    ostringstream ss;
    ss << *static_cast<short *>(value);
    string ret = ss.str();
    return ret;
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true)
      *static_cast<unsigned short*>(target) = NULL_U_SMALL_INT;
    else
      *static_cast<unsigned short*>(target) = static_cast<unsigned short>(atoi(str));
  }
  Operate* DuplicateOperator() const {
    return new OperateUSmallInt(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<unsigned short*>(value) = NULL_U_SMALL_INT;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<unsigned short*>(value) == NULL_U_SMALL_INT)
      return true;
    return false;
  }
};
/**
 * Operate for unsigned long
 */


class OperateULong : public Operate {
 public:
  OperateULong(bool _nullable = true):Operate(t_u_long, _nullable) { }
  inline string ToString(void* value) {
    string ret;
    if (this->nullable == true && (*(unsigned long*)value) == NULL_U_LONG)
      ret = "NULL";
    else {
      ostringstream ss;
      ss << *static_cast<unsigned long*>(value);
      ret = ss.str();
    }
    return ret;
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true)
      *static_cast<unsigned long*>(target) = NULL_U_LONG;
    else
      *static_cast<unsigned long*>(target) = strtoul(str, 0, 10);
  }
  Operate* DuplicateOperator() const {
    return new OperateULong(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<unsigned long*>(value) = NULL_U_LONG;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<unsigned long*>(value) == NULL_U_LONG)
      return true;
    return false;
  }
};
/**
 * Operate for string type
 */
class OperateString : public Operate {
 public:
  explicit OperateString(bool _nullable = true)
      : Operate(t_string, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true && *static_cast<char*>(value) == NULL_STRING)
      return "NULL";
    else
      return trimSpecialCharactor(string(static_cast<char*>(value)));
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true)
      *static_cast<char*>(target) = NULL_STRING;
    else
      strcpy(static_cast<char*>(target), str);
  }
  Operate* DuplicateOperator() const {
    return new OperateString(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<char*>(value) = NULL_STRING;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true && *static_cast<char*>(value) == NULL_STRING)
      return true;
    return false;
  }
};
/**
 * Operate for date type
 */
class OperateDate : public Operate {
 public:
  explicit OperateDate(bool _nullable = true) : Operate(t_date, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true &&
        static_cast<date*>(value)->is_neg_infinity() == true)
      return "NULL";
    else
      return to_iso_extended_string(*static_cast<date*>(value));
  }
  void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true) {
      SetNull(target);
    } else {
      string s(str);
      bool all_digit = false;
      if (s.length() == 8) {
        all_digit = true;
        for (int i = 0; i < 8; i++) {
          if (isdigit(s[i]) == 0) {
            all_digit = false;
            break;
          }
        }
      }
      if (all_digit == true)
        *static_cast<date*>(target) = from_undelimited_string(s);
      else
        *static_cast<date*>(target) = from_string(s);
    }
  }
  Operate* DuplicateOperator() const { return new OperateDate(this->nullable); }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    date d(NULL_DATE);
    *static_cast<date*>(value) = d;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true &&
        static_cast<date*>(value)->is_neg_infinity() == true)
      return true;
    return false;
  }
};

/**
 * Operate for time type
 */

class OperateTime : public Operate {
 public:
  explicit OperateTime(bool _nullable = true): Operate(t_time, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true &&
        static_cast<time_duration*>(value)->is_neg_infinity() == true)
      return "NULL";
    else
      return to_simple_string(*static_cast<time_duration*>(value));
  }
  inline void ToValue(void* target, const char* string) {
    if ((strcmp(string, "") == 0) && this->nullable == true)
      SetNull(target);
    else
      *static_cast<time_duration*>(target) = duration_from_string(string);
  }
  Operate* DuplicateOperator() const { return new OperateTime(this->nullable); }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    time_duration d(NULL_TIME);
    *static_cast<time_duration*>(value) = d;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true &&
        static_cast<time_duration*>(value)->is_neg_infinity() == true)
      return true;
    return false;
  }
};

/**
 * Operate for datetime type
 */
class OperateDatetime : public Operate {
 public:
  explicit OperateDatetime(bool _nullable = true):
  Operate(t_datetime, _nullable) {}
  inline string ToString(void* value) {
    if (this->nullable == true &&
        static_cast<ptime*>(value)->is_neg_infinity() == true)
      return "NULL";
    else
      return to_iso_extended_string(*static_cast<ptime*>(value));
  }
  inline void ToValue(void* target, const char* str) {
    if ((strcmp(str, "") == 0) && this->nullable == true)
      SetNull(target);
    else
      *static_cast<ptime*>(target) = time_from_string(str);
  }
  Operate* DuplicateOperator() const {
    return new OperateDatetime(this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    ptime d(NULL_DATETIME);
    *static_cast<ptime*>(value) = d;
    return true;
  }

  inline bool IsNull(void* value) const {
    if (this->nullable == true &&
        static_cast<ptime*>(value)->is_neg_infinity() == true)
      return true;
    return false;
  }
};
/**
 * Operate for decimal
 */
class OperateDecimal : public Operate {
 public:
  explicit OperateDecimal(unsigned _digit_len = 12, bool _nullable = true)
      :Operate(t_decimal, _nullable, _digit_len) {}
  inline string ToString(void* value) {
    if (this->nullable == true &&
        Compare(value, static_cast<void*>(&NULL_DECIMAL)) == 0)
      return "NULL";
    char buf[43] = {"\0"};
    ExportSerializeOutput out(buf, 43);
    static_cast<NValue*>(value)->serializeToExport(out, &digit_len);
    return string(buf + 4);
  }
  inline string toString(const NValue v, unsigned n_o_d_d = 12) {
    char buf[43] = {"\0"};
    ExportSerializeOutput out(buf, 43);
    (v).serializeToExport(out, &n_o_d_d);
    return string(buf + 4);
  }
  inline void ToValue(void* target, const char* string) {
    if ((strcmp(string, "") == 0) && this->nullable == true)
      *static_cast<NValue*>(target) = NULL_DECIMAL;
    else
      *static_cast<NValue*>(target) = NValue::getDecimalValueFromString(string);
  }
  Operate* DuplicateOperator() const {
    return new OperateDecimal(digit_len, this->nullable);
  }
  inline bool SetNull(void* value) {
    if (this->nullable == false) return false;
    *static_cast<NValue*>(value) = NULL_DECIMAL;
    return true;
  }
  inline bool IsNull(void* value) const {
    if (this->nullable == true &&
        Compare(value, static_cast<void*>(&NULL_DECIMAL)) == 0)
      return true;
    return false;
  }
};

/**
 *  The abstract of a table column.
 */
class ColumnType {
 public:
  /**
   * The constructor of column_type
   * @param type    the data type of this column
   * @param _size   the size of this column
   * @param _nullable  the column is allowed to be a null or not
   */
 ColumnType(data_type type, unsigned _size = 0, bool _nullable = true)
      : type(type), size(_size), nullable(_nullable) {
    this->initialize();
  }
 ColumnType(const ColumnType& r) {
    this->type = r.type;
    this->size = r.size;
    this->nullable = r.nullable;
    this->operate = r.operate->DuplicateOperator();
  }
  ColumnType() {
    this->type = t_int;
    this->size = 12;
    this->nullable = false;
    this->initialize();
  }
  ColumnType& operator=(const ColumnType& r) {
    this->type = r.type;
    this->size = r.size;
    this->nullable = r.nullable;
    this->operate = r.operate->DuplicateOperator();
  }
  ~ColumnType() {
    delete operate;
    operate = nullptr;
  }
 /**
  * get the length of this column
  */
  unsigned GetLength() const {
    if (type != t_string )
      return data_type_size_dic_abcx[type];
    else
      return byte_align(size);
  }
  bool operator==(const ColumnType& c) const {
    return this->type == c.type;
  }
  bool operator!=(const ColumnType& c) const {
    return this->type != c.type;
  }
  bool operator<(const class ColumnType& c) const {
    return this->type < c.type;
  }
  Operate* operate;
  data_type type;
  bool nullable;
  unsigned size;

 private:
  friend class boost::serialization::access;
  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& type& size& nullable;
    if (operate == nullptr) {
      initialize();
    }
  }
  /**
   * called after deserialization to construct the right operator.
   */
  void initialize() {
    switch (type) {
      case t_int:
        operate = new OperateInt(nullable);
        break;
      case t_float:
        operate = new OperateFloat(nullable);
        break;
      case t_double:
        operate = new OperateDouble(nullable);
        break;
      case t_string:
        operate = new OperateString(nullable);
        break;
      case t_u_long:
        operate = new OperateULong(nullable);
        break;
      case t_date:
        operate = new OperateDate(nullable);
        break;
      case t_time:
        operate = new OperateTime(nullable);
        break;
      case t_datetime:
        operate = new OperateDatetime(nullable);
        break;
      case t_decimal:
        operate = new OperateDecimal(nullable, size);
        break;
      case t_smallInt:
        operate = new OperateSmallInt(nullable);
        break;
      case t_u_smallInt:
        operate = new OperateUSmallInt(nullable);
        break;
      case t_boolean:
        operate = new OperateBool(nullable);
        break;
      default:
        operate = nullptr;
        break;
    }
  }
};

#endif  // COMMON_DATATYPE_H_
