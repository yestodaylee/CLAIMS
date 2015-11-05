/*
 * data_type.h
 *
 *  Created on: May 12, 2013
 *      Author: wangli
 */

#ifndef DATA_TYPE_H_
#define DATA_TYPE_H_
#include <assert.h>
#include <string.h>
#include <string>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <limits.h>
#include <float.h>

#include "hash.h"
#include "../utility/string_process.h"

using namespace boost::gregorian;
using namespace boost::posix_time;

#include "types/NValue.hpp"
using namespace decimal;
#define DATA_TYPE_NUMBER 20
enum data_type{t_smallInt,t_int,t_u_long,t_float,t_double,t_string, t_date, t_time, t_datetime, t_decimal, t_boolean, t_u_smallInt,t_date_day,t_date_week,t_date_month,t_date_year,t_date_quarter};
typedef void (*fun)(void*,void*);

#define NULL_SMALL_INT		SHRT_MAX
#define NULL_INT			INT_MAX
#define NULL_U_LONG			ULONG_LONG_MAX
#define NULL_FLOAT			FLT_MAX   //const transfor to int 2139095039
#define NULL_DOUBLE			DBL_MAX   //const transfor to int -1
#define NULL_STRING			'7'
#define NULL_DATE			neg_infin	// is_neg_infinity() return true
#define NULL_TIME			neg_infin
#define NULL_DATETIME		neg_infin
#define NULL_DECIMAL		nvalue_null
#define NULL_U_SMALL_INT	USHRT_MAX
#define NULL_BOOLEAN	    2


static NValue nvalue_null=NValue::getDecimalValueFromString("99999999999999999999999999.999999999999");

//static int count_open_for_data_column=0;

/**
 * the number of bytes that are aligned between any two adjacent data types
 */
#define allign_bytes 4
#define byte_align(size) (((size-1)/allign_bytes+1)*allign_bytes)

template<typename T>
inline void Add_FUNC(void* target, void* increment)
{
	*(T*)target+=*(T*)increment;
}
template<typename T>
inline void MULTIPLE(void *target,void *increment)
{
	(*(T*)target)=(*(T*)target)*(*(T*)increment);
}
template<>
inline void Add_FUNC<char*>(void* target, void* increment)
{
}

template<>
inline void Add_FUNC<NValue*>(void* target, void* increment)
{
	*(NValue*)target = ((NValue*)target)->op_add(*(NValue*)increment);
}
template<>
inline void Add_FUNC<date*>(void* target, void* increment)
{
	*(date*)target = *(date*)target + *(date_duration*)increment;
}
template<>
inline void Add_FUNC<ptime*>(void* target, void* increment)
{
	*(ptime*)target = *(ptime*)target + *(time_duration*)increment;
}
template<>
inline void Add_FUNC<time_duration*>(void* target, void* increment)
{
	*(time_duration*)target = *(time_duration*)target + *(time_duration*)increment;
}

template<typename T>
inline void MIN(void* target, void* increment)
{
	if(*(T*)target>*(T*)increment)
		*(T*)target=*(T*)increment;
}
template<>
inline void MIN<char*>(void* target, void* increment)
{
}
template<>
inline void MIN<NValue*>(void* target, void* increment)
{
	*(NValue*)target = ((NValue*)target)->op_min(*(NValue*)increment);
}

template<typename T>
inline void MAX(void* target, void* increment)
{
	if(*(T*)target<*(T*)increment)
		*(T*)target=*(T*)increment;
}
template<>
inline void MAX<char*>(void* target, void* increment)
{

}
template<>
inline void MAX<NValue*>(void* target, void* increment)
{
	*(NValue*)target = ((NValue*)target)->op_max(*(NValue*)increment);
}

template<typename T>
inline void IncreaseByOne(void* target,void* increment)
{
	(*(T*)target)++;
}
template<>
inline void IncreaseByOne<char*>(void* target,void* increment)
{

}

template<>
inline void IncreaseByOne<NValue*>(void* target,void* increment)
{
	NValue nv1 = NValue::getDecimalValueFromString("1");
	*(NValue*)target = ((NValue*)target)->op_add(nv1);
}
template<typename T>//暂时先实现这点
inline void Add_IncreaseByOne(void* target,void* increment)
{
//	*(T*)target+=*(T*)increment;//Add
//	(*(T*)target)++;//increase
}
template<>
inline void Add_IncreaseByOne<char*>(void* target,void* increment)
{

}

template<>
inline void Add_IncreaseByOne<NValue*>(void* target,void* increment)
{
	*(NValue*)target = ((NValue*)target)->op_add(*(NValue*)increment);//Add
	NValue nv1 = NValue::getDecimalValueFromString("1");
	*(NValue*)target = ((NValue*)target)->op_add(nv1);
}
//template<>
//inline void Add_IncreaseByOne<date*>(void* target, void* increment)
//{
//	*(date*)target = *(date*)target + *(date_duration*)increment;
//	(*(date*)target)++;
//}
//template<>
//inline void Add_IncreaseByOne<ptime*>(void* target, void* increment)
//{
//	*(ptime*)target = *(ptime*)target + *(date_duration*)increment;
//	(*(ptime*)target)++;
//}
//template<>
//inline void Add_IncreaseByOne<time_duration*>(void* target, void* increment)
//{
//	*(time_duration*)target = *(time_duration*)target + *(time_duration*)increment;
//	(*(time_duration*)target)++;
//}


template<typename T>
inline void assigns(const void* const& src, void* const &desc){
	*(T*)desc=*(T*)src;
}
template<>
inline void assigns<char*>(const void* const& src, void* const &desc){
	strcpy((char*)desc,(char*)src);
}
class Operate
{
public:
	Operate(bool _nullable = true):nullable(_nullable){};
	virtual ~Operate(){};
	inline void ass(void* src, void* desc){
		*(int*)desc=*(int*)src;
	}
	inline virtual void Assign(const void* const& src, void* const &desc) const =0;
	virtual unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const=0;
	virtual unsigned GetPartitionValue(const void* key, const unsigned long & mod)const=0;
	virtual unsigned GetPartitionValue(const void* key)const=0;
	virtual std::string ToString(void* value)=0;
	virtual void ToValue(void* target, const char* string)=0;
	virtual bool Equal(const void* const &a, const void* const & b)const=0;
	virtual bool Less(const void*& a, const void*& b)const=0;
	virtual bool Greate(const void*& a, const void*& b)const=0;
	virtual void Add(void* target, void* increment)=0;
	virtual void Multiply(void* target, void* increment)=0;
	virtual int Compare(const void* a,const void* b)const=0;
	virtual fun GetADDFunction()=0;
	virtual fun GetMINFunction()=0;
	virtual fun GetMAXFunction()=0;
	virtual fun GetIncreateByOneFunction()=0;
	virtual fun GetAVGFunction()=0;
	void (*assign)(const void* const& src, void* const &desc);
	virtual Operate* DuplicateOperator()const=0;

	inline virtual bool SetNull(void* value) = 0;
	inline virtual bool IsNull(void* value) const = 0;

public:
	bool nullable;
};

class OperateInt:public Operate
{
public:
	OperateInt(bool _nullable = true){this->nullable = _nullable; assign=assigns<int>;};
	~OperateInt(){};
	inline void Assign(const void* const& src, void* const &desc)const
	{
		*(int*)desc=*(int*)src;
	};
	inline std::string ToString( void* value)
	{
		std::string ret;
		if (this->nullable == true && (*(int*)value) == NULL_INT)
			ret = "NULL";
		else
		{
			std::ostringstream ss;
			ss<<*(int*)value;
			ret=ss.str();
		}
		return ret;
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(int*)target = NULL_INT;
		else
			*(int*)target=atoi(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(int*)a==*(int*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(int*)a<*(int*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(int*)a>*(int*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(int*)a-*(int*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<int>(target,increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<int>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<int>;
	}
	inline fun GetMINFunction()
	{
		return MIN<int>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<int>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<int>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<int>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(int*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(int*)key);
//				boost::hash_value(*(int*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(int*)key)%mod;
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(int*)value = NULL_INT;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(int*)value) == NULL_INT)
			return true;
		return false;
	}

	Operate* DuplicateOperator()const{
		return new OperateInt(this->nullable);
	}
};

class OperateFloat:public Operate
{
public:
	OperateFloat(bool _nullable = true){ this->nullable = _nullable; };
	~OperateFloat(){};
	inline void Assign(const void* const& src, void* const &desc)const
	{
		*(float*)desc=*(float*)src;
	};
	inline std::string ToString(void* value)
	{
		std::string ret;
		if (this->nullable == true && (*(float*)value) == NULL_FLOAT)
			ret = "NULL";
		else
		{
			std::ostringstream ss;
			ss<<*(float*)value;
			ret=ss.str();
		}
		return ret;
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(float*)target = NULL_FLOAT;
		else
			*(float*)target=atof(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(float*)a==*(float*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(float*)a<*(float*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(float*)a>*(float*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(float*)a-*(float*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<float>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<float>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<float>;
	}
	inline fun GetMINFunction()
	{
		return MIN<float>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<float>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<float>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<float>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(float*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(float*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(float*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateFloat(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(float*)value = NULL_FLOAT;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(int*)value) == (int)NULL_FLOAT)
			return true;
		return false;
	}
};

class OperateDouble:public Operate
{
public:
	OperateDouble(bool _nullable = true){ this->nullable = _nullable; };
	~OperateDouble(){};
	inline void Assign(const void* const& src, void* const &desc)const
	{
		*(double*)desc=*(double*)src;
	};
	inline std::string ToString(void* value)
	{
		std::string ret;
		if (this->nullable == true && (*(double*)value) == NULL_DOUBLE)
			ret = "NULL";
		else
		{
			std::ostringstream ss;
			ss<<*(double*)value;
			ret=ss.str();
		}
		return ret;
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(double*)target = NULL_DOUBLE;
		else
			*(double*)target=atof(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(double*)a==*(double*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(double*)a<*(double*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(double*)a>*(double*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(double*)a-*(double*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<double>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<double>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<double>;
	}
	inline fun GetMINFunction()
	{
		return MIN<double>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<double>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<double>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<double>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(double*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(double*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(double*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateDouble(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(double*)value = NULL_DOUBLE;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(int*)value) == (int)NULL_DOUBLE)
			return true;
		return false;
	}
};

class OperateULong:public Operate
{
public:
	OperateULong(bool _nullable = true){ this->nullable = _nullable; };
	~OperateULong(){};
	inline void Assign(const void* const& src, void* const &desc)const
	{
		*(unsigned long*)desc=*(unsigned long*)src;
	};
	inline std::string ToString(void* value)
	{
		std::string ret;
		if (this->nullable == true && (*(unsigned long*)value) == NULL_U_LONG)
			ret = "NULL";
		else
		{
			std::ostringstream ss;
			ss<<*(unsigned long*)value;
			ret=ss.str();
		}
		return ret;
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(unsigned long*)target = NULL_U_LONG;
		else
			*(unsigned long*)target=strtoul(string,0,10);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(unsigned long*)a==*(unsigned long*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(unsigned long*)a<*(unsigned long*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(unsigned long*)a>*(unsigned long*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(unsigned long*)a-*(unsigned long*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<unsigned long>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<unsigned long>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<unsigned long>;
	}
	inline fun GetMINFunction()
	{
		return MIN<unsigned long>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<unsigned long>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<unsigned long>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<unsigned long>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(unsigned long*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(unsigned long*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(unsigned long*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateULong(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(unsigned long*)value = NULL_U_LONG;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(unsigned long*)value) == NULL_U_LONG)
			return true;
		return false;
	}
};



class OperateString:public Operate
{
public:
	OperateString(bool _nullable = true){ this->nullable = _nullable; };
	~OperateString(){};
	inline void Assign(const void* const& src, void* const &desc)const
	{
		assert(desc!=0&&src!=0);
		strcpy((char*)desc,(char*)src);
	};
	inline std::string ToString(void* value)
	{
		if (this->nullable == true && (*(char*)value) == NULL_STRING)
			return "NULL";
		else
			return trimSpecialCharactor(std::string((char*)value));
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(char*)target = NULL_STRING;
		else
			strcpy((char*)target,string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return strcmp((char*)a,(char*)b)==0;
	}

	/**
	 * The following function may return a wrong value
	 */
	bool Less(const void*& a, const void*& b)const{
		return strcmp((char*)a,(char*)b)<0;
	}
	bool Greate(const void*& a, const void*& b)const{
		return strcmp((char*)a,(char*)b)>0;
	}
	int Compare(const void* a,const void* b)const{
		return strcmp((char*)a,(char*)b);
	}
	inline void Add(void* target, void* increment)
	{
		//TODO: throw exception or implement the Add for string.
		printf("The sum for String is not current supported!\n");
	}
	inline void Multiply(void* target, void* increment)
	{
		//TODO: throw exception or implement the Add for string.
		printf("The sum for String is not current supported!\n");
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<char*>;
	}
	inline fun GetMINFunction()
	{
		return MIN<char*>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<char*>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<char*>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<char *>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		printf("The hash function for char[] type is not implemented yet!\n");
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value((std::string)((char*)key));
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value((std::string)((char*)key))%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateString(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(char*)value = NULL_STRING;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(char*)value) == NULL_STRING)
			return true;
		return false;
	}
};

class OperateDate:public Operate
{
public:
	OperateDate(bool _nullable = true){ this->nullable = _nullable; };
//	~OperateDate(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		assert(desc!=0&&src!=0);
		*(date*)desc = *(date*)src;
	};
	inline std::string ToString(void* value)
	{
		if (this->nullable == true && (*(date*)value).is_neg_infinity() == true)
			return "NULL";
		else
			return to_iso_extended_string(*(date*)value);
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			SetNull(target);
		else
		{
			std::string s(string);
			bool all_digit = false;
			if (s.length() == 8)
			{
				all_digit = true;
				for (int i = 0; i < 8; i++)
				{
					if (isdigit(s[i]) == 0)
					{
						all_digit = false;
						break;
					}
				}

			}
			if (all_digit == true)
				*(date*)target = from_undelimited_string(s);
			else
				*(date*)target = from_string(s);
		}
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(date*)a == *(date*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(date*)a < *(date*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(date*)a > *(date*)b;
	}
	int Compare(const void* a,const void* b)const{
		if (*(date*)a < *(date*)b)
			return -1;
		else if (*(date*)a > *(date*)b)
			return 1;
		return 0;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<date*>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		//TODO: throw exception or implement the Add for string.
		printf("The sum for String is not current supported!\n");
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<date*>;
	}
	inline fun GetMINFunction()
	{
		return MIN<date*>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<date*>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<date*>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<date *>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		printf("The hash function for date type is not implemented yet!\n");
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value((*(boost::gregorian::date*)(key)).julian_day());
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value((*(boost::gregorian::date*)(key)).julian_day())%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateDate(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		date d(NULL_DATE);
		*(date*)value = d;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(date*)value).is_neg_infinity() == true)
			return true;
		return false;
	}
};

class OperateTime:public Operate
{
public:
	OperateTime(bool _nullable = true){ this->nullable = _nullable; };
//	~OperateTime(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		assert(desc!=0&&src!=0);
		*(time_duration*)desc = *(time_duration*)src;
	};
	inline std::string ToString(void* value)
	{
		if (this->nullable == true && (*(time_duration*)value).is_neg_infinity() == true)
			return "NULL";
		else
			return to_simple_string(*(time_duration*)value);
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			SetNull(target);
		else
			*(time_duration*)target = duration_from_string(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(time_duration*)a == *(time_duration*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(time_duration*)a < *(time_duration*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(time_duration*)a > *(time_duration*)b;
	}
	int Compare(const void* a,const void* b)const{
		if (*(time_duration*)a < *(time_duration*)b)
			return -1;
		else if (*(time_duration*)a > *(time_duration*)b)
			return 1;
		return 0;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<time_duration*>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		//TODO: throw exception or implement the Add for string.
		printf("The sum for String is not current supported!\n");
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<time_duration*>;
	}
	inline fun GetMINFunction()
	{
		return MIN<time_duration*>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<time_duration*>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<time_duration*>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<time_duration*>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		printf("The hash function for time type is not implemented yet!\n");
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value((*(time_duration*)(key)).total_nanoseconds());
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value((*(time_duration*)(key)).total_nanoseconds())%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateTime(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		time_duration d(NULL_TIME);
		*(time_duration*)value = d;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(time_duration*)value).is_neg_infinity() == true)
			return true;
		return false;
	}
};

class OperateDatetime:public Operate
{
public:
	OperateDatetime(bool _nullable = true){ this->nullable = _nullable; };
//	~OperateDatetime(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		assert(desc!=0&&src!=0);
		*(ptime*)desc = *(ptime*)src;
	};
	inline std::string ToString(void* value)
	{
		if (this->nullable == true && (*(ptime*)value).is_neg_infinity() == true)
			return "NULL";
		else
			return to_iso_extended_string(*(ptime*)value);
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			SetNull(target);
		else
			*(ptime*)target = time_from_string(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(ptime*)a == *(ptime*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(ptime*)a < *(ptime*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(ptime*)a > *(ptime*)b;
	}
	int Compare(const void* a,const void* b)const{
		if (*(ptime*)a < *(ptime*)b)
			return -1;
		else if (*(ptime*)a > *(ptime*)b)
			return 1;
		return 0;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<ptime*>(target, increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		//TODO: throw exception or implement the Add for string.
		printf("The sum for String is not current supported!\n");
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<ptime*>;
	}
	inline fun GetMINFunction()
	{
		return MIN<ptime*>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<ptime*>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<ptime*>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<ptime *>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		printf("The hash function for datetime type is not implemented yet!\n");
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(to_simple_string(*(ptime*)(key)))%mod;
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(to_simple_string(*(ptime*)(key)));
		//TODO: maybe there is a more efficient way.
	}
	Operate* DuplicateOperator()const{
		return new OperateDatetime(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		ptime d(NULL_DATETIME);
		*(ptime*)value = d;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(ptime*)value).is_neg_infinity() == true)
			return true;
		return false;
	}
};

class OperateSmallInt:public Operate
{
public:
	OperateSmallInt(bool _nullable = true){ this->nullable = _nullable; assign=assigns<short>;};
//	~OperateSmallInt(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		*(short*)desc=*(short*)src;
	};
	inline std::string ToString( void* value)
	{
		if (this->nullable == true && (*(short*)value) == NULL_SMALL_INT)
			return "NULL";
		else
		{
			std::ostringstream ss;
			ss<<*(short*)value;
			std::string ret=ss.str();
			return ret;
		}
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)//modified by Li Wang in Sep.10th
//		if(string==0 && this->nullable ==true)
			*(short*)target = NULL_SMALL_INT;
		else
			*(short*)target = (short)atoi(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(short*)a==*(short*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(short*)a < *(short*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(short*)a > *(short*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(short*)a - *(short*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<short>(target,increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<short>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<short>;
	}
	inline fun GetMINFunction()
	{
		return MIN<short>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<short>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<short>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<short>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(short*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(short*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(short*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateSmallInt(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(short*)value = NULL_SMALL_INT;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(short*)value) == NULL_SMALL_INT)
			return true;
		return false;
	}
};

class OperateUSmallInt:public Operate
{
public:
	OperateUSmallInt(bool _nullable = true){ this->nullable = _nullable; assign=assigns<unsigned short>;};
//	~OperateSmallInt(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		*(unsigned short*)desc=*(unsigned short*)src;
	};
	inline std::string ToString( void* value)
	{
		if (this->nullable == true && (*(unsigned short*)value) == NULL_U_SMALL_INT)
			return "NULL";
		std::ostringstream ss;
		ss<<*(unsigned short*)value;
		std::string ret=ss.str();
		return ret;
	};
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(unsigned short*)target = NULL_U_SMALL_INT;
		else
			*(unsigned short*)target = (unsigned short)atoi(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(unsigned short*)a==*(unsigned short*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(unsigned short*)a < *(unsigned short*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(unsigned short*)a > *(unsigned short*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(unsigned short*)a - *(unsigned short*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<unsigned short>(target,increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<unsigned short>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<unsigned short>;
	}
	inline fun GetMINFunction()
	{
		return MIN<unsigned short>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<unsigned short>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<unsigned short>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<unsigned short>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(unsigned short*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(unsigned short*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(unsigned short*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateUSmallInt(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(unsigned short*)value = NULL_U_SMALL_INT;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(unsigned short*)value) == NULL_U_SMALL_INT)
			return true;
		return false;
	}
};

class OperateDecimal:public Operate
{
public:
	OperateDecimal(unsigned number_of_decimal_digits = 12, bool _nullable = true):number_of_decimal_digits_(number_of_decimal_digits){assign=assigns<int>; this->nullable = _nullable; };
//	~OperateDecimal(){};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		*(NValue*)desc=*(NValue*)src;
	};
	inline std::string ToString( void* value)
	{
		if (this->nullable == true && Compare(value, (void*)(&NULL_DECIMAL)) == 0)
			return "NULL";
		char buf[43] = {"\0"};
		ExportSerializeOutput out(buf, 43);
		((NValue*)value)->serializeToExport(out,&number_of_decimal_digits_);
		return std::string(buf+4);
	};
	static std::string ToString(const NValue v,unsigned n_o_d_d=12){
//		if (this->nullable == true && compare(v, (void*)(&NULL_DECIMAL)) == 0)
//			return "NULL";
		char buf[43] = {"\0"};
		ExportSerializeOutput out(buf, 43);
		(v).serializeToExport(out,&n_o_d_d);
		return std::string(buf+4);
	}
	void ToValue(void* target, const char* string){
		if ((strcmp(string,"")==0) && this->nullable == true)
			*(NValue*)target = NULL_DECIMAL;
		else
			*(NValue*)target = NValue::getDecimalValueFromString(string);
	};
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return ((NValue*)a)->op_equals(*(NValue*)b);
	}
	bool Less(const void*& a, const void*& b)const{
		if (((NValue*)a)->op_equals(*(NValue*)b))
			return false;
		NValue tmp = ((NValue*)a)->op_min(*(NValue*)b);
		if (tmp.op_equals(*(NValue*)a))
			return true;
		return false;
	}
	bool Greate(const void*& a, const void*& b)const{
		if (((NValue*)a)->op_equals(*(NValue*)b))
			return false;
		NValue tmp = ((NValue*)a)->op_min(*(NValue*)b);
		if (tmp.op_equals(*(NValue*)a))
			return false;
		return true;
	}
	int Compare(const void* a,const void* b)const{
		if ((*(NValue*)a).op_equals(*(NValue*)b))
			return 0;
		else if (Less(a,b))
			return -1;
		return 1;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<NValue*>(target, increment);
//		((NValue*)target)->op_Add(*(NValue*)increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		(*(NValue*)target)=((NValue*)target)->op_multiply(*(NValue*)increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<NValue*>;
	}
	inline fun GetMINFunction()
	{
		return MIN<NValue*>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<NValue*>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<NValue*>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<NValue *>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
//		return partition_function->get_partition_value(*(NValue*)key);
//		printf("The hash function for decimal type is not implemented yet!\n");
		unsigned long ul1 = *(unsigned long*)((*(NValue*)key).m_data);
		unsigned long ul2 = *(unsigned long*)((*(NValue*)key).m_data+8);
		return partition_function->get_partition_value(ul1+ul2);
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key)const{
//		return boost::hash_value(*(NValue*)key);
//		printf("The hash function for decimal type is not implemented yet!\n");
		unsigned long ul1 = *(unsigned long*)((*(NValue*)key).m_data);
		unsigned long ul2 = *(unsigned long*)((*(NValue*)key).m_data+8);
		boost::hash_combine(ul1,ul2);
		return ul1;
		assert(false);

		return 0;
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		unsigned long ul1 = *(unsigned long*)((*(NValue*)key).m_data);
		unsigned long ul2 = *(unsigned long*)((*(NValue*)key).m_data+8);
		boost::hash_combine(ul1,ul2);
		return ul1%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateDecimal(number_of_decimal_digits_, this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(NValue*)value = NULL_DECIMAL;
		return true;
	}

	inline bool IsNull(void* value) const
	{
//		compare(value, (void*)(&NULL_DECIMAL));
		if (this->nullable == true && Compare(value, (void*)(&NULL_DECIMAL)) == 0)
			return true;
		return false;
	}

	unsigned number_of_decimal_digits_;
};

class OperateBool:public Operate{
public:
	OperateBool(bool _nullable = true){ this->nullable = _nullable; assign=assigns<int>;};
	inline void Assign(const void* const &src,void* const &desc)const
	{
		*(int *)desc=*(int *)src;
	};
	inline std::string ToString( void* value)
	{
		if (this->nullable == true && (*(int*)value) == NULL_BOOLEAN)
			return "NULL";
		else
		{
			std::ostringstream ss;
			if(*(int *)value==0)
				return "FALSE";
			else
				return "TRUE";
		}
	}
	inline void ToValue(void* target, const char* string){
		std::string f="FALSE";
		std::string t="TRUE";
		std::string n="NULL";
		if ((strcmp(string,n.c_str())==0) && this->nullable == true){
			*(int*)target = NULL_BOOLEAN;
		}
		else if(strcmp(f.c_str(),string)==0){
			*(int *)target = 0;
		}
		else{
			*(int *)target = 1;
		}
	}
	inline bool Equal(const void* const &a, const void* const & b)const
	{
		return *(int*)a==*(int*)b;
	}
	bool Less(const void*& a, const void*& b)const{
		return *(int*)a < *(int*)b;
	}
	bool Greate(const void*& a, const void*& b)const{
		return *(int*)a > *(int*)b;
	}
	int Compare(const void* a,const void* b)const{
		return *(int*)a - *(int*)b;
	}
	inline void Add(void* target, void* increment)
	{
		Add_FUNC<int>(target,increment);
	}
	inline void Multiply(void* target, void* increment)
	{
		MULTIPLE<int>(target, increment);
	}
	inline fun GetADDFunction()
	{
		return Add_FUNC<int>;
	}
	inline fun GetMINFunction()
	{
		return MIN<int>;
	}
	inline fun GetMAXFunction()
	{
		return MAX<int>;
	}
	inline fun GetIncreateByOneFunction()
	{
		return IncreaseByOne<int>;
	}
	inline fun 	GetAVGFunction()
	{
		return Add_IncreaseByOne<int>;
	}
	unsigned GetPartitionValue(const void* key,const PartitionFunction* partition_function)const{
		return partition_function->get_partition_value(*(int*)key);
	}
	unsigned GetPartitionValue(const void* key)const{
		return boost::hash_value(*(int*)key);
	}
	unsigned GetPartitionValue(const void* key, const unsigned long & mod)const{
		return boost::hash_value(*(int*)key)%mod;
	}
	Operate* DuplicateOperator()const{
		return new OperateBool(this->nullable);
	}

	inline bool SetNull(void* value)
	{
		if (this->nullable == false)
			return false;
		*(int*)value = NULL_BOOLEAN;
		return true;
	}

	inline bool IsNull(void* value) const
	{
		if (this->nullable == true && (*(int*)value) == NULL_SMALL_INT)
			return true;
		return false;
	}
};

class column_type
{
public:
	 column_type(data_type type,unsigned _size=0, bool _nullable=true):type(type),size(_size), nullable(_nullable){
		switch(type)
		{
			case t_int:operate=new OperateInt(_nullable);break;
			case t_float:operate=new OperateFloat(_nullable);break;
			case t_string:operate=new OperateString(_nullable);break;
			case t_double:operate=new OperateDouble(_nullable);break;
			case t_u_long:operate=new OperateULong(_nullable);break;
			case t_date: operate = new OperateDate(_nullable);break;
			case t_time: operate = new OperateTime(_nullable);break;
			case t_datetime: operate = new OperateDatetime(_nullable);break;

			case t_decimal: operate = new OperateDecimal(size, _nullable);break;
			case t_smallInt: operate = new OperateSmallInt(_nullable);break;
			case t_u_smallInt: operate = new OperateUSmallInt(_nullable);break;
			case t_boolean: operate = new OperateBool(_nullable);break;
			default:operate=0;break;
		}
	};
	 column_type(const column_type &r){
		 this->type=r.type;
		 this->size=r.size;
		 this->nullable = r.nullable;
		 this->operate=r.operate->DuplicateOperator();
	 }
	 column_type& operator=(const column_type &r){
		 this->type=r.type;
		 this->size=r.size;
		 this->nullable = r.nullable;
		 this->operate=r.operate->DuplicateOperator();
	 }
	column_type():operate(0){};
	~column_type(){
		delete operate;
		operate=0;
	};
	inline unsigned get_length() const
	{
		switch(type)
		{
			case t_int: return sizeof(int);
			case t_float:return sizeof(float);
			case t_double:return sizeof(double);
			case t_u_long:return sizeof(unsigned long);
			case t_string:return byte_align(size);
			case t_date: return sizeof(date);
			case t_time: return sizeof(time_duration);
			case t_datetime: return sizeof(ptime);
			case t_decimal: return 16;
			case t_smallInt: return sizeof(short);
			case t_u_smallInt: return sizeof(unsigned short);
			case t_boolean: return sizeof(int);
			default: return 0;

		}
	}
	bool operator==(const class column_type &c) const
	{
		return this->type==c.type;
	}
	bool operator<(const class column_type &c) const
	{

		return this->type<c.type;
	}
public:
	Operate* operate;
	//这个data_type是什么type
	data_type type;
	bool nullable;
	unsigned size;
private:
	//且这个data_type的size是多少
	friend class boost::serialization::access;
	template<class Archive>
	void serialize(Archive &ar, const unsigned int version)
	{
		ar & type & size & nullable;
		if(operate==0)
		{
			initialize();
		}
	}
	/**
	 * called after deserialization to construct the right operator.
	 */
	void initialize()
	{
		switch(type)
		{
			case t_int:operate=new OperateInt(nullable);break;
			case t_float:operate=new OperateFloat(nullable);break;
			case t_double:operate=new OperateDouble(nullable);break;
			case t_string:operate=new OperateString(nullable);break;
			case t_u_long:operate=new OperateULong(nullable);break;
			case t_date: operate = new OperateDate(nullable);break;
			case t_time: operate = new OperateTime(nullable);break;
			case t_datetime: operate = new OperateDatetime(nullable);break;
			case t_decimal: operate = new OperateDecimal(size, nullable);break;
			case t_smallInt: operate = new OperateSmallInt(nullable);break;
			case t_u_smallInt: operate = new OperateUSmallInt(nullable);break;
			case t_boolean: operate = new OperateBool(nullable);break;
			default:operate=0;break;
		}
	}
};


#endif /* DATA_TYPE_H_ */
