#include "../physical_query_plan/BlockStreamAggregationIterator.h"


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
 * ./LogicalPlan/logical_aggregation.h
 *
 *  Created on: Nov 11,2013
 *      Author: wangli, fangzhuhe
 *       Email: fzhedu@gmail.com
 *
 * Description: Aggregation operator is designed for executing group by and
 * aggregation function. LogicalAggregation is the logical type of Aggregation
 * operator, main function includes getting plan context after executing this
 * operator and generating corresponding physical operator.
 */
#define GLOG_NO_ABBREVIATED_SEVERITIES  // avoid macro conflict
#include "../logical_query_plan/logical_aggregation.h"
#include <vector>
#include "../logical_query_plan/plan_context.h"
#include "../IDsGenerator.h"
#include "../physical_query_plan/ExpandableBlockStreamExchangeEpoll.h"
#include "../physical_query_plan/BlockStreamExpander.h"
#include "../Catalog/stat/StatManager.h"
#include "../Config.h"
#include <glog/logging.h>
namespace claims {
namespace logical_query_plan {
LogicalAggregation::LogicalAggregation(
    std::vector<Attribute> group_by_attribute_list,
    std::vector<Attribute> aggregation_attribute_list,
    std::vector<BlockStreamAggregationIterator::State::aggregation>
        aggregation_function_list, LogicalOperator* child)
    : group_by_attribute_list_(group_by_attribute_list),
      aggregation_attribute_list_(aggregation_attribute_list),
      aggregation_function_list_(aggregation_function_list),
      plan_context_(NULL),
      child_(child) {
  assert(aggregation_attribute_list_.size() ==
         aggregation_function_list_.size());
  set_operator_type(kLogicalAggregation);
}

LogicalAggregation::~LogicalAggregation() {
  if (NULL != plan_context_) {
    delete plan_context_;
    plan_context_ = NULL;
  }
  if (NULL != child_) {
    delete child_;
    child_ = NULL;
  }
}
PlanContext LogicalAggregation::GetPlanContext() {
  if (NULL != plan_context_) return *plan_context_;
  PlanContext ret;
  const PlanContext child_context = child_->GetPlanContext();
  if (CanOmitHashRepartition(child_context)) {
    aggregation_style_ = kAgg;
    LOG(INFO) << "Aggregation style: kAgg" << std::endl;
  } else {  // as for the kLocalAggReparGlobalAgg style is optimal
            // to kReparAndGlobalAgg so it's set to be default.
    aggregation_style_ = kLocalAggReparGlobalAgg;
    LOG(INFO) << "Aggregation style: kLocalAggReparGlobalAgg" << std::endl;
  }
  switch (aggregation_style_) {
    case kAgg: {
      ret.attribute_list_ = GetAttrsAfterAgg();
      ret.commu_cost_ = child_context.commu_cost_;
      ret.plan_partitioner_ =
          child_context.plan_partitioner_;
      Attribute partition_key =
          child_context.plan_partitioner_.get_partition_key();
      partition_key.table_id_ = INTERMEIDATE_TABLEID;
      ret.plan_partitioner_.set_partition_key(partition_key);
      for (unsigned i = 0;
           i < ret.plan_partitioner_.GetNumberOfPartitions(); i++) {
        const unsigned cardinality =
            ret.plan_partitioner_.GetPartition(i)
                ->get_cardinality();
        ret.plan_partitioner_.GetPartition(i)->set_cardinality(
            EstimateGroupByCardinality(child_context) /
            ret.plan_partitioner_.GetNumberOfPartitions());
      }
      break;
    }
    default: {
      /**
       * repartition aggregation is currently simplified.
       */

      // TODO(fzh): ideally, the partition properties (especially the the number
      // of partitions and partition style) after repartition aggregation should
      // be decided by the partition property enforcement.
      ret.attribute_list_ = GetAttrsAfterAgg();
      ret.commu_cost_ = child_context.commu_cost_ +
                                  child_context.GetAggregatedDatasize();
      ret.plan_partitioner_.set_partition_func(
          child_context.plan_partitioner_.get_partition_func());
      if (group_by_attribute_list_.empty()) {
        ret.plan_partitioner_.set_partition_key(Attribute());
      } else {
        const Attribute partition_key = group_by_attribute_list_[0];
        ret.plan_partitioner_.set_partition_key(partition_key);
      }
      NodeID location = 0;
      unsigned long data_cardinality =
          EstimateGroupByCardinality(child_context);
      PartitionOffset offset = 0;
      PlanPartitionInfo par(offset, data_cardinality, location);
      std::vector<PlanPartitionInfo> partition_list;
      partition_list.push_back(par);
      ret.plan_partitioner_.set_partition_list(partition_list);
      break;
    }
  }
  plan_context_ = new PlanContext();
  *plan_context_ = ret;
  return ret;
}

bool LogicalAggregation::CanOmitHashRepartition(
    const PlanContext& child_plan_context) const {
  if (child_plan_context.plan_partitioner_.GetNumberOfPartitions() == 1 &&
      child_plan_context.plan_partitioner_.GetPartition(0)
              ->get_location() == 0)
    return true;
  if (!child_plan_context.IsHashPartitioned()) return false;

  /**
   * the hash property of the input data can be leveraged in the
   * aggregation as long as the hash attribute is one of the group-by
   * attributes.
   */
  const Attribute partition_key =
      child_plan_context.plan_partitioner_.get_partition_key();
  for (unsigned i = 0; i < group_by_attribute_list_.size(); i++) {
    if (group_by_attribute_list_[i] == partition_key) return true;
  }
  return false;
}

void LogicalAggregation::ChangeSchemaForAVG(
    BlockStreamAggregationIterator::State& state_) {
  state_.avgIndex.clear();
  for (unsigned i = 0; i < state_.aggregations.size(); i++) {
    if (state_.aggregations[i] == BlockStreamAggregationIterator::State::avg) {
      state_.aggregations[i] = BlockStreamAggregationIterator::State::sum;
      state_.avgIndex.push_back(i);
    }
  }
  state_.hashSchema = state_.output->duplicateSchema();

  // if the agg style isn kLocalAggReparGlobalAgg
  // at local, output_schema = hash_schema, but for global,
  // input_schema = hash_schema
  // the hash_schema has to replace the column of avg() with sum() and append
  // one count() column at the end.

  if (state_.avgIndex.size() > 0) {
    ColumnType count_column_type = ColumnType(t_u_long, 8);
    state_.hashSchema->addColumn(count_column_type, 8);
    if (state_.agg_node_type ==
        BlockStreamAggregationIterator::State::Hybrid_Agg_Private) {
      state_.aggregations.push_back(
          BlockStreamAggregationIterator::State::count);
      state_.aggregationIndex.push_back(state_.aggregationIndex.size() +
                                        state_.groupByIndex.size());
      state_.output = state_.hashSchema->duplicateSchema();
    } else if (state_.agg_node_type ==
               BlockStreamAggregationIterator::State::Hybrid_Agg_Global) {
      state_.aggregations.push_back(BlockStreamAggregationIterator::State::sum);
      state_.aggregationIndex.push_back(state_.aggregationIndex.size() +
                                        state_.groupByIndex.size());
      state_.input = state_.hashSchema->duplicateSchema();
    } else if (state_.agg_node_type ==
               BlockStreamAggregationIterator::State::Not_Hybrid_Agg) {
      state_.aggregations.push_back(
          BlockStreamAggregationIterator::State::count);
      state_.aggregationIndex.push_back(state_.aggregationIndex.size() +
                                        state_.groupByIndex.size());
    }
  }
}
/**
 * Note: if group_by_attribute_list_ is empty, the partition key is
 * ATTRIBUTE_NULL
 */
BlockStreamIteratorBase* LogicalAggregation::GetPhysicalPlan(
    const unsigned& block_size) {
  if (NULL == plan_context_) {
    GetPlanContext();
  }
  BlockStreamIteratorBase* ret;
  const PlanContext child_plan_context = child_->GetPlanContext();
  BlockStreamAggregationIterator::State aggregation_state;
  aggregation_state.groupByIndex =
      GetInvolvedAttrIdList(group_by_attribute_list_, child_plan_context);
  aggregation_state.aggregationIndex =
      GetInvolvedAttrIdList(aggregation_attribute_list_, child_plan_context);
  aggregation_state.aggregations = aggregation_function_list_;
  aggregation_state.block_size = block_size;
  aggregation_state.nbuckets = EstimateGroupByCardinality(child_plan_context);
  aggregation_state.bucketsize = 64;
  aggregation_state.input = GetSchema(child_plan_context.attribute_list_);
  aggregation_state.output = GetSchema(plan_context_->attribute_list_);
  aggregation_state.child = child_->GetPhysicalPlan(block_size);

  switch (aggregation_style_) {
    case kAgg: {
      aggregation_state.agg_node_type =
          BlockStreamAggregationIterator::State::Not_Hybrid_Agg;
      ChangeSchemaForAVG(aggregation_state);
      ret = new BlockStreamAggregationIterator(aggregation_state);
      break;
    }
    case kLocalAggReparGlobalAgg: {
      aggregation_state.agg_node_type = BlockStreamAggregationIterator::State::
          Hybrid_Agg_Private;  // as regard to AVG(),for partition node and
                               // global node ,we should do change schema.
      ChangeSchemaForAVG(aggregation_state);
      BlockStreamAggregationIterator* local_aggregation =
          new BlockStreamAggregationIterator(aggregation_state);
      BlockStreamExpander::State expander_state;
      expander_state.block_count_in_buffer_ = EXPANDER_BUFFER_SIZE;
      expander_state.block_size_ = block_size;
      expander_state.init_thread_count_ = Config::initial_degree_of_parallelism;
      expander_state.child_ = local_aggregation;
      expander_state.schema_ = aggregation_state.hashSchema->duplicateSchema();
      BlockStreamIteratorBase* expander_lower =
          new BlockStreamExpander(expander_state);

      ExpandableBlockStreamExchangeEpoll::State exchange_state;
      exchange_state.block_size_ = block_size;
      exchange_state.child_ = expander_lower;
      exchange_state.exchange_id_ =
          IDsGenerator::getInstance()->generateUniqueExchangeID();
      exchange_state.lower_id_list_ = GetInvolvedNodeID(
          child_->GetPlanContext().plan_partitioner_);
      exchange_state.upper_id_list_ =
          GetInvolvedNodeID(plan_context_->plan_partitioner_);
      if (group_by_attribute_list_.empty()) {
        exchange_state.partition_schema_ =
            partition_schema::set_hash_partition(0);
      } else {
        exchange_state.partition_schema_ = partition_schema::set_hash_partition(
            GetInvolvedAttrIdList(GetGroupByAttrsAfterAgg(), *plan_context_)[0]);
      }
      exchange_state.schema_ = aggregation_state.hashSchema->duplicateSchema();
      BlockStreamIteratorBase* exchange =
          new ExpandableBlockStreamExchangeEpoll(exchange_state);

      BlockStreamAggregationIterator::State global_aggregation_state;
      global_aggregation_state.aggregationIndex =
          GetInvolvedAttrIdList(GetAggAttrsAfterAgg(), *plan_context_);
      global_aggregation_state.aggregations =
          ChangeForGlobalAggregation(aggregation_function_list_);
      global_aggregation_state.block_size = block_size;
      global_aggregation_state.bucketsize = 64;
      global_aggregation_state.child = exchange;
      global_aggregation_state.groupByIndex =
          GetInvolvedAttrIdList(GetGroupByAttrsAfterAgg(), *plan_context_);
      global_aggregation_state.input = GetSchema(plan_context_->attribute_list_);
      global_aggregation_state.nbuckets = aggregation_state.nbuckets;
      global_aggregation_state.output = GetSchema(plan_context_->attribute_list_);
      global_aggregation_state.agg_node_type =
          BlockStreamAggregationIterator::State::Hybrid_Agg_Global;
      ChangeSchemaForAVG(global_aggregation_state);
      BlockStreamIteratorBase* global_aggregation =
          new BlockStreamAggregationIterator(global_aggregation_state);
      ret = global_aggregation;
      break;
    }
    case kReparGlobalAgg: {
      // the corresponding physical operation is't implemented
      BlockStreamExpander::State expander_state;
      expander_state.block_count_in_buffer_ = EXPANDER_BUFFER_SIZE;
      expander_state.block_size_ = block_size;
      expander_state.init_thread_count_ = Config::initial_degree_of_parallelism;
      expander_state.child_ = child_->GetPhysicalPlan(block_size);
      expander_state.schema_ = GetSchema(child_plan_context.attribute_list_);
      BlockStreamIteratorBase* expander =
          new BlockStreamExpander(expander_state);
      ExpandableBlockStreamExchangeEpoll::State exchange_state;
      exchange_state.block_size_ = block_size;
      exchange_state.child_ = expander;  // child_->getIteratorTree(block_size);
      exchange_state.exchange_id_ =
          IDsGenerator::getInstance()->generateUniqueExchangeID();
      exchange_state.lower_id_list_ = GetInvolvedNodeID(
          child_->GetPlanContext().plan_partitioner_);
      exchange_state.upper_id_list_ =
          GetInvolvedNodeID(plan_context_->plan_partitioner_);
      if (group_by_attribute_list_.empty()) {
        /**
         * scalar aggregation allows parallel partitions to be partitioned in
         * any fashion.
         * In the current implementation, we use the first aggregation attribute
         * as the
         * partition attribute.
         */

        // TODO(fzh): select the proper partition attribute by considering the
        // cardinality and load balance.
        exchange_state.partition_schema_ =
            partition_schema::set_hash_partition(GetInvolvedAttrIdList(
                aggregation_attribute_list_, child_plan_context)[0]);
      } else {
        exchange_state.partition_schema_ = partition_schema::set_hash_partition(
            GetInvolvedAttrIdList(group_by_attribute_list_, child_plan_context)[0]);
      }
      exchange_state.schema_ = GetSchema(child_plan_context.attribute_list_);
      BlockStreamIteratorBase* exchange =
          new ExpandableBlockStreamExchangeEpoll(exchange_state);
      aggregation_state.agg_node_type =
          BlockStreamAggregationIterator::State::Not_Hybrid_Agg;
      ChangeSchemaForAVG(aggregation_state);
      aggregation_state.child = exchange;
      ret = new BlockStreamAggregationIterator(aggregation_state);
      break;
    }
  }
  return ret;
}
std::vector<unsigned> LogicalAggregation::GetInvolvedAttrIdList(
    const std::vector<Attribute>& attribute_list,
    const PlanContext& plan_context) const {
  std::vector<unsigned> ret;
  for (unsigned i = 0; i < attribute_list.size(); i++) {
    bool found = false;
    for (unsigned j = 0; j < plan_context.attribute_list_.size(); j++) {
      /*
       * @brief: attribute_list[j].isANY()
       */
      if (attribute_list[i].isANY() ||
          (plan_context.attribute_list_[j] == attribute_list[i])) {
        found = true;
        ret.push_back(j);
        break;
      }
    }
    if (found == false) {
      LOG(ERROR) << "can't find attrbute in plan context in "
                    "LogicalAggeration::GetInvolvedAttributeIdList"
                 << std::endl;
    }
  }
  return ret;
}
float LogicalAggregation::EstimateSelectivity() const { return 0.1; }

std::vector<BlockStreamAggregationIterator::State::aggregation>
LogicalAggregation::ChangeForGlobalAggregation(const std::vector<
    BlockStreamAggregationIterator::State::aggregation> list) const {
  std::vector<BlockStreamAggregationIterator::State::aggregation> ret;
  for (unsigned i = 0; i < list.size(); i++) {
    if (list[i] == BlockStreamAggregationIterator::State::count) {
      ret.push_back(BlockStreamAggregationIterator::State::sum);
    } else {
      ret.push_back(list[i]);
    }
  }
  return ret;
}
/**
 * In the current implementation, we assume that aggregation creates a new
 * table, i.e., intermediate table.
 * The id for the intermediate table is -1.
 */
std::vector<Attribute> LogicalAggregation::GetAttrsAfterAgg() const {
  std::vector<Attribute> ret;
  ret = GetGroupByAttrsAfterAgg();
  const std::vector<Attribute> aggregation_attributes = GetAggAttrsAfterAgg();
  ret.insert(ret.end(), aggregation_attributes.begin(),
             aggregation_attributes.end());
  return ret;
}
std::vector<Attribute> LogicalAggregation::GetGroupByAttrsAfterAgg() const {
  std::vector<Attribute> ret;

  for (unsigned i = 0; i < group_by_attribute_list_.size(); i++) {
    Attribute temp = group_by_attribute_list_[i];
    temp.index = i;
    temp.table_id_ = INTERMEIDATE_TABLEID;
    ret.push_back(temp);
  }
  return ret;
}
std::vector<Attribute> LogicalAggregation::GetAggAttrsAfterAgg() const {
  std::vector<Attribute> ret;

  unsigned aggregation_start_index = group_by_attribute_list_.size();
  for (unsigned i = 0; i < aggregation_attribute_list_.size(); i++) {
    Attribute temp = aggregation_attribute_list_[i];

    switch (aggregation_function_list_[i]) {
      case BlockStreamAggregationIterator::State::count: {
        if (!(temp.isNULL() || temp.isANY())) temp.attrType->~ColumnType();
        temp.attrType = new ColumnType(t_u_long, 8);
        temp.attrName = "count(" + temp.getName() + ")";
        temp.index = aggregation_start_index++;
        temp.table_id_ = INTERMEIDATE_TABLEID;
        break;
      }
      case BlockStreamAggregationIterator::State::max: {
        temp.attrName = "max(" + temp.getName() + ")";
        temp.index = aggregation_start_index++;
        temp.table_id_ = INTERMEIDATE_TABLEID;
        break;
      }
      case BlockStreamAggregationIterator::State::min: {
        temp.attrName = "min(" + temp.getName() + ")";
        temp.index = aggregation_start_index++;
        temp.table_id_ = INTERMEIDATE_TABLEID;
        break;
      }
      case BlockStreamAggregationIterator::State::sum: {
        temp.attrName = "sum(" + temp.getName() + ")";
        temp.index = aggregation_start_index++;
        temp.table_id_ = INTERMEIDATE_TABLEID;
        break;
      }
      case BlockStreamAggregationIterator::State::avg: {
        temp.attrName = "avg(" + temp.getName() + ")";
        temp.index = aggregation_start_index++;
        temp.table_id_ = INTERMEIDATE_TABLEID;
      } break;
      default: { assert(false); }
    }
    ret.push_back(temp);
  }
  return ret;
}
unsigned long LogicalAggregation::EstimateGroupByCardinality(
    const PlanContext& plan_context) const {
  if (group_by_attribute_list_.size() == 0) {
    return 1;
  }
  const unsigned long max_limits = 1024 * 1024;
  const unsigned long min_limits = 1024 * 512;
  unsigned long data_card = plan_context.GetAggregatedDataCardinality();
  unsigned long ret;
  for (unsigned i = 0; i < group_by_attribute_list_.size(); i++) {
    if (group_by_attribute_list_[i].isUnique()) {
      ret = ret < max_limits ? ret : max_limits;
      ret = ret > min_limits ? ret : min_limits;
      return ret;
    }
  }
  unsigned long group_by_domain_size = 1;
  for (unsigned i = 0; i < group_by_attribute_list_.size(); i++) {
    AttributeStatistics* attr_stat =
        StatManager::getInstance()->getAttributeStatistic(
            group_by_attribute_list_[i]);
    if (attr_stat == 0) {
      group_by_domain_size *= 1000;
    } else {
      group_by_domain_size *= attr_stat->getDistinctCardinality();
    }
  }
  ret = group_by_domain_size;  // TODO(fzh): This is only the upper bound of
                               // group_by
                               // domain size;

  ret = ret < max_limits ? ret : max_limits;
  ret = ret > min_limits ? ret : min_limits;
  return ret;
}
void LogicalAggregation::Print(int level) const {
  printf("%*.sAggregation:", level * 8, " ");
  switch (aggregation_style_) {
    case kAgg: {
      printf("kAgg\n");
      break;
    }
    case kReparGlobalAgg: {
      printf("kReparGlobalAgg\n");
      break;
    }
    case kLocalAggReparGlobalAgg: {
      printf("kLocalAggReparGlobalAgg!\n");
      break;
    }
    default: { printf("aggregation style is not given!\n"); }
  }
  printf("group-by attributes:\n");
  for (unsigned i = 0; i < this->group_by_attribute_list_.size(); i++) {
    printf("%*.s", level * 8, " ");
    printf("%s\n", group_by_attribute_list_[i].attrName.c_str());
  }
  printf("%*.saggregation attributes:\n", level * 8, " ");
  for (unsigned i = 0; i < aggregation_attribute_list_.size(); i++) {
    printf("%*.s", level * 8, " ");
    switch (aggregation_function_list_[i]) {
      case BlockStreamAggregationIterator::State::count: {
        printf("Count: %s\n", aggregation_attribute_list_[i].attrName.c_str());
        break;
      }
      case BlockStreamAggregationIterator::State::max: {
        printf("Max: %s\n", aggregation_attribute_list_[i].attrName.c_str());
        break;
      }
      case BlockStreamAggregationIterator::State::min: {
        printf("Min: %s\n", aggregation_attribute_list_[i].attrName.c_str());
        break;
      }
      case BlockStreamAggregationIterator::State::sum: {
        printf("Sum: %s\n", aggregation_attribute_list_[i].attrName.c_str());
        break;
      }
      case BlockStreamAggregationIterator::State::avg: {
        printf("Avg: %s\n", aggregation_attribute_list_[i].attrName.c_str());
        break;
      }
      default: { break; }
    }
  }
  child_->Print(level + 1);
}
}  // namespace logical_query_plan
}  // namespace claims
