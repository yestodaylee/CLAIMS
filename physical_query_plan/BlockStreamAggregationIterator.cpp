/*
 * BlockStreamAggregationIterator.cpp
 *
 * Created on: 2013-9-9
 * Author: casa
 */

#include "../physical_query_plan/BlockStreamAggregationIterator.h"

#include "../../Debug.h"
#include "../../utility/rdtsc.h"
#include "../../Executor/ExpanderTracker.h"
BlockStreamAggregationIterator::BlockStreamAggregationIterator(State state)
:state_(state),hashtable_(0),hash_(0),bucket_cur_(0),PhysicalOperator(4,3){
	InitExpandedStatus();
	assert(state_.hashSchema);
	InitAvgDivide();
}

BlockStreamAggregationIterator::BlockStreamAggregationIterator()
:hashtable_(0),hash_(0),bucket_cur_(0),PhysicalOperator(4,3){
	InitExpandedStatus();
	InitAvgDivide();
}

BlockStreamAggregationIterator::~BlockStreamAggregationIterator() {
	delete state_.input;
	delete state_.hashSchema;
	delete state_.output;
	delete state_.child;
}

BlockStreamAggregationIterator::State::State(
		Schema *input,
		Schema *output,
		Schema *hashSchema,
		BlockStreamIteratorBase *child,
		std::vector<unsigned> groupByIndex,
		std::vector<unsigned> aggregationIndex,
		std::vector<State::aggregation> aggregations,
		unsigned nbuckets,
		unsigned bucketsize,
		unsigned block_size,
		std::vector<unsigned>avgIndex,
		AggNodeType agg_node_type
):input(input),
output(output),
hashSchema(hashSchema),
child(child),
groupByIndex(groupByIndex),
aggregationIndex(aggregationIndex),
aggregations(aggregations),
nbuckets(nbuckets),
bucketsize(bucketsize),
block_size(block_size),
avgIndex(avgIndex),
agg_node_type(agg_node_type){

}

bool BlockStreamAggregationIterator::Open(const PartitionOffset& partition_offset){
	RegisterExpandedThreadToAllBarriers();

	if(TryEntryIntoSerializedSection(0)){
		ExpanderTracker::getInstance()->addNewStageEndpoint(pthread_self(),LocalStageEndPoint(stage_desc,"Aggregation",0));
	}
	BarrierArrive(0);

	state_.child->Open(partition_offset);
	if(ExpanderTracker::getInstance()->isExpandedThreadCallBack(pthread_self())){
		UnregisterExpandedThreadToAllBarriers();
		return true;
	}


	ticks start=curtick();
	if(TryEntryIntoSerializedSection(1)){
		prepareIndex();
		prepareAggregateFunctions();
		hash_=PartitionFunctionFactory::createGeneralModuloFunction(state_.nbuckets);
		hashtable_=new BasicHashTable(state_.nbuckets,state_.bucketsize,state_.hashSchema->getTupleMaxSize());//

	}

	start=curtick();
	/* A private hash table is allocated for each thread to buffer the local results. All the private hash table should be merged
	 * at the final phase to complete the aggregation. Aggregation using private hash tables is called private aggregation. Although
	 * consuming larger memory, private aggregation is more efficient than shared aggregation for scalar aggregation or aggregation
	 * with small groups, as private aggregation avoids the contention to the shared hash table.
	 */
	BasicHashTable* private_hashtable=new BasicHashTable(state_.nbuckets,state_.bucketsize,state_.hashSchema->getTupleMaxSize());

	start=curtick();
	BarrierArrive(1);
	void *cur=0;
	unsigned bn;
	bool key_exist;
	void* tuple_in_hashtable;
	void *key_in_input_tuple;
	void *key_in_hash_table;
	void *value_in_input_tuple;
	void *value_in_hash_table;
	void* new_tuple_in_hash_table;
	unsigned allocated_tuples_in_hashtable=0;
	BasicHashTable::Iterator ht_it=hashtable_->CreateIterator();
	BasicHashTable::Iterator pht_it=private_hashtable->CreateIterator();
	unsigned long long one=1;
	BlockStreamBase *bsb=BlockStreamBase::createBlock(state_.input,state_.block_size);
	bsb->setEmpty();

	start=curtick();

	while(state_.child->Next(bsb))	// traverse every block from child
	{
		BlockStreamBase::BlockStreamTraverseIterator *bsti=bsb->createIterator();
		bsti->reset();
		while((cur=bsti->currentTuple())!=0)	// traverse every tuple from block
		{
			/* get the corresponding bucket index according to the first column in
			 * group-by attributes.
			 * Note that bn is always 0 for scalar aggregation.
			 */
			bn=0;
			if(state_.groupByIndex.size()>0)
				bn=state_.input->getcolumn(state_.groupByIndex[0]).operate->GetPartitionValue(state_.input->getColumnAddess(state_.groupByIndex[0],cur),state_.nbuckets, nullptr);

			private_hashtable->placeIterator(pht_it,bn);
			key_exist=false;
			while((tuple_in_hashtable=pht_it.readCurrent())!=0)
			{
				/* set key_exist flag to true such that the the case for scalar
				 * aggregation (i.e., aggregation with no group-by attributes)
				 * could be considered as passed the group by value verification.
				 */
				key_exist=true;
				for(unsigned i=0;i<state_.groupByIndex.size();i++)
				{
					key_in_input_tuple=state_.input->getColumnAddess(state_.groupByIndex[i],cur);
					key_in_hash_table=state_.hashSchema->getColumnAddess(inputGroupByToOutput_[i],tuple_in_hashtable);
					if(!state_.input->getcolumn(state_.groupByIndex[i]).operate->Equal(key_in_input_tuple,key_in_hash_table))
					{
						key_exist=false;
						break;
					}
				}
				if(key_exist)	// hash table have the key (the value in group-by attribute)
				{
					for(unsigned i=0;i<state_.aggregationIndex.size();i++)
					{
						value_in_input_tuple=state_.input->getColumnAddess(state_.aggregationIndex[i],cur);
						value_in_hash_table=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],tuple_in_hashtable);
						private_hashtable->UpdateTuple(bn,value_in_hash_table,value_in_input_tuple,privateAggregationFunctions_[i]);
					}
					bsti->increase_cur_();
					break;
				}
				else
				{
					pht_it.increase_cur_();
				}
			}
			if(key_exist)
			{
				continue;
			}
			new_tuple_in_hash_table=private_hashtable->allocate(bn);
			for(unsigned i=0;i<state_.groupByIndex.size();i++)
			{
				key_in_input_tuple=state_.input->getColumnAddess(state_.groupByIndex[i],cur);
				key_in_hash_table=state_.hashSchema->getColumnAddess(inputGroupByToOutput_[i],new_tuple_in_hash_table);
				state_.input->getcolumn(state_.groupByIndex[i]).operate->Assign(key_in_input_tuple,key_in_hash_table);
			}

			for(unsigned i=0;i<state_.aggregationIndex.size();i++)
			{
				/**
				 * use if-else here is a kind of ugly.
				 * TODO: use a function which is initialized according to the aggregation function.
				 */
				if(state_.aggregations[i]==State::count)
				{
					value_in_input_tuple=&one;
				}
				else
				{
					value_in_input_tuple=state_.input->getColumnAddess(state_.aggregationIndex[i],cur);
				}
				value_in_hash_table=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],new_tuple_in_hash_table);
				state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->Assign(value_in_input_tuple,value_in_hash_table);
			}
			bsti->increase_cur_();
		}
		bsb->setEmpty();
	}

	for(int i=0;i<state_.nbuckets;i++)
	{
		private_hashtable->placeIterator(pht_it,i);
		while((cur=pht_it.readCurrent())!=0)	// traverse every tuple from block
		{
			/* get the corresponding bucket index according to the first column in
			 * group-by attributes.
			 * Note that bn is always 0 for scalar aggregation.
			 */
			bn=0;
			if(state_.groupByIndex.size()>0)
				bn=state_.hashSchema->getcolumn(0).operate->GetPartitionValue(state_.hashSchema->getColumnAddess(0,cur),state_.nbuckets, nullptr);

			hashtable_->lockBlock(bn);
			hashtable_->placeIterator(ht_it,bn);
			key_exist=false;
			while((tuple_in_hashtable=ht_it.readCurrent())!=0)
			{
				/* set key_exist flag to true such that the the case for scalar
				 * aggregation (i.e., aggregation with no group-by attributes)
				 * could be considered as passed the group by value verification.
				 */
				key_exist=true;
				for(unsigned i=0;i<state_.groupByIndex.size();i++)
				{
					key_in_input_tuple=state_.hashSchema->getColumnAddess(i,cur);
					key_in_hash_table=state_.hashSchema->getColumnAddess(inputGroupByToOutput_[i],tuple_in_hashtable);
					if(!state_.hashSchema->getcolumn(i).operate->Equal(key_in_input_tuple,key_in_hash_table))
					{
						key_exist=false;
						break;
					}
				}
				if(key_exist)	// hash table have the key (the value in group-by attribute)
				{
					for(unsigned i=0;i<state_.aggregationIndex.size();i++)
					{
						value_in_input_tuple=state_.hashSchema->getColumnAddess(i+state_.groupByIndex.size(),cur);
						value_in_hash_table=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],tuple_in_hashtable);
						hashtable_->UpdateTuple(bn,value_in_hash_table,value_in_input_tuple,globalAggregationFunctions_[i]);
					}
					pht_it.increase_cur_();
					hashtable_->unlockBlock(bn);
					break;
				}
				else
				{
					ht_it.increase_cur_();
				}
			}
			if(key_exist)
			{
				continue;
			}
			new_tuple_in_hash_table=hashtable_->allocate(bn);
			allocated_tuples_in_hashtable++;
			for(unsigned i=0;i<state_.groupByIndex.size();i++)
			{
				key_in_input_tuple=state_.hashSchema->getColumnAddess(i,cur);
				key_in_hash_table=state_.hashSchema->getColumnAddess(inputGroupByToOutput_[i],new_tuple_in_hash_table);
				state_.hashSchema->getcolumn(i).operate->Assign(key_in_input_tuple,key_in_hash_table);
			}

			for(unsigned i=0;i<state_.aggregationIndex.size();i++)
			{
				value_in_input_tuple=state_.hashSchema->getColumnAddess(i+state_.groupByIndex.size(),cur);
				value_in_hash_table=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],new_tuple_in_hash_table);
				state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->Assign(value_in_input_tuple,value_in_hash_table);
			}
			hashtable_->unlockBlock(bn);
			pht_it.increase_cur_();
		}
	}

	if(ExpanderTracker::getInstance()->isExpandedThreadCallBack(pthread_self())){
		UnregisterExpandedThreadToAllBarriers(1);
		return true;
	}
	BarrierArrive(2);

	if(TryEntryIntoSerializedSection(2)){
//		hashtable_->report_status();
		it_=hashtable_->CreateIterator();
		bucket_cur_=0;
		hashtable_->placeIterator(it_,bucket_cur_);
		SetReturnStatus(true);
		ExpanderTracker::getInstance()->addNewStageEndpoint(pthread_self(),LocalStageEndPoint(stage_src,"Aggregation  ",0));
		perf_info_=ExpanderTracker::getInstance()->getPerformanceInfo(pthread_self());
		perf_info_->initialize();
	}
	BarrierArrive(3);

	delete bsb;
	delete private_hashtable;
	return GetReturnStatus();
}

/*
 * In the current implementation, the lock is used based on the entire
 * hash table, which will definitely reduce the degree of parallelism.
 * But it is for now, assuming that the aggregated results are small.
 *
 */
bool BlockStreamAggregationIterator::Next(BlockStreamBase *block){
	if(ExpanderTracker::getInstance()->isExpandedThreadCallBack(pthread_self())){
		UnregisterExpandedThreadToAllBarriers(3);
		printf("<<<<<<<<<<<<<<<<<Aggregation next detected call back signal!>>>>>>>>>>>>>>>>>\n");
		return false;
	}
	void *cur_in_ht;
	void *tuple;
	void *key_in_hash_tuple;
	void *key_in_output_tuple;
	ht_cur_lock_.acquire();
	while(it_.readCurrent()!=0||(hashtable_->placeIterator(it_,bucket_cur_))!=false){
		while((cur_in_ht=it_.readCurrent())!=0)
		{
			if((tuple=block->allocateTuple(state_.output->getTupleMaxSize()))!=0)//the tuple is empty??
			{
				if(state_.avgIndex.size()>0&&(state_.agg_node_type==State::Hybrid_Agg_Global||state_.agg_node_type==State::Not_Hybrid_Agg))//avg=sum/tuple_size
				{
					for(unsigned i=0;i<state_.groupByIndex.size();i++)//in one tuple that are produced from aggregation statement, the groupby attributes is at the head, the rest attributes belong to the aggregation part.
					{
						key_in_hash_tuple=state_.hashSchema->getColumnAddess(inputGroupByToOutput_[i],cur_in_ht);
						key_in_output_tuple=state_.output->getColumnAddess(inputGroupByToOutput_[i],tuple);
						state_.output->getcolumn(inputGroupByToOutput_[i]).operate->Assign(key_in_hash_tuple,key_in_output_tuple);
					}
					state_.avgIndex.push_back(-1);//boundary point,
					int aggsize=state_.aggregationIndex.size()-1;
					unsigned i=0,j=0;
					unsigned long  count_value=(*(unsigned long *)state_.hashSchema->getColumnAddess(inputAggregationToOutput_[aggsize],cur_in_ht));
					for(;i<aggsize;i++)
					{
						if(state_.avgIndex[j]==i)	//avgIndex save the index of avg in aggregations,see logical_aggregation.cpp:116
						{
							assert(state_.aggregations[i]==State::sum);
							j++;
							void *sum_value=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],cur_in_ht); //get the value in hash table

							if(count_value==0)//how to report the error if divided by 0?
							{
								key_in_hash_tuple=sum_value;
							}
							else
							{// TODO: precision of avg result is not enough

								key_in_hash_tuple=sum_value;//the room is enough?
								ExectorFunction::avg_divide[state_.hashSchema->columns[inputAggregationToOutput_[i]].type](sum_value,count_value,key_in_hash_tuple);
							}
						}
						else
						{
							key_in_hash_tuple=state_.hashSchema->getColumnAddess(inputAggregationToOutput_[i],cur_in_ht);
						}
						key_in_output_tuple=state_.output->getColumnAddess(inputAggregationToOutput_[i],tuple);
						state_.output->getcolumn(inputAggregationToOutput_[i]).operate->Assign(key_in_hash_tuple,key_in_output_tuple);
					}
				}
				else{
					memcpy(tuple,cur_in_ht,state_.output->getTupleMaxSize());
				}
				it_.increase_cur_();
			}
			else{
				ht_cur_lock_.release();
				perf_info_->processed_one_block();
				return true;
			}
		}
		bucket_cur_++;
	}
	ht_cur_lock_.release();
	if(block->Empty()){
		return false;
	}
	else{
		perf_info_->processed_one_block();
		return true;
	}
}

bool BlockStreamAggregationIterator::Close(){

	InitExpandedStatus();

	delete hashtable_;
	globalAggregationFunctions_.clear();
	inputAggregationToOutput_.clear();
	inputGroupByToOutput_.clear();

	state_.child->Close();
	return true;
}
void BlockStreamAggregationIterator::Print(){
	printf("Aggregation:  %d buckets in hash table\n",state_.nbuckets);
	printf("---------------\n");
	state_.child->Print();
}

void BlockStreamAggregationIterator::prepareIndex() {
	unsigned outputindex=0;
	for(unsigned i=0;i<state_.groupByIndex.size();i++)
	{
		inputGroupByToOutput_[i]=outputindex++;	// index of group by attributes from input To output index
	}
	for(unsigned i=0;i<state_.aggregationIndex.size();i++)
	{
		inputAggregationToOutput_[i]=outputindex++;	// index of aggregation attributes from input To output index
	}
}

void BlockStreamAggregationIterator::prepareAggregateFunctions() {
	for(unsigned i=0;i<state_.aggregations.size();i++)
	{
		switch(state_.aggregations[i]){
		case BlockStreamAggregationIterator::State::count:
			privateAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetIncreateByOneFunction());
			globalAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetADDFunction());
			break;
		case BlockStreamAggregationIterator::State::min:
			privateAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetMINFunction());
			globalAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetMINFunction());
			break;
		case BlockStreamAggregationIterator::State::max:
			privateAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetMAXFunction());
			globalAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetMAXFunction());
			break;
		case BlockStreamAggregationIterator::State::sum:
			privateAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetADDFunction());
			globalAggregationFunctions_.push_back(state_.hashSchema->getcolumn(inputAggregationToOutput_[i]).operate->GetADDFunction());
			break;
		default://for avg has changed to sum and count
			printf("invalid aggregation function!\n");
		}
	}
}
