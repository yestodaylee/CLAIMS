/*
 * ProjectionStorage.h
 *
 *  Created on: Nov 14, 2013
 *      Author: wangli
 */

#ifndef PARTITIONSTORAGE_H_
#define PARTITIONSTORAGE_H_
#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include <vector>
#include <map>
#include "ChunkStorage.h"
#include "StorageLevel.h"
#include "PartitionReaderIterator.h"
#include "../utility/lock.h"
#include "../txn_manager/txn.hpp"
using std::vector;
using std::map;
using claims::txn::UInt64;
using claims::txn::PStrip;

class PartitionStorage {
public:
	class PartitionReaderItetaor{
	public:
//		PartitionReaderItetaor();
		PartitionReaderItetaor(PartitionStorage* partition_storage);
		virtual ~PartitionReaderItetaor();
		virtual ChunkReaderIterator* nextChunk();
		virtual bool nextBlock(BlockStreamBase* &block);
	protected:
		PartitionStorage* ps;
		unsigned chunk_cur_;
		ChunkReaderIterator* chunk_it_;
	};
	class AtomicPartitionReaderIterator:public PartitionReaderItetaor{
	public:
//		AtomicPartitionReaderIterator();
		AtomicPartitionReaderIterator(PartitionStorage* partition_storage):PartitionReaderItetaor(partition_storage){};
		virtual ~AtomicPartitionReaderIterator();
		ChunkReaderIterator* nextChunk();
		virtual bool nextBlock(BlockStreamBase* &block);
	private:
		Lock lock_;
	};
  class TxnPartitionReaderIterator:public PartitionReaderItetaor{
  public:
    TxnPartitionReaderIterator(PartitionStorage* partition_storage,
                               UInt64 checkpoint, vector<PStrip> & strip_list):
      PartitionReaderItetaor(partition_storage), checkpoint_(checkpoint) {
         for (auto & strip : strip_list) {
            auto begin = strip.first;
            auto end = strip.first + strip.second;
            while(begin < end) {
                auto block_id = begin / (64 * 1024);
                auto step = end <= block_id*1024*64 ? end - begin : 64 * 1024;
                blockid_strips_[block_id].push_back(PStrip(begin, begin + step));
                begin += step;
                }
           }
    };
    virtual ~TxnPartitionReaderIterator();
    ChunkReaderIterator* nextChunk();
    virtual bool nextBlock(BlockStreamBase* &block);
  private:
    Lock lock_;
    UInt64 checkpoint_;
    map<unsigned, vector<PStrip>> blockid_strips_;
    unsigned tuple_size_ = 10;


    vector<BlockStreamFix *> block_buffer_;
    BlockStreamFix * NewTmpFixBlock();
    UInt64 getBlockBegin(ChunkReaderIterator* chunk, BlockStreamBase* block);

  };
	friend class PartitionReaderItetaor;
	PartitionStorage(const PartitionID &partition_id,const unsigned &number_of_chunks,const StorageLevel&);
	virtual ~PartitionStorage();
	void addNewChunk();
	void updateChunksWithInsertOrAppend(const PartitionID &partition_id, const unsigned &number_of_chunks, const StorageLevel& storage_level);
	void removeAllChunks(const PartitionID &partition_id);
	PartitionReaderItetaor* createReaderIterator();
	PartitionReaderItetaor* createAtomicReaderIterator();
	PartitionReaderItetaor* createTxnReaderIterator();
protected:
	PartitionID partition_id_;
	unsigned number_of_chunks_;
	std::vector<ChunkStorage*> chunk_list_;
	StorageLevel desirable_storage_level_;
};

#endif /* PARTITIONSTORAGE_H_ */
