/*
 * ProjectionStorage.h
 *
 *  Created on: Nov 14, 2013
 *      Author: wangli
 */

#ifndef PARTITIONSTORAGE_H_
#define PARTITIONSTORAGE_H_
#include <vector>

#include "../common/error_define.h"

#ifdef DMALLOC
#include "dmalloc.h"
#endif
#include "ChunkStorage.h"
#include "StorageLevel.h"
#include "PartitionReaderIterator.h"
#include "../utility/lock.h"

class PartitionStorage {
 public:
  class PartitionReaderItetaor {
   public:
    //		PartitionReaderItetaor();
    PartitionReaderItetaor(PartitionStorage* partition_storage);
    virtual ~PartitionReaderItetaor();
    virtual ChunkReaderIterator* nextChunk();
    virtual bool nextBlock(BlockStreamBase*& block);

   protected:
    PartitionStorage* ps;
    unsigned chunk_cur_;
    ChunkReaderIterator* chunk_it_;
  };
  class AtomicPartitionReaderIterator : public PartitionReaderItetaor {
   public:
    //		AtomicPartitionReaderIterator();
    AtomicPartitionReaderIterator(PartitionStorage* partition_storage)
        : PartitionReaderItetaor(partition_storage){};
    virtual ~AtomicPartitionReaderIterator();
    ChunkReaderIterator* nextChunk();
    virtual bool nextBlock(BlockStreamBase*& block);

   private:
    Lock lock_;
  };
  class TxnPartitionReaderIterator : public PartitionReaderItetaor {
   public:
    TxnPartitionReaderIterator(PartitionStorage* partition_storage)
        : PartitionReaderItetaor(partition_storage){};
    virtual ~TxnPartitionReaderIterator();
    ChunkReaderIterator* nextChunk();
    virtual bool nextBlock(BlockStreamBase*& block);

   private:
    Lock lock_;
  };

 public:
  friend class PartitionReaderItetaor;
  PartitionStorage(const PartitionID& partition_id,
                   const unsigned& number_of_chunks, const StorageLevel&);
  virtual ~PartitionStorage();
  void addNewChunk();
  void updateChunksWithInsertOrAppend(const PartitionID& partition_id,
                                      const unsigned& number_of_chunks,
                                      const StorageLevel& storage_level);

  RetCode AddChunkWithMemoryToNum(const unsigned& expected_number_of_chunks,
                                  const StorageLevel& storage_level);
  void removeAllChunks(const PartitionID& partition_id);
  PartitionReaderItetaor* createReaderIterator();
  PartitionReaderItetaor* createAtomicReaderIterator();
  PartitionReaderItetaor* createTxnReaderIterator();
  const int GetChunkNum() const { return chunk_list_.size(); }

 protected:
  PartitionID partition_id_;
  unsigned number_of_chunks_;
  std::vector<ChunkStorage*> chunk_list_;
  StorageLevel desirable_storage_level_;
};

#endif /* PARTITIONSTORAGE_H_ */
