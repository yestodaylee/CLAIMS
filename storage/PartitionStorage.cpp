/*
 * ProjectionStorage.cpp
 *
 *  Created on: Nov 14, 2013
 *      Author: wangli
 */

#include <iostream>
#include "PartitionStorage.h"
#include "../Debug.h"
#include "MemoryStore.h"
#include "../Config.h"
#include "stdlib.h"
PartitionStorage::PartitionStorage(const PartitionID &partition_id,
                                   const unsigned &number_of_chunks,
                                   const StorageLevel& storage_level)
    : partition_id_(partition_id),
      number_of_chunks_(number_of_chunks),
      desirable_storage_level_(storage_level) {
  for (unsigned i = 0; i < number_of_chunks_; i++) {
    chunk_list_.push_back(
        new ChunkStorage(ChunkID(partition_id_, i), BLOCK_SIZE,
                         desirable_storage_level_));
  }
}

PartitionStorage::~PartitionStorage() {
  for (unsigned i = 0; i < chunk_list_.size(); i++) {
    chunk_list_[i]->~ChunkStorage();
  }
  chunk_list_.clear();
}

void PartitionStorage::addNewChunk() {
  number_of_chunks_++;
}

void PartitionStorage::updateChunksWithInsertOrAppend(
    const PartitionID &partition_id, const unsigned &number_of_chunks,
    const StorageLevel& storage_level) {
  if (!chunk_list_.empty()) {
    /*
     * when appending data, the last chunk may be dirty
     * so set storage level as HDFS
     * to make sure the data will be reload from HDFS
     */
    MemoryChunkStore::getInstance()->returnChunk(
        chunk_list_.back()->getChunkID());
//		if (Config::local_disk_mode == 0)
    // actually, DISK is not used, only HDFS and MEMORY is used
    chunk_list_.back()->setCurrentStorageLevel(HDFS);
//		else
//			chunk_list_.back()->setCurrentStorageLevel(DISK);
  }
  for (unsigned i = number_of_chunks_; i < number_of_chunks; i++)
    chunk_list_.push_back(
        new ChunkStorage(ChunkID(partition_id, i), BLOCK_SIZE, storage_level));
  number_of_chunks_ = number_of_chunks;
}

void PartitionStorage::removeAllChunks(const PartitionID &partition_id) {
  if (!chunk_list_.empty()) {
    vector<ChunkStorage*>::iterator iter = chunk_list_.begin();
    MemoryChunkStore* mcs = MemoryChunkStore::getInstance();
    for (; iter != chunk_list_.end(); iter++) {
      mcs->returnChunk((*iter)->getChunkID());
    }
    chunk_list_.clear();
    number_of_chunks_ = 0;
  }
}

PartitionStorage::PartitionReaderItetaor* PartitionStorage::createReaderIterator() {
  return new PartitionReaderItetaor(this);
}
PartitionStorage::PartitionReaderItetaor* PartitionStorage::createAtomicReaderIterator() {
  return new AtomicPartitionReaderIterator(this);
}

PartitionStorage::PartitionReaderItetaor::PartitionReaderItetaor(
    PartitionStorage* partition_storage)
    : ps(partition_storage),
      chunk_cur_(0),
      chunk_it_(0) {

}

//PartitionStorage::PartitionReaderItetaor::PartitionReaderItetaor():chunk_cur_(0){
//
//}
PartitionStorage::PartitionReaderItetaor::~PartitionReaderItetaor() {

}
ChunkReaderIterator* PartitionStorage::PartitionReaderItetaor::nextChunk() {
  if (chunk_cur_ < ps->number_of_chunks_)
    return ps->chunk_list_[chunk_cur_++]->createChunkReaderIterator();
  else
    return 0;
}
//PartitionStorage::AtomicPartitionReaderIterator::AtomicPartitionReaderIterator():PartitionReaderItetaor(){
//
//}
PartitionStorage::AtomicPartitionReaderIterator::~AtomicPartitionReaderIterator() {

}
ChunkReaderIterator* PartitionStorage::AtomicPartitionReaderIterator::nextChunk() {
//	lock_.acquire();
  ChunkReaderIterator* ret;
  if (chunk_cur_ < ps->number_of_chunks_)
    ret = ps->chunk_list_[chunk_cur_++]->createChunkReaderIterator();
  else
    ret = 0;
//	lock_.release();
  return ret;
}

bool PartitionStorage::PartitionReaderItetaor::nextBlock(
    BlockStreamBase*& block) {
  assert(false);
  if (chunk_it_ > 0 && chunk_it_->nextBlock(block)) {
    return true;
  }
  else {
    if ((chunk_it_ = nextChunk()) > 0) {
      return nextBlock(block);
    }
    else {
      return false;
    }
  }
}

bool PartitionStorage::AtomicPartitionReaderIterator::nextBlock(
    BlockStreamBase*& block) {
////	lock_.acquire();
//	if(chunk_it_>0&&chunk_it_->nextBlock(block)){
////		lock_.release();
//		return true;
//	}
//	else{
//		lock_.acquire();
//		if((chunk_it_=nextChunk())>0){
//			lock_.release();
//			return nextBlock(block);
//		}
//		else{
//			lock_.release();
//			return false;
//		}
//	}
  //	lock_.acquire();

  lock_.acquire();
  ChunkReaderIterator::block_accessor* ba;
  if (chunk_it_ != 0 && chunk_it_->getNextBlockAccessor(ba)) {
    lock_.release();
    ba->getBlock(block);
    return true;
  }
  else {
    if ((chunk_it_ = PartitionReaderItetaor::nextChunk()) > 0) {
      lock_.release();
      return nextBlock(block);
    }
    else {
      lock_.release();
      return false;
    }
  }
}
PartitionStorage::TxnPartitionReaderIterator::~TxnPartitionReaderIterator() {
    for (auto ptr : block_buffer_) {
      free(ptr->getBlockDataAddress());
      delete ptr;
    }

}

PartitionStorage::PartitionReaderItetaor* PartitionStorage::createTxnReaderIterator(
    unsigned block_size,
    UInt64 checkpoint,
    vector<PStrip> & strip_list) {
//  cout << "striplist" << endl;
//  for (auto & strip : strip_list)
//    cout << strip.first << "," << strip.second << endl;
//  vector<PStrip> v;
  return new TxnPartitionReaderIterator(this, block_size, checkpoint, strip_list);
}

bool PartitionStorage::TxnPartitionReaderIterator::nextBlock(
    BlockStreamBase*& block) {
  ChunkReaderIterator::block_accessor* ba;
  unsigned tuple_counts = 0;
  BlockStreamFix * block_tmp = nullptr;
  lock_.acquire();
  if (chunk_it_ != 0 && chunk_it_->getNextBlockAccessor(ba)) {
    ba->getBlock(block);
    auto block_begin = getBlockBegin(chunk_it_, block);
    auto block_end = block_begin + block_size_;
//    cout << "chunk_id:"<< "," <<  block_begin / (64 * 1024 * 1024) <<
//        ",block_id:" << block_begin / (64 * 1024) << endl;
    lock_.release();
    if (block_begin >= checkpoint_) { /*checkpoint后的数据*/
       if (blockid_strips_.find(block_begin/(block_size_)) != blockid_strips_.end()) {
         block_tmp = block;
         block = NewTmpFixBlock();
         for (auto & strip : blockid_strips_[block_begin/ (block_size_)]) {
           auto strip_begin = strip.first;
           auto strip_end = strip.first + strip.second;
           memcpy(block->getBlock() + tuple_counts * tuple_size_,
                  block_tmp->getBlock() + strip_end - strip_begin,
                  strip_end - strip_begin);
           if (strip_end == block_end)
             strip_end -= sizeof(unsigned);
           auto tuple_count = (strip_end - strip_begin) / tuple_size_;
           tuple_counts += tuple_count;
            }
         *(unsigned *)(block->getBlock() + block_size_ - sizeof(unsigned)) =
             tuple_counts;
         return true;
         }  /* 该block没有被任何strip投影到 */
         else return nextBlock(block);
     } else { /*checkpoint前的数据*/
       return true;
     }

  }
  else {
    if ((chunk_it_ = PartitionReaderItetaor::nextChunk()) > 0) {
      lock_.release();
      return nextBlock(block);
    }
    else {
      lock_.release();
      return false;
    }
  }
}
ChunkReaderIterator* PartitionStorage::TxnPartitionReaderIterator::nextChunk() {
//  lock_.acquire();
  ChunkReaderIterator* ret;
  if (chunk_cur_ < ps->number_of_chunks_)
    ret = ps->chunk_list_[chunk_cur_++]->createChunkReaderIterator();
  else
    ret = 0;
//  lock_.release();
  return ret;
}

BlockStreamFix * PartitionStorage::TxnPartitionReaderIterator::NewTmpFixBlock() {
  void *  data = (void *) malloc( block_size_);
  auto block = new BlockStreamFix(block_size_, 0, data, 0);
  block_buffer_.push_back(block);
  return block;
}
UInt64 PartitionStorage::TxnPartitionReaderIterator::getBlockBegin
            (ChunkReaderIterator* chunk, BlockStreamBase* block) {
  auto block_addr = (char*)block->getBlock();
  auto chunk_addr = (char*)((InMemoryChunkReaderItetaor*)chunk)->getChunk();
  auto chunk_id = ((InMemoryChunkReaderItetaor*)chunk)->chunk_id_.chunk_off;
  return chunk_id * block_size_ * 1024 + block_addr - chunk_addr;
}
