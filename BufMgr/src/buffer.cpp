/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb { 

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs) {
	bufDescTable = new BufDesc[bufs];

  for (FrameId i = 0; i < bufs; i++) 
  {
  	bufDescTable[i].frameNo = i;
  	bufDescTable[i].valid = false;
  }

  bufPool = new Page[bufs];

	int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
  hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table
  // hashTable 是一个指针类型 对成员变量的引用使用‘->’而非‘.’
  clockHand = bufs - 1;
}


BufMgr::~BufMgr() {
	for(FrameId i = 0 ; i < numBufs ; i ++ )  //删除管理器前写回所有有效的脏页
	{
		if(bufDescTable[i].valid == true && bufDescTable[i].dirty == true)
		{
			flushFile(bufDescTable[i].file);
		}
	}

	delete[] bufDescTable;
	delete[] bufPool;
	delete hashTable;  // hashTable已经实现了～方法
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs; // 加一后对frame数量做取模运算
}

void BufMgr::allocBuf(FrameId & frame) 
{
	unsigned int pin_statistic = 0;
	advanceClock();
	while(true)
	{
		if(!bufDescTable[clockHand].valid){
			frame = clockHand;
			return;
		}

		if(bufDescTable[clockHand].refbit){
			bufDescTable[clockHand].refbit = false;
		}
		else{
			if(bufDescTable[clockHand].pinCnt == 0){
				if(bufDescTable[clockHand].dirty){
					bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
					bufDescTable[clockHand].dirty = false;
				}
				try{
					hashTable->remove(bufDescTable[clockHand].file,bufDescTable[clockHand].pageNo);
				}catch(HashNotFoundException e){
				}
				frame = clockHand;
				//这里不置valid为1 因为读写需要时间 防止与其他alloc申请的冲突
				return;
			}
			else{  // 该frame被pin 需要统计
				pin_statistic ++;
				if(pin_statistic == numBufs){
					throw BufferExceededException();
				}
			}
		}
		advanceClock();
	}
}

	
void BufMgr::readPage(File* file, const PageId pageNo, Page*& page)
{
	FrameId frameNo;
	try{
		hashTable->lookup(file,pageNo,frameNo);
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
	}catch(HashNotFoundException e){
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file,pageNo,frameNo);
		bufDescTable[frameNo].Set(file,pageNo);
	}
	page = (bufPool + frameNo);
	return;
}


void BufMgr::unPinPage(File* file, const PageId pageNo, const bool dirty) 
{
	FrameId frameNo;
	try{
		hashTable->lookup(file,pageNo,frameNo);
	}catch(HashNotFoundException e){
		//do nothing
		return;
	}
	if(bufDescTable[frameNo].pinCnt > 0){
		bufDescTable[frameNo].pinCnt--;
		if(dirty){
			bufDescTable[frameNo].dirty = true;
		}
	}else{
		throw PageNotPinnedException(bufDescTable[frameNo].file->filename(), bufDescTable[frameNo].pageNo, frameNo);
	}

}

void BufMgr::flushFile(const File* file) 
{
	for(FrameId i = 0; i < numBufs; i ++){
		if(bufDescTable[i].file == file){
			if(bufDescTable[i].pinCnt > 0)
				throw PagePinnedException(file->filename(),bufDescTable[i].pageNo,i);
			if(!bufDescTable[i].valid)
				throw BadBufferException(i,bufDescTable[i].dirty,bufDescTable[i].valid,bufDescTable[i].refbit);
			if(bufDescTable[i].dirty){
				bufDescTable[i].file -> writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(file,bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File* file, PageId &pageNo, Page*& page) 
{
	Page newPage = file->allocatePage();
	pageNo = newPage.page_number();  //page number
	FrameId frameNo;  // frame number
	allocBuf(frameNo); // 分配frame
	hashTable->insert(file,pageNo,frameNo);
	bufPool[frameNo] = newPage;  // 在管理器中存入该页
	bufDescTable[frameNo].Set(file,pageNo);
	page = (bufPool + frameNo);  // 将保存该页的frame地址存放到变量page中
}

void BufMgr::disposePage(File* file, const PageId PageNo)
{
	FrameId frameNo;
	try{
		hashTable->lookup(file, PageNo, frameNo);  // 在hash表中查找对应page
		hashTable->remove(file, PageNo);  // 如果找到就删除
		bufDescTable[frameNo].Clear();  // bufDescTable也要初始化相应的frame
		//todo bufPool需要处理吗？
	}catch(HashNotFoundException e){
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void) 
{
  BufDesc* tmpbuf;
	int validFrames = 0;
  
  for (std::uint32_t i = 0; i < numBufs; i++)
	{
  	tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

  	if (tmpbuf->valid == true)
    	validFrames++;
  }

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

}
