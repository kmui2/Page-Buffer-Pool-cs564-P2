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
#include "exceptions/invalid_page_exception.h"
#include "file_iterator.h"


namespace badgerdb
{

BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;

	
}

BufMgr::~BufMgr()
{
	for (FrameId i = 0; i < numBufs; i++)
	{
		if (bufDescTable[i].dirty && bufDescTable[i].valid) {
			flushFile(bufDescTable[i].file);
		}
	}
	delete [] bufDescTable;
	delete [] bufPool;
	delete hashTable;
}

void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
	
}

void BufMgr::allocBuf(FrameId &frame)
{
	bool allPinned = true;
	uint32_t i;
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].pinCnt == 0) {
			allPinned = false;
		}
	} 
	if (allPinned)
		throw BufferExceededException();


	uint32_t ticks = 0;
	bool found = false;
	while (ticks < numBufs*2 && !found) {
		advanceClock();

		if (!bufDescTable[clockHand].valid)
			found = true;

		else if (bufDescTable[clockHand].refbit) {
			bufDescTable[clockHand].refbit = false; 

			if (bufDescTable[clockHand].pinCnt == 0)
				found = true;
		}
		ticks++;
	}
	
	if (bufDescTable[clockHand].dirty)
		bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
	
	
	if (bufDescTable[clockHand].valid)
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
	bufDescTable[clockHand].Clear();
	frame = clockHand;
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	badgerdb::Page new_page;
	const badgerdb::RecordId& rid = new_page.insertRecord("hello, world!");
	new_page.getRecord(rid); // returns "hello, world!"

	FrameId frameNo = 0;
	try { 
		hashTable->lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];

	}
	catch (HashNotFoundException e){
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo];
	}

}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	try { 
		FrameId frameNo = 0;
		hashTable->lookup(file, pageNo, frameNo);
		if (dirty)
			bufDescTable[frameNo].dirty = true;
		if (bufDescTable[frameNo].pinCnt == 0)
			throw PageNotPinnedException(file->filename(), pageNo, frameNo);

		bufDescTable[frameNo].pinCnt--;
	}
	catch (InvalidPageException e){
		// std::cout << "An attempt was made to access an unpin an invalid page in a file." << std::endl;
	}

}

void BufMgr::flushFile(const File *file)
{
	uint32_t i;
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file ) {
			if (bufDescTable[i].pinCnt > 0) {
				throw PagePinnedException(file->filename(),bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			else if (bufDescTable[i].valid == 0) {
				throw BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
			}
		}
	}
	
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file) {
			File *file = bufDescTable[i].file;
			if (bufDescTable[i].dirty == true) {
				file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	Page newPage = file->allocatePage();
	FrameId frameNo;
	allocBuf(frameNo);
	pageNo = newPage.page_number();
	hashTable->insert(file, pageNo, frameNo);
	bufDescTable[frameNo].Set(file, pageNo);
	bufPool[frameNo] = newPage;
	page = &bufPool[frameNo];
	
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
	FrameId  frameNo = 0;
	try {
		hashTable->lookup(file, PageNo, frameNo);
		bufDescTable[frameNo].Clear();
		hashTable->remove(file, PageNo);
	}	
	catch (HashNotFoundException) {
		// std::cout << "HashNotFoundException BufMgr::disposePage(File *file, const PageId PageNo)" << std::endl;
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

}
}
