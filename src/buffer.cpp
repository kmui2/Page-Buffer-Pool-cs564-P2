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
		if (bufDescTable[i].dirty) {
			BufMgr::flushFile(bufDescTable[i].file);
		}
	}
	delete bufDescTable;
	delete bufPool;
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
	throw new BufferExceededException();
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	badgerdb::Page new_page;
	const badgerdb::RecordId& rid = new_page.insertRecord("hello, world!");
	new_page.getRecord(rid); // returns "hello, world!"

	FrameId frameNo = 0;
	try { 
		hashTable->lookup(file, pageNo, frameNo);
		bufDescTable[frameNo].refbit = false;
		bufDescTable[frameNo].pinCnt++;
		*page = bufPool[frameNo];

	}
	catch (HashNotFoundException e){
		allocBuf(frameNo);
		bufPool[frameNo] = file->readPage(pageNo);
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		*page = bufPool[frameNo];
	}
	
	// try {
	// 	*page = file->readPage(pageNo);
	// 	uint32_t i;
	// 	for (i = 0; i < numBufs; i++) {

	// 	}
	
	// }
	// catch (...){

	// }
	// catch (InvalidPageException e) {
	// }

}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	try { 
		FrameId frameNo = 0;
		hashTable->lookup(file, pageNo, frameNo);
		if (dirty) {
			bufDescTable[frameNo].dirty = true;
		}
		if (bufDescTable[frameNo].pinCnt == 0) {
			throw new PageNotPinnedException(file->filename(), pageNo, frameNo);
		}
		bufDescTable[frameNo].pinCnt--;
	}
	catch (InvalidPageException e){
		std::cout << "An attempt was made to access an unpin an invalid page in a file." << std::endl;
	}

}

void BufMgr::flushFile(const File *file)
{
	uint32_t i;
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file ) {
			if (bufDescTable[i].pinCnt > 0) {
				throw new PagePinnedException(file->filename(),bufDescTable[i].pageNo, bufDescTable[i].frameNo);
			}
			else if (bufDescTable[i].valid == 0) {
				throw new BadBufferException(bufDescTable[i].frameNo, bufDescTable[i].dirty, bufDescTable[i].valid, bufDescTable[i].refbit);
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

	// FileIterator it = new FileIterator(file);

	// for (it = file->begin(); it != file->end(); ++it) {
		
	// }
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
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
