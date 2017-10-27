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
	bufDescTable = new BufDesc[bufs]; // describes the frames in the buffer (file, dirty, pin count, etc)

	// initialize the bufDescTable with appropriate values
	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs]; // the actual buffer of Pages

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;

	
}

BufMgr::~BufMgr()
{
	// flush all files in the buffer to disk
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
	// advances the clock hand through the indices of the buffer
	clockHand = (clockHand + 1) % numBufs;
	
}

void BufMgr::allocBuf(FrameId &frame)
{
	// check whether all frames are pinned
	bool allPinned = true;
	uint32_t i;
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].pinCnt == 0) {
			allPinned = false;
		}
	} 
	// if they are, throw an exception
	if (allPinned)
		throw BufferExceededException();

	// search for a frame that can be replaced
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
	
	// write the frame to disk before clearing it, if necessary
	if (bufDescTable[clockHand].dirty)
		bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
	
	// remove the file and page number from tables
	if (bufDescTable[clockHand].valid)
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
	bufDescTable[clockHand].Clear();
	// set the passed frameId to the newly allocated frame
	frame = clockHand;
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
	// badgerdb::Page new_page;
	// const badgerdb::RecordId& rid = new_page.insertRecord("hello, world!");
	// new_page.getRecord(rid); // returns "hello, world!"

	FrameId frameNo = 0;
	try { 
		// lookup the file and page number in the hashtable
		hashTable->lookup(file, pageNo, frameNo);
		// if it exists, the related frame number will be given. if not, catch statement will execute
		bufDescTable[frameNo].refbit = true;
		bufDescTable[frameNo].pinCnt++;
		page = &bufPool[frameNo];

	}
	catch (HashNotFoundException e) {
		// if the file's page is not already in the buffer, allocate a frame
		allocBuf(frameNo);
		// read the page into the newly allocated frame in the buffer
		bufPool[frameNo] = file->readPage(pageNo);
		// insert it into the hashtable and bufDescTable so we know its there
		hashTable->insert(file, pageNo, frameNo);
		bufDescTable[frameNo].Set(file, pageNo);
		page = &bufPool[frameNo];
	}

}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
	// find the page in the table, set it's dirty property in the desc table if necessary, and decrement its pin count
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
	// check that the file exists somewhere in the buffer, and has a pin count of 0 and is valid
	for (i = 0; i < numBufs; i++) {
		if (bufDescTable[i].file == file ) {
			if (bufDescTable[i].pinCnt > 0) {
				// can't dispose of a file that's still pinned
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
			// if the frame is dirty, indicating changes should be propogated to disk, then write them
			if (bufDescTable[i].dirty == true) {
				file->writePage(bufPool[i]);
				bufDescTable[i].dirty = false;
			}
			// remove the file from the tables
			hashTable->remove(file, bufDescTable[i].pageNo);
			bufDescTable[i].Clear();
		}
	}
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
	// allocates a page within a file, and inserts the file and page into the buffer
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
	// removes the given page from the tables we use to keep track of the buffer, then deletes it from the file
	FrameId  frameNo = 0;
	try {
		hashTable->lookup(file, PageNo, frameNo);
		bufDescTable[frameNo].Clear();
		hashTable->remove(file, PageNo);
	}	
	catch (HashNotFoundException e) {
		// std::cout << "HashNotFoundException BufMgr::disposePage(File *file, const PageId PageNo)" << std::endl;
	}
	file->deletePage(PageNo);
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	// prints out the BufMgr's bufDescTable
	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

}
}
