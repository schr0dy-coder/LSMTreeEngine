#pragma once
#include <string>
#include <vector>
#include "memtable/MemTable.h"
#include "wal/WAL.h"
#include "util/BloomFilter.h"
using namespace std;
class LSMEngine {
public:
	explicit LSMEngine(const string& dataDir);

	void put(const string& key, const string& value);
	void del(const string& key);
	bool get(const string& key, string& value);
	void compact();

	static constexpr const char* TOMBSTONE = "__TOMBSTONE__";

private:
	string dataDir;
	MemTable memTable;
	unique_ptr<WAL> wal;
	size_t memtableByteLimit = 100;
	size_t sstableCounter = 0;
	vector<string> sstableFiles;
	map<string, map<string, uint64_t>> sstableIndexes;
	map<string, BloomFilter> sstableFilters;
	void flushIfNeeded();
	void flushMemTable();
	void discoverSSTables();
};