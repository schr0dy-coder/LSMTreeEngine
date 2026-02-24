	#include "engine/LSMEngine.h"
#include "sstable/SSTable.h"
#include <sstream>
#include <filesystem>
#include <iostream>
#include <memory>
#include <iomanip>
using namespace std;

LSMEngine::LSMEngine(const string& dir) : dataDir(dir)
{
	filesystem::create_directories(dataDir);
	wal = make_unique<WAL>(dataDir + "/wal.log");

	discoverSSTables();

	wal->replay([this](const string& key, const string& value) {
		memTable.put(key, value);
		});
}

void LSMEngine::discoverSSTables() {
	sstableFiles.clear();
	vector<pair<int, string>> sstables;

	for (const auto& entry : filesystem::directory_iterator(dataDir)) {
		if (entry.path().extension() == ".sst") {
			string filename = entry.path().stem().string();
			try {
				int gen = stoi(filename);
				sstables.push_back({ gen, entry.path().string() });
			}
			catch (...) {
				// Ignore files that don't match our naming convention
			}
		}
	}

	sort(sstables.begin(), sstables.end());

	for (const auto& p : sstables) {
		sstableFiles.push_back(p.second);
		sstableCounter = max(sstableCounter, static_cast<size_t>(p.first));
		SSTable::buildIndex(p.second, sstableIndexes[p.second]);
		
		// Rebuild Bloom Filter on startup
		for (const auto& [k, v] : sstableIndexes[p.second]) {
			sstableFilters[p.second].add(k);
		}
	}
}
void LSMEngine::put(const string& key, const string& value) {
	wal->append(key, string(value));
	wal->flush();
	memTable.put(key,string(value));
	flushIfNeeded();
}

void LSMEngine::del(const string& key) {
	wal->append(key, string(TOMBSTONE));
	wal->flush();
	memTable.put(key, string(TOMBSTONE));
	flushIfNeeded();
}

bool LSMEngine::get(const string& key, string& value) {
	// 1. Check MemTable
	if (memTable.get(key, value)) {
		return value != string(TOMBSTONE);
	}

	// 2. Check SSTables (Newest to Oldest)
	for (auto it = sstableFiles.rbegin(); it != sstableFiles.rend(); ++it) {
		const string& filePath = *it;
		
		// 2a. Check Bloom Filter first
		if (sstableFilters.count(filePath) && !sstableFilters[filePath].mightContain(key)) {
			continue; // Definitely not here, skip disk/index
		}

		// 2b. Check Index
		if (sstableIndexes.count(filePath)) {
			const auto& index = sstableIndexes[filePath];
			if (index.count(key)) {
				uint64_t offset = index.at(key);
				if (SSTable::getFromFile(filePath, key, value, offset)) {
					return value != string(TOMBSTONE);
				}
			}
		} else {
			// Fallback if index somehow doesn't exist (should not happen)
			if (SSTable::getFromFile(filePath, key, value)) {
				return value != string(TOMBSTONE);
			}
		}
	}

	return false;
}

void LSMEngine::flushIfNeeded() {
	size_t currentSize = memTable.byteSize();
	if (currentSize >= memtableByteLimit) {
		flushMemTable();
	}
}

void LSMEngine::flushMemTable() {
	if (memTable.size() == 0)
		return;
	
	stringstream ss;
	ss << dataDir << '\\' << setw(4) << setfill('0') << ++sstableCounter << ".sst";
	string finalPath = ss.str();
	string tempPath = dataDir + "\\temp.sst";
	
	// Atomic Rename Pattern:
	// 1. Write to temp file
	SSTable::writeToFile(tempPath, memTable.data());
	
	// 2. Rename to final destination
	filesystem::rename(tempPath, finalPath);

	sstableFiles.push_back(finalPath);
	SSTable::buildIndex(finalPath, sstableIndexes[finalPath]);
	
	// Populate Bloom Filter during flush
	for (const auto& [k, v] : memTable.data()) {
		sstableFilters[finalPath].add(k);
	}

	memTable.clear();
	wal->clear();
}

void LSMEngine::compact() {
	if (sstableFiles.empty()) return;

	map<string, string> mergedData;
	// Iterate chronological (oldest to newest) so newer entries naturally overwrite older ones in the map
	for (const auto& file : sstableFiles) {
		SSTable::readAllFromFile(file, mergedData);
	}

	// Remove tombstones during compaction (since this is major compaction merging all files)
	for (auto it = mergedData.begin(); it != mergedData.end(); ) {
		if (it->second == string(TOMBSTONE)) {
			it = mergedData.erase(it);
		} else {
			++it;
		}
	}

	// Create new SSTable using Atomic Rename Pattern
	stringstream ss;
	ss << dataDir << "\\" << setw(4) << setfill('0') << ++sstableCounter << ".sst";
	string finalPath = ss.str();
	string tempPath = dataDir + "\\temp_compact.sst";

	SSTable::writeToFile(tempPath, mergedData);
	filesystem::rename(tempPath, finalPath);

	// Cleanup old files, indexes and filters
	for (const auto& file : sstableFiles) {
		filesystem::remove(file);
		sstableIndexes.erase(file);
		sstableFilters.erase(file);
	}

	sstableFiles.clear();
	sstableFiles.push_back(finalPath);
	SSTable::buildIndex(finalPath, sstableIndexes[finalPath]);
	
	// Populate Bloom Filter for compacted file
	for (const auto& [k, v] : mergedData) {
		sstableFilters[finalPath].add(k);
	}
}