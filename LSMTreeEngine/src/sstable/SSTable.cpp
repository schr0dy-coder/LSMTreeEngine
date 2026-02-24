#include "sstable/SSTable.h"
#include <fstream>
#include<iostream>
using namespace std;
void SSTable::writeToFile(
	const string& filePath,
	const map<string, string>& data
) {
	ofstream out(filePath, ios::binary | ios::trunc);
	for (const auto& [key, value] : data) {
		uint32_t keySize = static_cast<uint32_t>(key.size());
		uint32_t valueSize = static_cast<uint32_t>(value.size());
		out.write(reinterpret_cast<const char*>(&keySize), sizeof(uint32_t));
		out.write(key.data(), keySize);

		out.write(reinterpret_cast<const char*>(&valueSize), sizeof(uint32_t));
		out.write(value.data(), valueSize);
	}
	out.close();
}

bool SSTable::getFromFile(
	const string& filePath,
	const string& searchKey,
	string& valueOut,
	uint64_t offset
) {
	ifstream in(filePath, ios::binary);
	if (!in.is_open())
		return false;
	
	in.seekg(offset);

	while (true) {
		uint32_t keySize, valueSize;
		if (!in.read(reinterpret_cast<char*>(&keySize), sizeof(uint32_t)))
			break;
		string key(keySize, '\0');
		if (!in.read(key.data(), keySize))
			break;
		if (!in.read(reinterpret_cast<char*>(&valueSize), sizeof(uint32_t)))
			break;
		string value(valueSize, '\0');
		if (!in.read(value.data(), valueSize))
			break;
		
		if (key == searchKey) {
			valueOut = value;
			return true;
		}
	}
	return false;
}

void SSTable::readAllFromFile(const string& filePath, map<string, string>& dataOut) {
	ifstream in(filePath, ios::binary);
	if (!in.is_open()) return;

	while (true) {
		uint32_t keySize, valueSize;
		if (!in.read(reinterpret_cast<char*>(&keySize), sizeof(uint32_t)))
			break;
		string key(keySize, '\0');
		if (!in.read(key.data(), keySize))
			break;
		if (!in.read(reinterpret_cast<char*>(&valueSize), sizeof(uint32_t)))
			break;
		string value(valueSize, '\0');
		if (!in.read(value.data(), valueSize))
			break;

		dataOut[key] = value;
	}
}

void SSTable::buildIndex(const string& filePath, map<string, uint64_t>& index) {
	ifstream in(filePath, ios::binary);
	if (!in.is_open()) return;

	while (true) {
		uint64_t currentOffset = in.tellg();
		uint32_t keySize, valueSize;
		
		if (!in.read(reinterpret_cast<char*>(&keySize), sizeof(uint32_t)))
			break;
		string key(keySize, '\0');
		if (!in.read(key.data(), keySize))
			break;
		
		index[key] = currentOffset;

		if (!in.read(reinterpret_cast<char*>(&valueSize), sizeof(uint32_t)))
			break;
		in.seekg(valueSize, ios::cur);
	}
}
