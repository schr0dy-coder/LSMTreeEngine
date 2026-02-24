#pragma once
#include <string>
#include <map>
using namespace std;
class SSTable {
public:
	static void writeToFile(
		const string& filePath, const map<string, string>& data
	);
	static bool getFromFile(
		const string& filePath,
		const string& searchKey,
		string& valueOut,
		uint64_t offset = 0
		);
	static void readAllFromFile(
		const string& filePath,
		map<string, string>& dataOut
	);
	static void buildIndex(
		const string& filePath,
		map<string, uint64_t>& index
	);
};