#pragma once
#include <map>
#include <string>
using namespace std;
class MemTable {
public:
	void put(const string& key, const string& value);
	bool get(const string& key, string& value) const;
	void clear();
	size_t size() const;
	size_t byteSize() const;
	const map<string, string>& data() const;
private:
	map<string, string> table;
};