#include "memtable/MemTable.h"
using namespace std;

void MemTable::put(const string& key, const string& value) {
	table[key] = value;
}

bool MemTable::get(const string& key, string& value) const { // Change 'const string& value' to 'string& value'
	auto it = table.find(key);
	if (it == table.end()) {
		return false;
	}
	value = it->second; // This now works because 'value' is a non-const reference
	return true;
}

void MemTable::clear() {
	table.clear();
}
size_t MemTable::size() const {
	return table.size();
}
size_t MemTable::byteSize() const {
	size_t total = 0;
	for (const auto& [k, v] : table) {
		total += k.size() + v.size();
	}
	return total;
}
const map<string, string>& MemTable::data() const {
	return table;
}