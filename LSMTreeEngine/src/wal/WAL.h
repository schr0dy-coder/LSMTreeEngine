#pragma once 
#include <string> 
#include <fstream>
#include <functional>
using namespace std;
class WAL {
public:
	explicit WAL(const string& filepath);
	~WAL();
	void replay(function<void(const string&, const string&)> callback);
	void append(const string& key, const string& value);
	void clear();
	void flush();

private:
	string filePath;
	ofstream out;
};