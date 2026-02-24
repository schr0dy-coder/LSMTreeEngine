#include "wal/WAL.h"
#include <filesystem>
#include <fstream>
#include <functional>
using namespace std;
WAL::WAL(const string& path) : filePath(path) {
	out.open(filePath, ios::app | ios::binary);
}

WAL::~WAL() {
	if (out.is_open()) {
		out.close();
	}
}

void WAL::append(const string& key, const string& value) {
    uint32_t keySize = static_cast<uint32_t>(key.size());
    uint32_t valueSize = static_cast<uint32_t>(value.size());

	out.write(reinterpret_cast<const char*>(&keySize), sizeof(uint32_t));
	out.write(key.data(), keySize);

	out.write(reinterpret_cast<const char*>(&valueSize), sizeof(uint32_t));
	out.write(value.data(), valueSize);
	out.flush(); // Ensure data is pushed to OS buffers
}

void WAL::flush() {
	out.flush();
}

void WAL::clear() {
	out.close();
	filesystem::remove(filePath);
	out.open(filePath, ios::app | ios::binary);
}

void WAL::replay(function<void(const string&, const string&)> callback) {
    std::ifstream in(filePath, ios::binary);

    if (!in.is_open())
        return;

    while (true) {
        uint32_t keySize, valueSize;

        if (!in.read(reinterpret_cast<char*>(&keySize), sizeof(uint32_t)))
            break;

        string key(keySize, '\0');
        if (!in.read(key.data(), keySize))
            break;

        if (!in.read(reinterpret_cast<char*>(&valueSize), sizeof(uint32_t)))
            break;

        std::string value(valueSize, '\0');
        if (!in.read(value.data(), valueSize))
            break;

        callback(key, value);
    }

    in.close();
}