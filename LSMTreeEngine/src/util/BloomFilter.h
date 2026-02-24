#pragma once
#include <vector>
#include <string>
#include <functional>

class BloomFilter {
public:
    explicit BloomFilter(size_t size = 1000, int numHashes = 3) 
        : bits(size, false), numHashes(numHashes) {}

    void add(const std::string& key) {
        for (int i = 0; i < numHashes; ++i) {
            size_t hash = hashFunc(key, i);
            bits[hash % bits.size()] = true;
        }
    }

    bool mightContain(const std::string& key) const {
        if (bits.empty()) return true; // Safety default
        for (int i = 0; i < numHashes; ++i) {
            size_t hash = hashFunc(key, i);
            if (!bits[hash % bits.size()]) {
                return false;
            }
        }
        return true;
    }

    void clear() {
        std::fill(bits.begin(), bits.end(), false);
    }

private:
    std::vector<bool> bits;
    int numHashes;

    size_t hashFunc(const std::string& key, int seed) const {
        // Simple hash combining key and seed
        size_t h = std::hash<std::string>{}(key);
        return h ^ (seed * 0x9e3779b9 + (h << 6) + (h >> 2));
    }
};
