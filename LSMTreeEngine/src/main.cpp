#include "engine/LSMEngine.h"
#include <filesystem>
#include <sstable/SSTable.h>
#include <iostream>
#include <chrono>
#include <iomanip>
#include <vector>
#include <random>
using namespace std;

void runBenchmark() {
    std::filesystem::remove_all("data");
    
    // Use a larger MemTable limit for the benchmark to reduce flush frequency
    LSMEngine engine("data");

    const int numInserts = 2000;
    cout << "--- Starting Benchmark (" << numInserts << " inserts) ---\n";

    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < numInserts; ++i) {
        string key = "key_" + std::to_string(i);
        string val = "value_" + std::to_string(i);
        engine.put(key, val);
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    cout << "Bulk Insert Throughput: " << std::fixed << std::setprecision(2) 
         << (numInserts / diff.count()) << " Ops/sec\n";

    cout << "\n--- Starting Random Read Latency Test (100 reads) ---\n";
    std::mt19937 rng(42);
    std::uniform_int_distribution<int> dist(0, numInserts * 2); // 50% hits, 50% misses

    auto rStart = std::chrono::high_resolution_clock::now();
    int hits = 0;
    for (int i = 0; i < 100; ++i) {
        string key = "key_" + std::to_string(dist(rng));
        string v;
        if (engine.get(key, v)) hits++;
    }
    auto rEnd = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> rDiff = rEnd - rStart;

    cout << "Average Read Latency: " << (rDiff.count() / 100.0) << " ms\n";
    cout << "Read Hit Rate: " << hits << "%\n";
}

int main() {
    runBenchmark();
    return 0;
}