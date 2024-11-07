#include <benchmark/benchmark.h>
#include <random>
#include <unordered_map>
#include "table.cpp"

const size_t NUM_OPERATIONS = 10000000;

// Generate consistent test data
std::vector<uint64_t> generate_test_keys() {
    std::mt19937_64 gen(1234);  // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dis;
    std::vector<uint64_t> keys(NUM_OPERATIONS);
    for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
        keys[i] = dis(gen);
    }
    return keys;
}

static const std::vector<uint64_t> TEST_KEYS = generate_test_keys();
static const size_t ERASE_COUNT = NUM_OPERATIONS * 0.3;
static std::vector<size_t> ERASE_INDICES = []() {
    std::vector<size_t> indices(ERASE_COUNT);
    for (size_t i = 0; i < ERASE_COUNT; ++i) {
        indices[i] = i * 3;
    }
    return indices;
}();

// Sequential Insert Benchmarks
static void BM_UnorderedMap_SequentialInsert(benchmark::State& state) {
    for (auto _ : state) {
        std::unordered_map<uint64_t, uint64_t> map;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            benchmark::DoNotOptimize(map[TEST_KEYS[i]] = TEST_KEYS[i]);
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

static void BM_OpenAddressTable_SequentialInsert(benchmark::State& state) {
    for (auto _ : state) {
        OpenAddressTable table;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            benchmark::DoNotOptimize(table.insert(TEST_KEYS[i], TEST_KEYS[i]));
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

// Sequential Lookup Benchmarks
static void BM_UnorderedMap_SequentialLookup(benchmark::State& state) {
    std::unordered_map<uint64_t, uint64_t> map;
    for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
        map[TEST_KEYS[i]] = TEST_KEYS[i];
    }

    for (auto _ : state) {
        uint64_t sum = 0;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            auto it = map.find(TEST_KEYS[i]);
            if (it != map.end()) benchmark::DoNotOptimize(sum += it->second);
        }
        benchmark::DoNotOptimize(sum);
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

static void BM_OpenAddressTable_SequentialLookup(benchmark::State& state) {
    OpenAddressTable table;
    for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
        table.insert(TEST_KEYS[i], TEST_KEYS[i]);
    }

    for (auto _ : state) {
        uint64_t sum = 0;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            auto val = table.get(TEST_KEYS[i]);
            if (val) benchmark::DoNotOptimize(sum += *val);
        }
        benchmark::DoNotOptimize(sum);
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

// Sequential Delete Benchmarks
static void BM_UnorderedMap_SequentialDelete(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        std::unordered_map<uint64_t, uint64_t> map;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            map[TEST_KEYS[i]] = TEST_KEYS[i];
        }
        state.ResumeTiming();

        for (size_t idx : ERASE_INDICES) {
            benchmark::DoNotOptimize(map.erase(TEST_KEYS[idx]));
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(ERASE_COUNT);
    }
}

static void BM_OpenAddressTable_SequentialDelete(benchmark::State& state) {
    for (auto _ : state) {
        state.PauseTiming();
        OpenAddressTable table;
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            table.insert(TEST_KEYS[i], TEST_KEYS[i]);
        }
        state.ResumeTiming();

        for (size_t idx : ERASE_INDICES) {
            benchmark::DoNotOptimize(table.erase(TEST_KEYS[idx]));
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(ERASE_COUNT);
    }
}

// Add these to your benchmark.cpp file

// Helper function for random operations (similar to your first benchmark)
inline std::pair<uint64_t, uint64_t> get_random_pair(std::mt19937_64& gen,
                                                     std::uniform_int_distribution<uint64_t>& dis) {
    return {dis(gen), dis(gen)};
}

// Mixed Operations Benchmark for std::unordered_map
static void BM_UnorderedMap_MixedOperations(benchmark::State& state) {
    std::mt19937_64 gen(1234);  // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dis;

    for (auto _ : state) {
        std::unordered_map<uint64_t, uint64_t> map;
        map.reserve(NUM_OPERATIONS / 2);

        for (size_t i = 0; i < NUM_OPERATIONS; i++) {
            auto [key, value] = get_random_pair(gen, dis);
            switch (i % 3) {
                case 0: {  // Insert
                    benchmark::DoNotOptimize(map.insert_or_assign(key, value));
                    break;
                }
                case 1: {  // Get
                    auto it = map.find(key);
                    if (it != map.end()) benchmark::DoNotOptimize(it->second);
                    break;
                }
                case 2: {  // Erase
                    benchmark::DoNotOptimize(map.erase(key));
                    break;
                }
            }
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

// Mixed Operations Benchmark for OpenAddressTable
static void BM_OpenAddressTable_MixedOperations(benchmark::State& state) {
    std::mt19937_64 gen(1234);  // Fixed seed for reproducibility
    std::uniform_int_distribution<uint64_t> dis;

    for (auto _ : state) {
        OpenAddressTable table(NUM_OPERATIONS / 2);  // Start with reasonable size

        for (size_t i = 0; i < NUM_OPERATIONS; i++) {
            auto [key, value] = get_random_pair(gen, dis);
            switch (i % 3) {
                case 0: {  // Insert
                    benchmark::DoNotOptimize(table.insert(key, value));
                    break;
                }
                case 1: {  // Get
                    auto val = table.get(key);
                    if (val) benchmark::DoNotOptimize(*val);
                    break;
                }
                case 2: {  // Erase
                    benchmark::DoNotOptimize(table.erase(key));
                    break;
                }
            }
        }
        benchmark::ClobberMemory();
        state.SetItemsProcessed(NUM_OPERATIONS);
    }
}

BENCHMARK(BM_UnorderedMap_MixedOperations)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_OpenAddressTable_MixedOperations)->Unit(benchmark::kMillisecond);

BENCHMARK(BM_UnorderedMap_SequentialInsert)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_OpenAddressTable_SequentialInsert)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UnorderedMap_SequentialLookup)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_OpenAddressTable_SequentialLookup)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_UnorderedMap_SequentialDelete)->Unit(benchmark::kMillisecond);
BENCHMARK(BM_OpenAddressTable_SequentialDelete)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();