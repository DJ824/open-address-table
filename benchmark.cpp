#include <benchmark/benchmark.h>
#include <random>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <numeric>
#include "table.cpp"

const size_t NUM_OPERATIONS = 10'000'000;
const size_t INITIAL_SIZE = 1'000'000;
const size_t WARMUP_RUNS = 3;

struct Statistics {
    double mean;
    double median;
    double p95;
    double min;
    double max;
};

Statistics calculate_stats(std::vector<double>& measurements) {
    if (measurements.empty()) return {0, 0, 0, 0, 0};

    std::sort(measurements.begin(), measurements.end());

    double sum = std::accumulate(measurements.begin(), measurements.end(), 0.0);
    double mean = sum / measurements.size();
    double median = measurements.size() % 2 == 0
                    ? (measurements[measurements.size()/2 - 1] + measurements[measurements.size()/2]) / 2
                    : measurements[measurements.size()/2];

    size_t p95_index = static_cast<size_t>(measurements.size() * 0.95);
    double p95 = measurements[p95_index];

    return {
            mean,
            median,
            p95,
            measurements.front(),
            measurements.back()
    };
}

static void BM_OpenAddressTable_MixedWithWarmup(benchmark::State& state) {
    std::vector<double> measurements;

    for (auto _ : state) {
        state.PauseTiming();
        OpenAddressTable hashmap;
        std::minstd_rand generator(42);  // Fixed seed for reproducibility
        std::uniform_int_distribution<int> uniform_distribution(2, INITIAL_SIZE);

        // Initial insertions
        for (size_t i = 0; i < INITIAL_SIZE; ++i) {
            const uint64_t value = uniform_distribution(generator);
            hashmap.insert(value, 0);
        }

        // Reset generator for consistent operation mix
        generator.seed(42);
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            const uint64_t value = uniform_distribution(generator);
            auto result = hashmap.get(value);
            if (!result.has_value()) {
                hashmap.insert(value, 0);
            } else {
                hashmap.erase(value);
            }
        }
        auto end = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                end - start).count() / static_cast<double>(NUM_OPERATIONS);

        // Only store measurements after warmup
        if (state.iterations() > WARMUP_RUNS) {
            measurements.push_back(duration);
        }

        state.SetItemsProcessed(NUM_OPERATIONS);
    }

    // Calculate and report statistics
    auto stats = calculate_stats(measurements);
    state.counters["mean_ns"] = stats.mean;
    state.counters["median_ns"] = stats.median;
    state.counters["p95_ns"] = stats.p95;
    state.counters["min_ns"] = stats.min;
    state.counters["max_ns"] = stats.max;
}

// Benchmark for std::unordered_map with warmup
static void BM_UnorderedMap_MixedWithWarmup(benchmark::State& state) {
    std::vector<double> measurements;

    for (auto _ : state) {
        state.PauseTiming();
        std::unordered_map<uint64_t, uint64_t> hashmap;
        hashmap.reserve(INITIAL_SIZE);
        std::minstd_rand generator(42);  // Fixed seed for reproducibility
        std::uniform_int_distribution<int> uniform_distribution(2, INITIAL_SIZE);

        // Initial insertions
        for (size_t i = 0; i < INITIAL_SIZE; ++i) {
            const uint64_t value = uniform_distribution(generator);
            hashmap.insert({value, 0});
        }

        // Reset generator for consistent operation mix
        generator.seed(42);
        state.ResumeTiming();

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < NUM_OPERATIONS; ++i) {
            const uint64_t value = uniform_distribution(generator);
            auto it = hashmap.find(value);
            if (it == hashmap.end()) {
                hashmap.insert({value, 0});
            } else {
                hashmap.erase(it);
            }
        }
        auto end = std::chrono::high_resolution_clock::now();

        double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                end - start).count() / static_cast<double>(NUM_OPERATIONS);

        // Only store measurements after warmup
        if (state.iterations() > WARMUP_RUNS) {
            measurements.push_back(duration);
        }

        state.SetItemsProcessed(NUM_OPERATIONS);
    }

    // Calculate and report statistics
    auto stats = calculate_stats(measurements);
    state.counters["mean_ns"] = stats.mean;
    state.counters["median_ns"] = stats.median;
    state.counters["p95_ns"] = stats.p95;
    state.counters["min_ns"] = stats.min;
    state.counters["max_ns"] = stats.max;
}

// Register benchmarks with appropriate settings
BENCHMARK(BM_OpenAddressTable_MixedWithWarmup)
        ->Unit(benchmark::kMicrosecond)
        ->Iterations(WARMUP_RUNS + 5)  // 3 warmup + 5 measured runs
        ->UseRealTime();

BENCHMARK(BM_UnorderedMap_MixedWithWarmup)
        ->Unit(benchmark::kMicrosecond)
        ->Iterations(WARMUP_RUNS + 5)  // 3 warmup + 5 measured runs
        ->UseRealTime();

BENCHMARK_MAIN();