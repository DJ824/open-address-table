#include <chrono>
#include <iostream>
#include <random>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <iomanip>
#include "table.cpp"

void print_statistics(const std::vector<double>& measurements, const std::string& label) {
    if (measurements.empty()) return;

    auto sorted_measurements = measurements;
    std::sort(sorted_measurements.begin(), sorted_measurements.end());

    double sum = std::accumulate(measurements.begin(), measurements.end(), 0.0);
    double mean = sum / measurements.size();
    double median = measurements.size() % 2 == 0
                    ? (sorted_measurements[measurements.size()/2 - 1] + sorted_measurements[measurements.size()/2]) / 2
                    : sorted_measurements[measurements.size()/2];

    size_t p95_index = static_cast<size_t>(measurements.size() * 0.95);
    double p95 = sorted_measurements[p95_index];

    std::cout << label << ":\n"
              << "  Mean: " << std::fixed << std::setprecision(2) << mean << " ns/iter\n"
              << "  Median: " << median << " ns/iter\n"
              << "  P95: " << p95 << " ns/iter\n\n";
}

int main(int argc, char* argv[]) {
    const size_t WARMUP_RUNS = 3;
    const size_t MEASURED_RUNS = 5;
    const size_t size = 1'000'000;
    const size_t iters = 10'000'000;

    {
        std::vector<double> measurements;
        std::cout << "OpenAddressTable:\n";

        for (size_t run = 0; run < (WARMUP_RUNS + MEASURED_RUNS); ++run) {
            OpenAddressTable hashmap;
            std::minstd_rand generator(42 + run); 
            std::uniform_int_distribution<int> uniform_distribution(2, size);

            for (size_t i = 0; i < size; ++i) {
                const uint64_t value = uniform_distribution(generator);
                hashmap.insert(value, 0);
            }

            generator.seed(42 + run);

            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < iters; ++i) {
                const uint64_t value = uniform_distribution(generator);
                auto result = hashmap.get(value);
                if (!result.has_value()) {
                    hashmap.insert(value, 0);
                } else {
                    hashmap.erase(value);
                }
            }
            auto stop = std::chrono::high_resolution_clock::now();

            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    stop - start).count() / static_cast<double>(iters);

            if (run >= WARMUP_RUNS) {
                measurements.push_back(duration);
            }

            if (run == WARMUP_RUNS + MEASURED_RUNS - 1) {
                std::cout << "Final size: " << hashmap.size() << "\n";
            }
        }

        print_statistics(measurements, "OpenAddressTable Results");
    }

    {
        std::vector<double> measurements;
        std::cout << "std::unordered_map:\n";

        for (size_t run = 0; run < (WARMUP_RUNS + MEASURED_RUNS); ++run) {
            std::unordered_map<uint64_t, uint64_t> hashmap;
            hashmap.reserve(size);
            std::minstd_rand generator(42 + run);
            std::uniform_int_distribution<int> uniform_distribution(2, size);

            for (size_t i = 0; i < size; ++i) {
                const uint64_t value = uniform_distribution(generator);
                hashmap.insert({value, 0});
            }

            generator.seed(42 + run);

            auto start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < iters; ++i) {
                const uint64_t value = uniform_distribution(generator);
                auto it = hashmap.find(value);
                if (it == hashmap.end()) {
                    hashmap.insert({value, 0});
                } else {
                    hashmap.erase(it);
                }
            }
            auto stop = std::chrono::high_resolution_clock::now();

            double duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    stop - start).count() / static_cast<double>(iters);

            if (run >= WARMUP_RUNS) {
                measurements.push_back(duration);
            }

            if (run == WARMUP_RUNS + MEASURED_RUNS - 1) {
                std::cout << "Final size: " << hashmap.size() << "\n";
            }
        }

        print_statistics(measurements, "std::unordered_map Results");
    }

    return 0;
}
