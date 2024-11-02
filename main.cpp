#include <vector>
#include <optional>
#include <chrono>
#include <iostream>
#include <random>
#include <unordered_map>
#include <iomanip>
#include "open_address_table.cpp"

void benchmark(size_t num_operations) {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;

    std::vector<uint64_t> test_keys(num_operations);
    for (size_t i = 0; i < num_operations; ++i) {
        test_keys[i] = dis(gen);
    }

    size_t erase_count = num_operations * 0.3;
    std::vector<size_t> erase_indexes(erase_count);
    for (size_t i = 0; i < erase_count; ++i) {
        erase_indexes[i] = i * 3; // Erase every third key
    }

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "testing with " << num_operations << " operations\n";
    std::cout << "will erase " << erase_count << " keys\n\n";

    {
        std::unordered_map<uint64_t, uint64_t> map;

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            map[test_keys[i]] = test_keys[i];
        }
        auto insert_end = std::chrono::high_resolution_clock::now();
        auto insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end - start);

        uint64_t sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            auto it = map.find(test_keys[i]);
            if (it != map.end()) sum += it->second;
        }
        auto lookup_end = std::chrono::high_resolution_clock::now();
        auto lookup_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end - start);

        start = std::chrono::high_resolution_clock::now();
        for (size_t idx : erase_indexes) {
            map.erase(test_keys[idx]);
        }
        auto erase_end = std::chrono::high_resolution_clock::now();
        auto erase_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(erase_end - start);

        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            auto it = map.find(test_keys[i]);
            if (it != map.end()) sum += it->second;
        }
        auto lookup_after_erase_end = std::chrono::high_resolution_clock::now();
        auto lookup_after_erase_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_after_erase_end - start);

        std::cout << "std::unordered_map:\n";
        std::cout << "insert time: " << insert_duration.count() << " ns (avg "
                  << (insert_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "lookup time (before erase): " << lookup_duration.count() << " ns (avg "
                  << (lookup_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "erase time: " << erase_duration.count() << " ns (avg "
                  << (erase_duration.count() / erase_count) << " ns/op)\n";
        std::cout << "lookup time (after erase): " << lookup_after_erase_duration.count() << " ns (avg "
                  << (lookup_after_erase_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "final size: " << map.size() << "\n\n";
    }

    {
        OpenAddressHashMap map;

        auto start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            map.insert(test_keys[i], test_keys[i]);
        }
        auto insert_end = std::chrono::high_resolution_clock::now();
        auto insert_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(insert_end - start);

        uint64_t sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            auto val = map.get(test_keys[i]);
            if (val) sum += *val;
        }
        auto lookup_end = std::chrono::high_resolution_clock::now();
        auto lookup_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_end - start);

        start = std::chrono::high_resolution_clock::now();
        for (size_t idx : erase_indexes) {
            map.erase(test_keys[idx]);
        }
        auto erase_end = std::chrono::high_resolution_clock::now();
        auto erase_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(erase_end - start);

        sum = 0;
        start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < num_operations; ++i) {
            auto val = map.get(test_keys[i]);
            if (val) sum += *val;
        }
        auto lookup_after_erase_end = std::chrono::high_resolution_clock::now();
        auto lookup_after_erase_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(lookup_after_erase_end - start);

        std::cout << "OpenAddressTable:\n";
        std::cout << "insert time: " << insert_duration.count() << " ns (avg "
                  << (insert_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "lookup time (before erase): " << lookup_duration.count() << " ns (avg "
                  << (lookup_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "erase time: " << erase_duration.count() << " ns (avg "
                  << (erase_duration.count() / erase_count) << " ns/op)\n";
        std::cout << "lookup time (after erase): " << lookup_after_erase_duration.count() << " ns (avg "
                  << (lookup_after_erase_duration.count() / num_operations) << " ns/op)\n";
        std::cout << "final size: " << map.size() << "\n";
    }
}

int main() {
    const size_t NUM_OPERATIONS = 10000000;
    benchmark(NUM_OPERATIONS);
    return 0;
}