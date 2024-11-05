#include <gtest/gtest.h>
#include <random>
#include <unordered_set>
#include <algorithm>
#include "table.cpp"

class OpenAddressTableTest : public ::testing::Test {
protected:
    OpenAddressTable table;

    void SetUp() override {
        table = OpenAddressTable(16);
    }
};

TEST_F(OpenAddressTableTest, EmptyTableOperations) {
    EXPECT_TRUE(table.empty());
    EXPECT_EQ(table.size(), 0);
    EXPECT_EQ(table.capacity(), 16);
    EXPECT_FALSE(table.erase(1));
    EXPECT_FALSE(table.get(1).has_value());
}

TEST_F(OpenAddressTableTest, BasicInsertAndGet) {
    EXPECT_TRUE(table.insert(1, 100));
    EXPECT_FALSE(table.empty());
    EXPECT_EQ(table.size(), 1);

    auto result = table.get(1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 100);
}

TEST_F(OpenAddressTableTest, UpdateExistingKey) {
    EXPECT_TRUE(table.insert(1, 100));
    EXPECT_TRUE(table.insert(1, 200));  // Update
    EXPECT_EQ(table.size(), 1);

    auto result = table.get(1);
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 200);
}

TEST_F(OpenAddressTableTest, BasicErase) {
    EXPECT_TRUE(table.insert(1, 100));
    EXPECT_TRUE(table.erase(1));
    EXPECT_EQ(table.size(), 0);
    EXPECT_FALSE(table.get(1).has_value());
}

TEST_F(OpenAddressTableTest, ResizeTriggering) {
    const size_t initial_capacity = table.capacity();

    size_t inserted = 0;
    while (table.capacity() == initial_capacity) {
        EXPECT_TRUE(table.insert(inserted, inserted * 10));
        inserted++;
    }

    for (size_t i = 0; i < inserted; i++) {
        auto result = table.get(i);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), i * 10);
    }
}

TEST_F(OpenAddressTableTest, ProbeSequenceHandling) {
    const size_t mask = table.capacity() - 1;
    std::vector<uint64_t> keys;

    for (uint64_t i = 0; i < 5; i++) {
        uint64_t key = i * table.capacity();
        keys.push_back(key);
        EXPECT_TRUE(table.insert(key, i));
    }

    for (size_t i = 0; i < keys.size(); i++) {
        auto result = table.get(keys[i]);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), i);
    }
}

TEST_F(OpenAddressTableTest, EdgeCases) {
    // Test with extreme values
    EXPECT_TRUE(table.insert(UINT64_MAX, 100));
    EXPECT_TRUE(table.insert(0, 200));

    auto result1 = table.get(UINT64_MAX);
    auto result2 = table.get(0);

    EXPECT_TRUE(result1.has_value());
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result1.value(), 100);
    EXPECT_EQ(result2.value(), 200);
}

TEST_F(OpenAddressTableTest, LoadFactorBehavior) {
    const double threshold = OpenAddressTable::LOAD_FACTOR_THRESHOLD;
    const size_t initial_capacity = table.capacity();
    size_t max_elements = static_cast<size_t>(initial_capacity * threshold);

    for (size_t i = 0; i < max_elements; i++) {
        EXPECT_TRUE(table.insert(i, i));
        EXPECT_LE(table.load_factor(), threshold);
    }

    // Next insert should trigger resize
    EXPECT_TRUE(table.insert(max_elements, max_elements));
    EXPECT_GT(table.capacity(), initial_capacity);
}

TEST_F(OpenAddressTableTest, DeletionPatterns) {
    std::vector<uint64_t> keys = {1, 2, 3, 4, 5};
    for (auto key : keys) {
        EXPECT_TRUE(table.insert(key, key * 10));
    }

    EXPECT_TRUE(table.erase(3));
    EXPECT_TRUE(table.erase(1));
    EXPECT_TRUE(table.erase(5));

    EXPECT_TRUE(table.get(2).has_value());
    EXPECT_TRUE(table.get(4).has_value());
    EXPECT_FALSE(table.get(1).has_value());
    EXPECT_FALSE(table.get(3).has_value());
    EXPECT_FALSE(table.get(5).has_value());

    EXPECT_TRUE(table.insert(6, 60));
    EXPECT_TRUE(table.insert(7, 70));

    auto result1 = table.get(6);
    auto result2 = table.get(7);
    EXPECT_TRUE(result1.has_value());
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result1.value(), 60);
    EXPECT_EQ(result2.value(), 70);
}

TEST_F(OpenAddressTableTest, StressTest) {
    const size_t num_operations = 10000;
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dis;
    std::unordered_map<uint64_t, uint64_t> reference_map;

    for (size_t i = 0; i < num_operations; i++) {
        uint64_t key = dis(gen);
        uint64_t value = dis(gen);

        switch (i % 3) {
            case 0: {
                table.insert(key, value);
                reference_map[key] = value;
                break;
            }
            case 1: { // Get
                auto table_result = table.get(key);
                auto map_result = reference_map.find(key);
                if (map_result != reference_map.end()) {
                    EXPECT_TRUE(table_result.has_value());
                    EXPECT_EQ(table_result.value(), map_result->second);
                } else {
                    EXPECT_FALSE(table_result.has_value());
                }
                break;
            }
            case 2: { // Erase
                bool table_erase = table.erase(key);
                bool map_erase = reference_map.erase(key) > 0;
                EXPECT_EQ(table_erase, map_erase);
                break;
            }
        }
    }

    for (const auto& [key, value] : reference_map) {
        auto result = table.get(key);
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), value);
    }
}