//
// Created by Devang Jaiswal on 11/2/24.
//
#include <vector>
#include <optional>
#include "xxhash/xxhash.h"

struct Entry {
    uint64_t key_;
    uint64_t val_;
    uint16_t probe_dist_;
    uint8_t status_;
} __attribute__((packed, aligned(16)));

class OpenAddressTable {
public:
    // ensure the vector stats at the 64 byte cache line boundary
    alignas(64) std::vector<Entry> data_;
    size_t size_;
    size_t tombstone_ct_;

    static constexpr size_t CACHE_LINE_SIZE = 64;
    static constexpr size_t ENTRIES_PER_CACHE_LINE = 4;
    static constexpr double LOAD_FACTOR_THRESHOLD = 0.75;
    // prefetch 4 cache lines
    static constexpr size_t PREFETCH_DISTANCE = 4;


    explicit OpenAddressTable(size_t initial_size = 64)
            : data_(initial_size), size_(0), tombstone_ct_(0) {
        std::fill(data_.begin(), data_.end(), Entry{0, 0, 0, 0});
    }

    static size_t hash_key(uint64_t key) {
        return XXH64(&key, sizeof(key), 0);
    }


    __attribute__((always_inline))
    size_t next_probe_position(size_t current_pos) {
        size_t next_pos = (current_pos + 1) & (data_.size() - 1);

        if (next_pos % ENTRIES_PER_CACHE_LINE == 0) {
            for (size_t i = 1; i <= PREFETCH_DISTANCE; ++i) {
                size_t prefetch_pos = (next_pos + i * CACHE_LINE_SIZE) & (data_.size() - 1);
                // 0 for reading, and 3 for high temporal locality
                // as each entry is accessed multiple times in succession in the operations
                // keeps recently accessed data (hot data) in l1 cache, reducing time for cache miss penalty
                __builtin_prefetch(&data_[prefetch_pos], 0, 3);
            }
        }

        return next_pos;
    }

    __attribute__((always_inline))
    void resize() {
        if (data_.empty()) {
            data_.resize(16);
            return;
        }

        const size_t old_size = data_.size();
        const size_t new_size = old_size * 2;

        std::vector<Entry> new_data(new_size);

        for (size_t i = 0; i < std::min(new_size, size_t(64)) / CACHE_LINE_SIZE; ++i) {
            __builtin_prefetch(&new_data[i * CACHE_LINE_SIZE]);
        }

        size_ = 0;
        tombstone_ct_ = 0;

        for (size_t i = 0; i < old_size; i += ENTRIES_PER_CACHE_LINE) {
            __builtin_prefetch(&data_[std::min(i + ENTRIES_PER_CACHE_LINE * 4, old_size)]);

            for (size_t j = 0; j < ENTRIES_PER_CACHE_LINE && i + j < old_size; ++j) {
                const auto& entry = data_[i + j];
                if (entry.status_ == 2) {
                    insert_during_resize(new_data, entry.key_, entry.val_);
                }
            }
        }

        data_ = std::move(new_data);
    }

    __attribute__((always_inline))
    void insert_during_resize(std::vector<Entry>& new_data, uint64_t key, uint64_t val) {
        const size_t mask = new_data.size() - 1;
        size_t pos = hash_key(key) & mask;
        size_t probe_dist = 0;

        while (true) {
            if (new_data[pos].status_ == 0) {
                new_data[pos] = Entry{key, val, static_cast<uint8_t>(probe_dist), 2};
                ++size_;
                return;
            }

            if (probe_dist > new_data[pos].probe_dist_) {
                Entry entry{key, val, static_cast<uint8_t>(probe_dist), 2};
                std::swap(entry, new_data[pos]);
                key = entry.key_;
                val = entry.val_;
                probe_dist = entry.probe_dist_;
            }

            pos = (pos + 1) & mask;
            ++probe_dist;
        }
    }


    __attribute__((always_inline))
    bool insert(uint64_t key, uint64_t val) {
        if (load_factor() >= LOAD_FACTOR_THRESHOLD) {
            resize();
        }

        const size_t mask = data_.size() - 1;
        size_t pos = hash_key(key) & mask;

        for (size_t i = 0; i <= PREFETCH_DISTANCE; ++i) {
            __builtin_prefetch(&data_[pos + i * CACHE_LINE_SIZE], 1, 3);
        }

        Entry entry{key, val, 0, 2};
        size_t probe_dist = 0;

        while (true) {
            if (data_[pos].status_ == 0) {  // Empty slot
                data_[pos] = entry;
                ++size_;
                return true;
            }

            if (data_[pos].status_ == 2 && data_[pos].key_ == key) {
                data_[pos].val_ = val;
                return true;
            }

            if (probe_dist > data_[pos].probe_dist_) {
                if (data_[pos].status_ != 1) {
                    std::swap(entry, data_[pos]);
                }
                probe_dist = entry.probe_dist_;
            }

            pos = next_probe_position(pos);
            ++probe_dist;
            entry.probe_dist_ = static_cast<uint8_t>(probe_dist);
        }
    }


    __attribute__((always_inline))
    std::optional<uint64_t> get(uint64_t key) {
        if (data_.empty()) {
            return std::nullopt;
        }

        const size_t mask = data_.size() - 1;
        size_t pos = hash_key(key) & mask;
        size_t probe_dist = 0;

        __builtin_prefetch(&data_[pos + ENTRIES_PER_CACHE_LINE]);

        while (true) {
            if (data_[pos].status_ == 2) {
                if (data_[pos].key_ == key) {
                    return data_[pos].val_;
                }

                if (probe_dist > data_[pos].probe_dist_) {
                    return std::nullopt;
                }

            } else if (data_[pos].status_ == 0) {
                return std::nullopt;
            }

            pos = next_probe_position(pos);
            ++probe_dist;
        }
    }

    __attribute__((always_inline))
    bool erase(uint64_t key) {
        if (data_.empty()) {
            return false;
        }

        const size_t mask = data_.size() - 1;
        size_t pos = hash_key(key) & mask;
        size_t probe_dist = 0;

        while (true) {
            if (data_[pos].status_ == 0) {
                return false;
            }

            if (data_[pos].status_ == 2 && data_[pos].key_ == key) {
                auto curr_pos = pos;

                // backward shift deletion, probe and shift back until we find an empty entry or a new hash origin
                while (true) {
                    size_t next_pos = next_probe_position(curr_pos);

                    if (data_[next_pos].status_ != 2 || data_[next_pos].probe_dist_ == 0) {
                        data_[curr_pos] = Entry{0, 0, 0, 0};
                        break;
                    }

                    data_[curr_pos] = data_[next_pos];
                    data_[curr_pos].probe_dist_--;
                    curr_pos = next_pos;
                }
                --size_;
                return true;
            }
            if (probe_dist > data_[pos].probe_dist_) {
                return false;
            }

            pos = next_probe_position(pos);
            ++probe_dist;
        }
    }

    size_t size() const { return size_; }

    bool empty() const { return size_ == 0; }

    size_t capacity() const { return data_.size(); }

    double load_factor() const {
        return data_.empty() ? 0.0 : static_cast<double>(size_) / data_.size();
    }
};

/* no inline
 * OpenAddressTable:
insert time: 1621415083 ns (avg 162 ns/op)
lookup time (before erase): 488703458 ns (avg 48 ns/op)
erase time: 353286375 ns (avg 117 ns/op)
lookup time (after erase): 391374875 ns (avg 39 ns/op)
final size: 7000000

 with inline
insert time: 1556482416 ns (avg 155 ns/op)
lookup time (before erase): 321002958 ns (avg 32 ns/op)
erase time: 304808000 ns (avg 101 ns/op)
lookup time (after erase): 260594542 ns (avg 26 ns/op)
final size: 7000000

 */