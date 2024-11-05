//
// Created by Devang Jaiswal on 11/2/24.
//
#include <vector>
#include <optional>
#include "xxhash/xxhash.h"

struct Entry {
    uint64_t key_;
    uint64_t val_;
    uint8_t status_;      // 0 for empty, 1 for tombstone, 2 for occupied
    uint8_t probe_dist_;
    //uint8_t padding_[14];
} __attribute__((packed, aligned(16)));

class OpenAddressTable {
public:
    // ensure the vector stats at the 64 byte cache line boundary
    alignas(64) std::vector<Entry> data_;
    size_t size_;
    size_t tombstone_ct_;

    static constexpr size_t CACHE_LINE_SIZE = 64;
    static constexpr size_t ENTRIES_PER_CACHE_LINE = 2;
    static constexpr double LOAD_FACTOR_THRESHOLD = 0.5;

    explicit OpenAddressTable(size_t initial_size = 16)
            : data_(initial_size), size_(0), tombstone_ct_(0) {
        std::fill(data_.begin(), data_.end(), Entry{0, 0, 0, 0});
    }

    static size_t hash_key(uint64_t key) {
        return XXH64(&key, sizeof(key), 0);
    }

    // prefetch entries 2 at a time
    // probes will hit in cache, if we miss, we utilize the entire cache line
    __attribute__((always_inline))
    size_t next_probe_position(size_t current_pos) {
        size_t next_pos = (current_pos + 1) & (data_.size() - 1);

        if (next_pos % ENTRIES_PER_CACHE_LINE == 0 && next_pos + ENTRIES_PER_CACHE_LINE < data_.size()) {
            __builtin_prefetch(&data_[next_pos + ENTRIES_PER_CACHE_LINE]);
        }

        return next_pos;
    }

    __attribute__((always_inline))
    void resize() {
        if (data_.empty()) {
            data_.resize(16);
            return;
        }

        std::vector<Entry> old_data = std::move(data_);
        data_.resize(old_data.size() * 2);
        std::fill(data_.begin(), data_.end(), Entry{0, 0, 0, 0}); // Initialize new entries
        size_ = 0;
        tombstone_ct_ = 0;

        // reinsert the data for entires that are only occupied, prefetching by cache line
        for (size_t i = 0; i < old_data.size(); i += ENTRIES_PER_CACHE_LINE) {
            // safety check out of bounds
            __builtin_prefetch(&old_data[std::min(i + ENTRIES_PER_CACHE_LINE,
                                                  old_data.size())]);

            for (size_t j = 0; j < ENTRIES_PER_CACHE_LINE && i + j < old_data.size(); ++j) {
                const auto &entry = old_data[i + j];
                if (entry.status_ == 2) {
                    insert(entry.key_, entry.val_);
                }
            }
        }
    }

    __attribute__((always_inline))
    bool insert(uint64_t key, uint64_t val) {
        if (load_factor() >= LOAD_FACTOR_THRESHOLD) {
            resize();
        }
        const size_t mask = data_.size() - 1;
        size_t pos = hash_key(key) & mask;
        size_t probe_dist = 0;

        Entry entry{key, val, 2, static_cast<uint8_t>(probe_dist)};

        while (true) {
            if ((pos % ENTRIES_PER_CACHE_LINE) == 0) {
                __builtin_prefetch(&data_[pos + ENTRIES_PER_CACHE_LINE]);
            }

            if (data_[pos].status_ == 0) {
                data_[pos] = entry;
                ++size_;
                return true;
            }

            if (data_[pos].status_ == 2 && data_[pos].key_ == key) {
                data_[pos].val_ = val;
                return true;
            }

            // if we have to probe further than the current probe dist from hash origin, swap to minimize variance
            if (probe_dist > data_[pos].probe_dist_) {
                std::swap(entry, data_[pos]);
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
