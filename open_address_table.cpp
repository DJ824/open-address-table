//
// Created by Devang Jaiswal on 11/2/24.
//
#include <vector>
#include <optional>


struct Entry {
    uint64_t key_;
    uint64_t val_;
    static constexpr uint64_t EMPTY_KEY = std::numeric_limits<uint64_t>::max();
    static constexpr uint64_t TOMBSTONE = std::numeric_limits<uint64_t>::max() - 1;
};

class OpenAddressTable {
    std::vector<Entry> data_;
    size_t size_;
    // keep size at power of 2 to use fast bitwise AND instead of mod, here roughly 2^21
    static constexpr size_t INITIAL_CAPACITY = 2097152;
    static constexpr float MAX_LOAD_FACTOR = 0.9f;
    size_t tombstone_ct_;




public:
    OpenAddressTable() {
        // initially mark all slots as empty
        data_.resize(INITIAL_CAPACITY, {Entry::EMPTY_KEY, 0});
        size_ = 0;
        tombstone_ct_ = 0;
    }

    // insert via linear probing, if spot is full, +1
    __attribute__((always_inline)) bool insert(uint64_t key, uint64_t val) {
        const size_t mask = data_.size() - 1;

        // expect to not have to resize
        if (__builtin_expect(float(size_ + tombstone_ct_) / float(data_.size()) >= MAX_LOAD_FACTOR, 0)) {
            resize();
        }

        // since our capacity is a power of 2, any_number & capacity - 1 is same as any_number % capacity
        size_t pos = (key * 11400714819323198485ULL) & mask;
        size_t first_tombstone = SIZE_MAX;

        // expect to have few collisions
        while (__builtin_expect(data_[pos].key_ != Entry::EMPTY_KEY, 0)) {
            if (data_[pos].key_ == key) {
                data_[pos].val_ = val;
                return false;
            }

            if (data_[pos].key_ == Entry::TOMBSTONE && first_tombstone == SIZE_MAX) {
                first_tombstone = pos;
            }
            pos = (pos + 1) & mask;
        }

        // if we found a deleted entry before an empty entry, reuse the deleted entry
        if (first_tombstone != SIZE_MAX) {
            pos = first_tombstone;
            --tombstone_ct_;
        }

        data_[pos] = {key, val};
        ++size_;
        return true;
    }

    void resize() {
        std::vector<Entry> old_data = std::move(data_);
        data_.resize(old_data.size() * 2, {Entry::EMPTY_KEY, 0});
        size_ = 0;

        for (const auto& entry : old_data) {
            if (entry.key_ != Entry::EMPTY_KEY && entry.key_ != Entry::TOMBSTONE) {
                insert(entry.key_, entry.val_);
            }
        }
    }

    __attribute__((always_inline)) std::optional<uint64_t> get(uint64_t key) const {
        const size_t mask = data_.size() - 1;
        size_t pos = (key * 11400714819323198485ULL) & mask;

        while (__builtin_expect(data_[pos].key_ != Entry::EMPTY_KEY, 1)) {
            if (__builtin_expect(data_[pos].key_ == key, 1)) {
                return data_[pos].val_;
            }

            // skip past deleted entries
            if (__builtin_expect(data_[pos].key_ == Entry::TOMBSTONE, 1)) {
                pos = (pos + 1) & mask;
                continue;
            }
            pos = (pos + 1) & mask;
        }
        return std::nullopt;
    }

    __attribute__((always_inline)) bool erase(uint64_t key) {
        const size_t mask = data_.size() - 1;
        size_t pos = (key * 11400714819323198485ULL) & mask;

        while (__builtin_expect(data_[pos].key_ != Entry::EMPTY_KEY, 1)) {
            if (__builtin_expect(data_[pos].key_ == key, 1)) {
                data_[pos].key_ = Entry::TOMBSTONE;
                --size_;
                ++tombstone_ct_;
                return true;
            }
            pos = (pos + 1) & mask;
        }
        return false;
    }

    size_t size() const {
        return size_;
    }

    size_t capacity() const {
        return data_.size();
    }

    void clear() {
        data_.clear();
        data_.resize(INITIAL_CAPACITY, {Entry::EMPTY_KEY, 0});
        size_ = 0;
        tombstone_ct_ = 0;
    }
};
