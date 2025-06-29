#include <algorithm>
#include <cassert>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

struct PerfProfiler {
  static void mark_start() {}
  static void mark_end() {}
};

#include "table.h"

using namespace std::chrono;

struct Order {
  uint64_t id_;
  uint32_t price_;
  uint32_t size_;
  uint64_t unix_time_;
  bool side_;

  Order() = default;
  Order(uint64_t id, uint32_t p, uint32_t s, uint64_t t, bool side)
      : id_(id), price_(p), size_(s), unix_time_(t), side_(side) {}

  bool operator==(const Order &o) const {
    return id_ == o.id_ && price_ == o.price_ && size_ == o.size_ &&
           unix_time_ == o.unix_time_ && side_ == o.side_;
  }
};

template <> struct std::hash<Order> {
  size_t operator()(const Order &o) const noexcept {
    return std::hash<uint64_t>{}(o.id_);
  }
};

struct BenchmarkConfig {
  size_t num_entries = 1'000'000;
  size_t num_operations = 10'000'000;
  uint32_t seed = 42;
  bool verbose = false;
  bool run_std_map = true;
};

struct BenchmarkResult {
  std::string name;
  double mean_ns_per_op;
  double max_ns_per_op;
  size_t final_size;
  std::chrono::duration<double> total_time;
  double throughput_ops_per_sec;
};

class OrderGenerator {
  std::minstd_rand rng_;
  std::uniform_int_distribution<uint64_t> id_{1, UINT64_C(1) << 63};
  std::uniform_int_distribution<uint32_t> price_{100, 50'000};
  std::uniform_int_distribution<uint32_t> size_{1, 10'000};
  std::uniform_int_distribution<uint64_t> ts_{1'600'000'000, 1'700'000'000};
  std::bernoulli_distribution side_{0.5};

public:
  explicit OrderGenerator(uint32_t seed) : rng_(seed) {}

  Order next_order() {
    return Order{id_(rng_), price_(rng_), size_(rng_), ts_(rng_), side_(rng_)};
  }

  uint64_t next_id() { return id_(rng_); }

  std::vector<uint64_t> make_keys(size_t n) {
    std::vector<uint64_t> v(n);
    for (auto &k : v)
      k = next_id();
    return v;
  }

  std::vector<Order> make_orders(size_t n) {
    std::vector<Order> v(n);
    for (auto &o : v)
      o = next_order();
    return v;
  }
};

template <class HashTable> class HashTableBenchmark {
  const BenchmarkConfig &cfg_;

  template <class Table> void populate(Table &table, OrderGenerator &gen) {
    for (size_t i = 0; i < cfg_.num_entries; ++i) {
      auto ord = gen.next_order();
      if constexpr (std::is_same_v<Table,
                                   std::unordered_map<uint64_t, Order>>) {
        table.emplace(ord.id_, ord);
      } else {
        table.insert(ord.id_, ord);
      }
    }
  }

  template <typename T> static size_t table_size(const T &t) {
    return t.size();
  }

public:
  explicit HashTableBenchmark(const BenchmarkConfig &c) : cfg_(c) {}

  BenchmarkResult run(const std::string &name,
                      const std::vector<uint64_t> &keys,
                      const std::vector<Order> &orders) {
    OrderGenerator gen(cfg_.seed + 12345);
    HashTable table;
    populate(table, gen);

    if (cfg_.verbose)
      std::cerr << '[' << name << "] start benchmark with " << table.size()
                << " pre‑filled entries\n";

    PerfProfiler::mark_start();
    auto t0 = steady_clock::now();

    for (size_t i = 0; i < cfg_.num_operations; ++i) {
      uint64_t k = keys[i];
      if constexpr (std::is_same_v<HashTable,
                                   std::unordered_map<uint64_t, Order>>) {
        auto it = table.find(k);
        if (it != table.end())
          table.erase(it);
        else
          table.emplace(k, orders[i]);
      } else {
        auto hit = table.get(k);
        if (hit)
          table.erase(k);
        else
          table.insert(k, orders[i]);
      }
    }

    auto t1 = steady_clock::now();
    PerfProfiler::mark_end();

    double total_ns = duration_cast<nanoseconds>(t1 - t0).count();
    double mean_ns_op = total_ns / cfg_.num_operations;
    double thput = cfg_.num_operations / duration<double>(t1 - t0).count();

    return {name, mean_ns_op, 0.0, table_size(table), t1 - t0, thput};
  }
};

void print_header() {
  std::cout << std::left << std::setw(22) << "Implementation" << std::setw(15)
            << "Mean (ns/op)" << std::setw(15) << "Throughput" << std::setw(12)
            << "Final Sz" << std::setw(10) << "Time (s)" << '\n'
            << std::string(74, '-') << '\n';
}

void print_one(const BenchmarkResult &r) {
  std::cout << std::left << std::setw(22) << r.name << std::setw(15)
            << static_cast<uint64_t>(r.mean_ns_per_op) << std::setw(15)
            << static_cast<uint64_t>(r.throughput_ops_per_sec) << std::setw(12)
            << r.final_size << std::setw(10) << std::fixed
            << std::setprecision(2) << r.total_time.count() << '\n';
}

int main(int argc, char *argv[]) {
  BenchmarkConfig cfg;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "-c" && i + 1 < argc)
      cfg.num_entries = std::stoull(argv[++i]);
    else if (a == "-i" && i + 1 < argc)
      cfg.num_operations = std::stoull(argv[++i]);
    else if (a == "-s" && i + 1 < argc)
      cfg.seed = std::stoul(argv[++i]);
    else if (a == "-v")
      cfg.verbose = true;
    else if (a == "--custom-only")
      cfg.run_std_map = false;
    else if (a == "-h" || a == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]\n"
                << " -c N   pre‑fill entries   (default 1M)\n"
                << " -i N   benchmark ops      (default 10M)\n"
                << " -s N   RNG seed           (default 42)\n"
                << " -v     verbose\n"
                << " --custom-only  skip std::unordered_map\n";
      return 0;
    }
  }

  OrderGenerator gen(cfg.seed);
  auto keys = gen.make_keys(cfg.num_operations);
  auto orders = gen.make_orders(cfg.num_operations);
  for (size_t i = 0; i < cfg.num_operations; ++i)
    orders[i].id_ = keys[i];

  std::vector<BenchmarkResult> results;

  {
    HashTableBenchmark<OpenAddressTable<uint64_t, Order>> bench(cfg);
    results.emplace_back(bench.run("OpenAddressTable", keys, orders));
  }

  if (cfg.run_std_map) {
    HashTableBenchmark<std::unordered_map<uint64_t, Order>> bench(cfg);
    results.emplace_back(bench.run("std::unordered_map", keys, orders));
  }

  print_header();
  for (const auto &r : results)
    print_one(r);

  return 0;
}
