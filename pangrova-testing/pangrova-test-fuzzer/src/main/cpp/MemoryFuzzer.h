#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates memory-intensive queries to stress-test spill-to-disk
 * correctness. Produces large joins, sorts, and aggregations that
 * exceed typical memory budgets, verifying results match the
 * reference engine regardless of memory pressure.
 */
class MemoryFuzzer {
public:
    explicit MemoryFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);

private:
    std::string generateLargeSort();
    std::string generateLargeJoin();
    std::string generateLargeAggregate();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
};

} // namespace pangrova::testing
