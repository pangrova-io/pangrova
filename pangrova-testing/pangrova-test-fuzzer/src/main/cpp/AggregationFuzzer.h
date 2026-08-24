#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates random aggregation queries covering global aggregates,
 * GROUP BY with multiple keys, HAVING clauses, DISTINCT aggregates,
 * and FILTER clauses.
 */
class AggregationFuzzer {
public:
    explicit AggregationFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);
    std::string generateAggregationQuery();

private:
    std::string generateGlobalAggregate();
    std::string generateGroupByAggregate();
    std::string generateDistinctAggregate();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
};

} // namespace pangrova::testing
