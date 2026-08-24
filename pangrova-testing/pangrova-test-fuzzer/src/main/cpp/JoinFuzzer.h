#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates random SELECT queries with joins and aggregations.
 * Covers INNER/LEFT/RIGHT/FULL/CROSS joins, GROUP BY with aggregate
 * functions, HAVING clauses, ORDER BY, and LIMIT.
 */
class JoinFuzzer {
public:
    explicit JoinFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);
    std::string generateJoinQuery();

private:
    std::string generateJoinClause();
    std::string generateAggregateSelect();
    std::string generateWhereClause();
    std::string generateOrderBy();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
};

} // namespace pangrova::testing
