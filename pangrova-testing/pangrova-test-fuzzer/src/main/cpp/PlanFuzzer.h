#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates random Substrait-style query plans as SQL and validates
 * execution against the reference engine. Covers scan, filter, project,
 * aggregate, join, sort, and limit plan nodes in random combinations.
 */
class PlanFuzzer {
public:
    explicit PlanFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);

private:
    std::string generatePlan();
    std::string generateScanFilter();
    std::string generateProjection();
    std::string generateAggregatePlan();
    std::string generateJoinPlan();
    std::string generateSortLimitPlan();
    std::string generateSubquery();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
};

} // namespace pangrova::testing
