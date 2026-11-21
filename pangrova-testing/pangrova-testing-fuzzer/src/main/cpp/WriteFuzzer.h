#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates random INSERT, UPDATE, and DELETE statements.
 * Validates that DML operations execute without crashes against
 * the reference engine, covering value types, NULL handling,
 * subqueries, and multi-row operations.
 */
class WriteFuzzer {
public:
    explicit WriteFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);

private:
    std::string generateInsert();
    std::string generateUpdate();
    std::string generateDelete();
    std::string generateRandomValue(int colType);
    std::string generateWhereClause();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
};

} // namespace pangrova::testing
