#pragma once

#include "ReferenceEngine.h"
#include <cstdint>
#include <string>
#include <vector>

namespace pangrova::testing {

/**
 * Generates random but type-valid expression trees and compares evaluation
 * against ReferenceEngine (DuckDB). Produces arithmetic, comparison,
 * logical, string, and NULL-handling expressions with configurable depth.
 */
class ExpressionFuzzer {
public:
    explicit ExpressionFuzzer(ReferenceEngine& reference, uint64_t seed = 0);

    struct FuzzResult {
        int totalTests;
        int passed;
        int failed;
        std::vector<std::string> failures;
    };

    FuzzResult run(int iterations);
    std::string generateExpression();

private:
    enum class ExprType {
        ARITHMETIC,
        COMPARISON,
        LOGICAL,
        STRING_FUNC,
        NULL_HANDLING,
        CAST,
        CASE_WHEN
    };

    std::string generateArithmetic(int depth);
    std::string generateComparison(int depth);
    std::string generateLogical(int depth);
    std::string generateStringFunc();
    std::string generateNullHandling(int depth);
    std::string generateCast();
    std::string generateCaseWhen(int depth);
    std::string generateLeaf();
    std::string generateNumericLeaf();
    std::string generateStringLeaf();

    uint64_t nextRandom();
    int randomInt(int min, int max);

    ReferenceEngine& reference_;
    uint64_t seed_;
    static constexpr int kMaxDepth = 3;
};

} // namespace pangrova::testing
