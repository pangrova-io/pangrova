#include "ExpressionFuzzer.h"

#include <array>
#include <sstream>

namespace pangrova::testing {

ExpressionFuzzer::ExpressionFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 42 : seed) {}

uint64_t ExpressionFuzzer::nextRandom() {
    // xorshift64 PRNG for deterministic reproducibility
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int ExpressionFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

ExpressionFuzzer::FuzzResult ExpressionFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        std::string expr = generateExpression();
        std::string sql = "SELECT " + expr + " FROM t1 LIMIT 1";
        auto refResult = reference_.execute(sql);
        if (refResult.success) {
            ++result.passed;
        } else {
            ++result.failed;
            result.failures.push_back(
                "expr=[" + expr + "] error=[" + refResult.errorMessage + "]");
        }
    }
    return result;
}

std::string ExpressionFuzzer::generateExpression() {
    auto type = static_cast<ExprType>(randomInt(0, 6));
    switch (type) {
        case ExprType::ARITHMETIC:
            return generateArithmetic(0);
        case ExprType::COMPARISON:
            return generateComparison(0);
        case ExprType::LOGICAL:
            return generateLogical(0);
        case ExprType::STRING_FUNC:
            return generateStringFunc();
        case ExprType::NULL_HANDLING:
            return generateNullHandling(0);
        case ExprType::CAST:
            return generateCast();
        case ExprType::CASE_WHEN:
            return generateCaseWhen(0);
    }
    return generateArithmetic(0);
}

std::string ExpressionFuzzer::generateArithmetic(int depth) {
    if (depth >= kMaxDepth) {
        return generateNumericLeaf();
    }
    static constexpr std::array<const char*, 5> ops = {"+", "-", "*", "/", "%"};
    std::string left = (randomInt(0, 1) == 0)
        ? generateNumericLeaf()
        : generateArithmetic(depth + 1);
    std::string right = generateNumericLeaf();
    // Avoid division by zero with NULLIF
    const char* op = ops[randomInt(0, 4)];
    if (op[0] == '/' || op[0] == '%') {
        return "(" + left + " " + op + " NULLIF(" + right + ", 0))";
    }
    return "(" + left + " " + op + " " + right + ")";
}

std::string ExpressionFuzzer::generateComparison(int depth) {
    static constexpr std::array<const char*, 6> ops =
        {"=", "!=", "<", ">", "<=", ">="};
    std::string left = (depth < kMaxDepth && randomInt(0, 2) == 0)
        ? generateArithmetic(depth + 1)
        : generateNumericLeaf();
    std::string right = generateNumericLeaf();
    return "(" + left + " " + ops[randomInt(0, 5)] + " " + right + ")";
}

std::string ExpressionFuzzer::generateLogical(int depth) {
    if (depth >= kMaxDepth) {
        return generateComparison(kMaxDepth);
    }
    int choice = randomInt(0, 3);
    if (choice == 0) {
        return "NOT " + generateComparison(depth + 1);
    }
    std::string left = generateComparison(depth + 1);
    std::string right = generateComparison(depth + 1);
    if (choice == 1) return "(" + left + " AND " + right + ")";
    if (choice == 2) return "(" + left + " OR " + right + ")";
    return "(" + left + " IS NOT DISTINCT FROM " + right + ")";
}

std::string ExpressionFuzzer::generateStringFunc() {
    int choice = randomInt(0, 7);
    std::string col = generateStringLeaf();
    switch (choice) {
        case 0: return "LENGTH(" + col + ")";
        case 1: return "UPPER(" + col + ")";
        case 2: return "LOWER(" + col + ")";
        case 3: return "TRIM(" + col + ")";
        case 4: return "SUBSTRING(" + col + ", 1, 3)";
        case 5: return "REPLACE(" + col + ", 'a', 'x')";
        case 6: return "CONCAT(" + col + ", '_suffix')";
        default: return "REVERSE(" + col + ")";
    }
}

std::string ExpressionFuzzer::generateNullHandling(int depth) {
    int choice = randomInt(0, 3);
    std::string leaf = generateLeaf();
    switch (choice) {
        case 0: return "COALESCE(" + leaf + ", " + generateLeaf() + ")";
        case 1: return leaf + " IS NULL";
        case 2: return leaf + " IS NOT NULL";
        default:
            return "NULLIF(" + generateNumericLeaf() + ", "
                + generateNumericLeaf() + ")";
    }
}

std::string ExpressionFuzzer::generateCast() {
    static constexpr std::array<const char*, 4> types =
        {"INTEGER", "DOUBLE", "VARCHAR", "BOOLEAN"};
    std::string leaf = generateLeaf();
    return "TRY_CAST(" + leaf + " AS " + types[randomInt(0, 3)] + ")";
}

std::string ExpressionFuzzer::generateCaseWhen(int depth) {
    std::string cond = (depth < kMaxDepth)
        ? generateComparison(depth + 1)
        : "(id > 2)";
    std::string thenExpr = generateNumericLeaf();
    std::string elseExpr = generateNumericLeaf();
    return "CASE WHEN " + cond + " THEN " + thenExpr + " ELSE " + elseExpr
        + " END";
}

std::string ExpressionFuzzer::generateLeaf() {
    return (randomInt(0, 1) == 0) ? generateNumericLeaf() : generateStringLeaf();
}

std::string ExpressionFuzzer::generateNumericLeaf() {
    int choice = randomInt(0, 5);
    switch (choice) {
        case 0: return "id";
        case 1: return "val";
        case 2: return std::to_string(randomInt(-100, 100));
        case 3: return std::to_string(randomInt(0, 99)) + "." +
                       std::to_string(randomInt(1, 99));
        case 4: return "ABS(val)";
        default: return "COALESCE(id, 0)";
    }
}

std::string ExpressionFuzzer::generateStringLeaf() {
    int choice = randomInt(0, 3);
    switch (choice) {
        case 0: return "name";
        case 1: return "'" + std::string(1, 'a' + randomInt(0, 25)) + "'";
        case 2: return "COALESCE(name, 'default')";
        default: return "CAST(id AS VARCHAR)";
    }
}

} // namespace pangrova::testing
