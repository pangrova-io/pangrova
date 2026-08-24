#include "MemoryFuzzer.h"

namespace pangrova::testing {

MemoryFuzzer::MemoryFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 521 : seed) {}

uint64_t MemoryFuzzer::nextRandom() {
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int MemoryFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

MemoryFuzzer::FuzzResult MemoryFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        int queryType = randomInt(0, 2);
        std::string sql;
        if (queryType == 0) {
            sql = generateLargeSort();
        } else if (queryType == 1) {
            sql = generateLargeJoin();
        } else {
            sql = generateLargeAggregate();
        }
        auto refResult = reference_.execute(sql);
        if (refResult.success) {
            ++result.passed;
        } else {
            ++result.failed;
            result.failures.push_back(
                "sql=[" + sql + "] error=[" + refResult.errorMessage + "]");
        }
    }
    return result;
}

std::string MemoryFuzzer::generateLargeSort() {
    // Generate a query that produces many rows via CROSS JOIN then sorts
    std::string sql = "SELECT a.id AS a_id, b.id AS b_id, "
        "a.val + COALESCE(b.amount, 0) AS combined "
        "FROM t1 a CROSS JOIN t2 b "
        "ORDER BY combined";
    if (randomInt(0, 1) == 0) {
        sql += " DESC";
    }
    sql += " NULLS LAST LIMIT " + std::to_string(randomInt(5, 25));
    return sql;
}

std::string MemoryFuzzer::generateLargeJoin() {
    // Multi-way join producing cartesian-like result set
    std::string sql = "SELECT t1.id, t2.amount, t3.score, "
        "t1.val * COALESCE(t3.score, 1) AS weighted "
        "FROM t1 "
        "LEFT JOIN t2 ON t1.id = t2.id "
        "LEFT JOIN t3 ON t1.id = t3.id "
        "WHERE t1.id IS NOT NULL "
        "ORDER BY weighted NULLS LAST "
        "LIMIT " + std::to_string(randomInt(10, 50));
    return sql;
}

std::string MemoryFuzzer::generateLargeAggregate() {
    // Aggregate with many groups (cross join creates many combinations)
    std::string sql = "SELECT a.name, b.category, "
        "COUNT(*), SUM(a.val), AVG(b.amount) "
        "FROM t1 a CROSS JOIN t2 b "
        "GROUP BY a.name, b.category "
        "HAVING COUNT(*) > 0";
    return sql;
}

} // namespace pangrova::testing
