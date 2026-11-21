#include "AggregationFuzzer.h"

#include <array>

namespace pangrova::testing {

AggregationFuzzer::AggregationFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 199 : seed) {}

uint64_t AggregationFuzzer::nextRandom() {
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int AggregationFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

AggregationFuzzer::FuzzResult AggregationFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        std::string sql = generateAggregationQuery();
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

std::string AggregationFuzzer::generateAggregationQuery() {
    int queryType = randomInt(0, 2);
    switch (queryType) {
        case 0: return generateGlobalAggregate();
        case 1: return generateGroupByAggregate();
        default: return generateDistinctAggregate();
    }
}

std::string AggregationFuzzer::generateGlobalAggregate() {
    static constexpr std::array<const char*, 8> aggs = {
        "COUNT(*)", "SUM(val)", "AVG(val)", "MIN(id)", "MAX(val)",
        "COUNT(name)", "SUM(CASE WHEN flag THEN 1 ELSE 0 END)",
        "COUNT(*) FILTER (WHERE id > 2)"
    };
    int numAggs = randomInt(1, 4);
    std::string sql = "SELECT ";
    for (int i = 0; i < numAggs; ++i) {
        if (i > 0) sql += ", ";
        sql += aggs[randomInt(0, 7)];
    }
    sql += " FROM t1";
    if (randomInt(0, 1) == 0) {
        sql += " WHERE id IS NOT NULL";
    }
    return sql;
}

std::string AggregationFuzzer::generateGroupByAggregate() {
    static constexpr std::array<const char*, 5> aggs = {
        "COUNT(*)", "SUM(val)", "AVG(val)", "MIN(id)", "MAX(val)"
    };
    static constexpr std::array<const char*, 4> groupKeys = {
        "flag", "name", "flag, name IS NOT NULL", "id % 2"
    };
    std::string groupKey = groupKeys[randomInt(0, 3)];
    int numAggs = randomInt(1, 3);
    std::string sql = "SELECT " + std::string(groupKey);
    for (int i = 0; i < numAggs; ++i) {
        sql += ", " + std::string(aggs[randomInt(0, 4)]);
    }
    sql += " FROM t1 GROUP BY " + std::string(groupKey);
    if (randomInt(0, 2) == 0) {
        sql += " HAVING COUNT(*) >= " + std::to_string(randomInt(1, 3));
    }
    return sql;
}

std::string AggregationFuzzer::generateDistinctAggregate() {
    static constexpr std::array<const char*, 3> distinctAggs = {
        "COUNT(DISTINCT name)", "COUNT(DISTINCT flag)", "COUNT(DISTINCT id)"
    };
    std::string sql = "SELECT " + std::string(distinctAggs[randomInt(0, 2)]);
    if (randomInt(0, 1) == 0) {
        sql += ", COUNT(*)";
    }
    sql += " FROM t1";
    if (randomInt(0, 1) == 0) {
        sql += " WHERE val IS NOT NULL";
    }
    return sql;
}

} // namespace pangrova::testing
