#include "JoinFuzzer.h"

#include <array>

namespace pangrova::testing {

JoinFuzzer::JoinFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 137 : seed) {}

uint64_t JoinFuzzer::nextRandom() {
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int JoinFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

JoinFuzzer::FuzzResult JoinFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        std::string sql = generateJoinQuery();
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

std::string JoinFuzzer::generateJoinQuery() {
    int queryType = randomInt(0, 2);
    if (queryType == 0) {
        // Simple join query
        return "SELECT t1.id, t2.amount " + generateJoinClause()
            + generateWhereClause() + generateOrderBy() + " LIMIT 10";
    }
    if (queryType == 1) {
        // Aggregate query with GROUP BY
        return generateAggregateSelect();
    }
    // Multi-table join
    return "SELECT t1.id, t2.amount, t3.score "
        "FROM t1 " + generateJoinClause()
        + " JOIN t3 ON t1.id = t3.id"
        + generateWhereClause() + " LIMIT 10";
}

std::string JoinFuzzer::generateJoinClause() {
    static constexpr std::array<const char*, 5> joinTypes = {
        "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN"
    };
    int joinIdx = randomInt(0, 4);
    std::string join = "FROM t1 " + std::string(joinTypes[joinIdx]) + " t2";
    if (joinIdx == 4) {
        // CROSS JOIN has no ON clause
        return join;
    }
    // Vary join conditions
    int condType = randomInt(0, 2);
    if (condType == 0) {
        return join + " ON t1.id = t2.id";
    }
    if (condType == 1) {
        return join + " ON t1.id = t2.id AND t1.flag = t2.active";
    }
    return join + " ON t1.val > t2.amount";
}

std::string JoinFuzzer::generateAggregateSelect() {
    static constexpr std::array<const char*, 5> aggFuncs = {
        "COUNT(*)", "SUM(t1.val)", "AVG(t2.amount)", "MIN(t1.id)", "MAX(t2.amount)"
    };
    static constexpr std::array<const char*, 3> groupCols = {
        "t1.flag", "t2.category", "t2.active"
    };
    int numAggs = randomInt(1, 3);
    std::string select = "SELECT ";
    std::string groupCol = groupCols[randomInt(0, 2)];
    select += groupCol;
    for (int i = 0; i < numAggs; ++i) {
        select += ", " + std::string(aggFuncs[randomInt(0, 4)]);
    }
    select += " FROM t1 INNER JOIN t2 ON t1.id = t2.id";
    select += " GROUP BY " + groupCol;
    // Optionally add HAVING
    if (randomInt(0, 1) == 0) {
        select += " HAVING COUNT(*) > 0";
    }
    return select;
}

std::string JoinFuzzer::generateWhereClause() {
    if (randomInt(0, 2) == 0) {
        return "";
    }
    int condType = randomInt(0, 4);
    switch (condType) {
        case 0: return " WHERE t1.id > " + std::to_string(randomInt(0, 5));
        case 1: return " WHERE t2.amount IS NOT NULL";
        case 2: return " WHERE t1.flag = true";
        case 3: return " WHERE t1.name IS NOT NULL AND t2.category = 'A'";
        default: return " WHERE t1.id BETWEEN 1 AND 4";
    }
}

std::string JoinFuzzer::generateOrderBy() {
    if (randomInt(0, 2) == 0) {
        return "";
    }
    static constexpr std::array<const char*, 4> cols = {
        "t1.id", "t2.amount", "t1.val", "t1.name"
    };
    std::string order = " ORDER BY " + std::string(cols[randomInt(0, 3)]);
    if (randomInt(0, 1) == 0) {
        order += " DESC";
    }
    if (randomInt(0, 1) == 0) {
        order += " NULLS LAST";
    }
    return order;
}

} // namespace pangrova::testing
