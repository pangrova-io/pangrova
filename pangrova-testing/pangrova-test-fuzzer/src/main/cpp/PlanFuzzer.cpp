#include "PlanFuzzer.h"

#include <array>

namespace pangrova::testing {

PlanFuzzer::PlanFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 389 : seed) {}

uint64_t PlanFuzzer::nextRandom() {
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int PlanFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

PlanFuzzer::FuzzResult PlanFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        std::string sql = generatePlan();
        auto refResult = reference_.execute(sql);
        if (refResult.success) {
            ++result.passed;
        } else {
            ++result.failed;
            result.failures.push_back(
                "plan=[" + sql + "] error=[" + refResult.errorMessage + "]");
        }
    }
    return result;
}

std::string PlanFuzzer::generatePlan() {
    int planType = randomInt(0, 4);
    switch (planType) {
        case 0: return generateScanFilter();
        case 1: return generateProjection();
        case 2: return generateAggregatePlan();
        case 3: return generateJoinPlan();
        default: return generateSortLimitPlan();
    }
}

std::string PlanFuzzer::generateScanFilter() {
    static constexpr std::array<const char*, 3> tables = {"t1", "t2", "t3"};
    std::string table = tables[randomInt(0, 2)];
    std::string sql = "SELECT * FROM " + std::string(table);
    int filterType = randomInt(0, 4);
    switch (filterType) {
        case 0: sql += " WHERE id > " + std::to_string(randomInt(0, 5)); break;
        case 1: sql += " WHERE id IS NOT NULL"; break;
        case 2: sql += " WHERE id BETWEEN 1 AND 3"; break;
        case 3: sql += " WHERE id IN (1, 2, 3)"; break;
        default: break; // no filter
    }
    return sql + " LIMIT " + std::to_string(randomInt(1, 20));
}

std::string PlanFuzzer::generateProjection() {
    static constexpr std::array<const char*, 6> exprs = {
        "id * 2 AS doubled",
        "COALESCE(val, 0) AS safe_val",
        "CASE WHEN id > 2 THEN 'high' ELSE 'low' END AS tier",
        "id + COALESCE(val, 0) AS combined",
        "LENGTH(COALESCE(name, '')) AS name_len",
        "ABS(val) AS abs_val"
    };
    int numCols = randomInt(1, 4);
    std::string select = "SELECT id";
    for (int i = 0; i < numCols; ++i) {
        select += ", " + std::string(exprs[randomInt(0, 5)]);
    }
    return select + " FROM t1 WHERE id IS NOT NULL";
}

std::string PlanFuzzer::generateAggregatePlan() {
    static constexpr std::array<const char*, 5> aggs = {
        "COUNT(*)", "SUM(val)", "AVG(val)", "MIN(id)", "MAX(val)"
    };
    static constexpr std::array<const char*, 3> groupExprs = {
        "flag", "name IS NOT NULL", "id > 2"
    };
    int numAggs = randomInt(1, 3);
    std::string sql = "SELECT ";
    bool hasGroupBy = (randomInt(0, 1) == 0);
    if (hasGroupBy) {
        std::string groupExpr = groupExprs[randomInt(0, 2)];
        sql += groupExpr;
        for (int i = 0; i < numAggs; ++i) {
            sql += ", " + std::string(aggs[randomInt(0, 4)]);
        }
        sql += " FROM t1 GROUP BY " + groupExpr;
    } else {
        // Global aggregate
        for (int i = 0; i < numAggs; ++i) {
            if (i > 0) sql += ", ";
            sql += aggs[randomInt(0, 4)];
        }
        sql += " FROM t1";
    }
    return sql;
}

std::string PlanFuzzer::generateJoinPlan() {
    static constexpr std::array<const char*, 4> joinTypes = {
        "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"
    };
    std::string join = joinTypes[randomInt(0, 3)];
    int complexity = randomInt(0, 2);
    if (complexity == 0) {
        return "SELECT t1.id, t1.val, t2.amount FROM t1 "
            + std::string(join) + " t2 ON t1.id = t2.id LIMIT 10";
    }
    if (complexity == 1) {
        return "SELECT t1.id, t2.amount, t3.score FROM t1 "
            + std::string(join) + " t2 ON t1.id = t2.id "
            "INNER JOIN t3 ON t1.id = t3.id LIMIT 10";
    }
    return "SELECT t1.flag, COUNT(*), SUM(t2.amount) FROM t1 "
        + std::string(join) + " t2 ON t1.id = t2.id "
        "GROUP BY t1.flag";
}

std::string PlanFuzzer::generateSortLimitPlan() {
    static constexpr std::array<const char*, 4> orderCols = {
        "id", "val", "name", "id DESC"
    };
    std::string sql = "SELECT * FROM t1";
    if (randomInt(0, 1) == 0) {
        sql += " WHERE id IS NOT NULL";
    }
    sql += " ORDER BY " + std::string(orderCols[randomInt(0, 3)]);
    if (randomInt(0, 1) == 0) {
        sql += " NULLS LAST";
    }
    sql += " LIMIT " + std::to_string(randomInt(1, 10));
    if (randomInt(0, 2) == 0) {
        sql += " OFFSET " + std::to_string(randomInt(0, 3));
    }
    return sql;
}

std::string PlanFuzzer::generateSubquery() {
    return "SELECT * FROM (SELECT id, val * 2 AS doubled FROM t1 WHERE id > 1) sub "
           "WHERE doubled > 0 LIMIT 5";
}

} // namespace pangrova::testing
