#include "WriteFuzzer.h"

#include <array>

namespace pangrova::testing {

WriteFuzzer::WriteFuzzer(ReferenceEngine& reference, uint64_t seed)
    : reference_(reference), seed_(seed == 0 ? 257 : seed) {}

uint64_t WriteFuzzer::nextRandom() {
    seed_ ^= seed_ << 13;
    seed_ ^= seed_ >> 7;
    seed_ ^= seed_ << 17;
    return seed_;
}

int WriteFuzzer::randomInt(int min, int max) {
    return min + static_cast<int>(nextRandom() % (max - min + 1));
}

WriteFuzzer::FuzzResult WriteFuzzer::run(int iterations) {
    FuzzResult result{iterations, 0, 0, {}};
    for (int i = 0; i < iterations; ++i) {
        // Reset state each iteration to avoid accumulating side effects
        reference_.reset();
        int stmtType = randomInt(0, 2);
        std::string sql;
        if (stmtType == 0) {
            sql = generateInsert();
        } else if (stmtType == 1) {
            sql = generateUpdate();
        } else {
            sql = generateDelete();
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

std::string WriteFuzzer::generateInsert() {
    int variant = randomInt(0, 2);
    if (variant == 0) {
        // Single row INSERT with random values
        return "INSERT INTO t1 (id, val, name, flag) VALUES ("
            + generateRandomValue(0) + ", "
            + generateRandomValue(1) + ", "
            + generateRandomValue(2) + ", "
            + generateRandomValue(3) + ")";
    }
    if (variant == 1) {
        // Multi-row INSERT
        std::string sql = "INSERT INTO t1 (id, val, name, flag) VALUES ";
        int numRows = randomInt(2, 4);
        for (int i = 0; i < numRows; ++i) {
            if (i > 0) sql += ", ";
            sql += "(" + generateRandomValue(0) + ", "
                + generateRandomValue(1) + ", "
                + generateRandomValue(2) + ", "
                + generateRandomValue(3) + ")";
        }
        return sql;
    }
    // INSERT ... SELECT
    return "INSERT INTO t3 (id, score, label) "
           "SELECT id, val, name FROM t1 WHERE id IS NOT NULL";
}

std::string WriteFuzzer::generateUpdate() {
    int variant = randomInt(0, 2);
    if (variant == 0) {
        return "UPDATE t1 SET val = " + generateRandomValue(1)
            + generateWhereClause();
    }
    if (variant == 1) {
        return "UPDATE t1 SET name = " + generateRandomValue(2)
            + ", flag = " + generateRandomValue(3)
            + generateWhereClause();
    }
    // UPDATE with expression
    return "UPDATE t1 SET val = val + 1" + generateWhereClause();
}

std::string WriteFuzzer::generateDelete() {
    int variant = randomInt(0, 2);
    if (variant == 0) {
        return "DELETE FROM t1" + generateWhereClause();
    }
    if (variant == 1) {
        return "DELETE FROM t2 WHERE id IN (SELECT id FROM t1 WHERE flag = true)";
    }
    return "DELETE FROM t3 WHERE score < " + std::to_string(randomInt(0, 100));
}

std::string WriteFuzzer::generateRandomValue(int colType) {
    // 20% chance of NULL for any column
    if (randomInt(0, 4) == 0) {
        return "NULL";
    }
    switch (colType) {
        case 0: // INTEGER
            return std::to_string(randomInt(-50, 200));
        case 1: // DOUBLE
            return std::to_string(randomInt(-100, 100)) + "."
                + std::to_string(randomInt(0, 99));
        case 2: // VARCHAR
        {
            static constexpr std::array<const char*, 6> strings = {
                "'alpha'", "'beta'", "'gamma'", "'delta'", "'epsilon'", "'zeta'"
            };
            return strings[randomInt(0, 5)];
        }
        case 3: // BOOLEAN
            return (randomInt(0, 1) == 0) ? "true" : "false";
        default:
            return "NULL";
    }
}

std::string WriteFuzzer::generateWhereClause() {
    int choice = randomInt(0, 4);
    switch (choice) {
        case 0: return " WHERE id = " + std::to_string(randomInt(1, 5));
        case 1: return " WHERE id > " + std::to_string(randomInt(0, 3));
        case 2: return " WHERE name IS NOT NULL";
        case 3: return " WHERE flag = true AND id < 4";
        default: return "";
    }
}

} // namespace pangrova::testing
