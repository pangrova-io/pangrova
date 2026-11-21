#pragma once

#include <string>
#include <vector>
#include <memory>

namespace pangrova::testing {

/**
 * DuckDB wrapper for query result comparison in fuzzer tests.
 * Executes SQL against an in-memory DuckDB instance and returns structured results.
 */
class ReferenceEngine {
public:
    ReferenceEngine();
    ~ReferenceEngine();

    /**
     * Execute DDL/DML setup statements (CREATE TABLE, INSERT, etc.).
     * Throws on failure.
     */
    void executeSetup(const std::string& sql);

    struct QueryResult {
        std::vector<std::string> columnNames;
        std::vector<std::vector<std::string>> rows;
        bool success;
        std::string errorMessage;
    };

    /**
     * Execute a query and return structured results.
     * On SQL error, returns success=false with errorMessage populated.
     */
    QueryResult execute(const std::string& sql);

    /**
     * Reset the database state (drop all tables, re-create fuzzer schema).
     */
    void reset();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace pangrova::testing
