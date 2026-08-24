#include "ReferenceEngine.h"

#include <duckdb.hpp>
#include <stdexcept>

namespace pangrova::testing {

struct ReferenceEngine::Impl {
    duckdb::DuckDB db;
    duckdb::Connection conn;

    Impl() : db(nullptr), conn(db) {}
};

namespace {

void setupFuzzerSchema(duckdb::Connection& conn) {
    conn.Query("CREATE SCHEMA IF NOT EXISTS fuzzer");
    conn.Query(R"(
        CREATE TABLE fuzzer.t1 (
            id INTEGER, val DOUBLE, name VARCHAR, flag BOOLEAN,
            ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    )");
    conn.Query(R"(
        CREATE TABLE fuzzer.t2 (
            id INTEGER, amount DOUBLE, category VARCHAR, active BOOLEAN
        )
    )");
    conn.Query(R"(
        CREATE TABLE fuzzer.t3 (
            id INTEGER, score DOUBLE, label VARCHAR
        )
    )");
    conn.Query(R"(
        INSERT INTO fuzzer.t1 VALUES
            (1, 3.14, 'alpha', true, '2024-01-01 00:00:00'),
            (2, -1.5, 'beta', false, '2024-06-15 12:30:00'),
            (3, 0.0, NULL, true, '2024-12-31 23:59:59'),
            (4, 999.99, 'delta', NULL, NULL),
            (NULL, NULL, 'epsilon', false, '2024-03-01 08:00:00')
    )");
    conn.Query(R"(
        INSERT INTO fuzzer.t2 VALUES
            (1, 100.0, 'A', true),
            (2, 200.5, 'B', false),
            (3, NULL, 'A', true),
            (5, 50.0, NULL, false),
            (NULL, 75.0, 'C', NULL)
    )");
    conn.Query(R"(
        INSERT INTO fuzzer.t3 VALUES
            (1, 85.0, 'pass'),
            (2, 42.0, 'fail'),
            (3, 91.5, 'pass'),
            (4, NULL, NULL)
    )");
    conn.Query("SET search_path TO 'fuzzer'");
}

} // namespace

ReferenceEngine::ReferenceEngine() : impl_(std::make_unique<Impl>()) {
    setupFuzzerSchema(impl_->conn);
}

ReferenceEngine::~ReferenceEngine() = default;

void ReferenceEngine::executeSetup(const std::string& sql) {
    auto result = impl_->conn.Query(sql);
    if (result->HasError()) {
        throw std::runtime_error("Setup SQL failed: " + result->GetError());
    }
}

ReferenceEngine::QueryResult ReferenceEngine::execute(const std::string& sql) {
    QueryResult qr;
    auto result = impl_->conn.Query(sql);
    if (result->HasError()) {
        qr.success = false;
        qr.errorMessage = result->GetError();
        return qr;
    }
    qr.success = true;
    for (duckdb::idx_t col = 0; col < result->ColumnCount(); ++col) {
        qr.columnNames.push_back(result->ColumnName(col));
    }
    for (duckdb::idx_t row = 0; row < result->RowCount(); ++row) {
        std::vector<std::string> rowData;
        for (duckdb::idx_t col = 0; col < result->ColumnCount(); ++col) {
            auto val = result->GetValue(col, row);
            rowData.push_back(val.IsNull() ? "NULL" : val.ToString());
        }
        qr.rows.push_back(std::move(rowData));
    }
    return qr;
}

void ReferenceEngine::reset() {
    impl_->conn.Query("DROP SCHEMA IF EXISTS fuzzer CASCADE");
    impl_ = std::make_unique<Impl>();
    setupFuzzerSchema(impl_->conn);
}

} // namespace pangrova::testing
