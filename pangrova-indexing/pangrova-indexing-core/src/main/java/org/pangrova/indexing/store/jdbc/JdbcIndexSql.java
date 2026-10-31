package org.pangrova.indexing.store.jdbc;

import org.pangrova.indexing.api.state.IndexState;

import static org.pangrova.indexing.store.jdbc.IndexMetadataMapper.placeholders;

/** SQL construction helpers shared by JDBC index stores. */
final class JdbcIndexSql {

    private JdbcIndexSql() {}

    static void validateGenerationTransition(
            int newGeneration, IndexState expectedState, int expectedGeneration) {
        if (newGeneration != Math.addExact(expectedGeneration, 1)) {
            throw new IllegalArgumentException(
                    "newGeneration must advance exactly one generation");
        }
        if (expectedState != IndexState.READY && expectedState != IndexState.STALE) {
            throw new IllegalArgumentException(
                    "Only READY or STALE indexes can reserve an online generation");
        }
    }

    static String buildColumnIndexSql(int stateCount, int columnCount) {
        StringBuilder sql = new StringBuilder(
                "SELECT * FROM index_descriptors "
                        + "WHERE tenant_id=? AND catalog_name=? AND schema_name=? AND table_name=? "
                        + "AND state IN (" + placeholders(stateCount) + ")");
        if (columnCount > 0) {
            sql.append(" AND (");
            appendColumnPredicates(sql, columnCount);
            sql.append(")");
        }
        return sql.toString();
    }

    private static void appendColumnPredicates(StringBuilder sql, int columnCount) {
        for (int index = 0; index < columnCount; index++) {
            if (index > 0) {
                sql.append(" OR ");
            }
            sql.append("jsonb_exists(column_names, ?)");
        }
    }
}
