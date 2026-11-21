package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;
import org.pangrova.client.http.PangrovaRestClient;
import org.pangrova.common.client.session.RetryPolicy;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual HTTP/JDBC metadata protocol behavior, not a deployed catalog authorization test. */
class JdbcMetadataProtocolTest {
    @Test
    void schemasIncludeEmptyNamespaceOnceAndHonorPattern() throws Exception {
        try (var peer = new MetadataProtocolPeer(); var connection = connect(peer)) {
            var metadata = connection.getMetaData();
            try (var schemas = metadata.getSchemas("lakehouse", null)) {
                assertThat(names(schemas, "TABLE_SCHEM")).containsExactly("empty", "public");
            }
            try (var schemas = metadata.getSchemas("lakehouse", "pub_ic")) {
                assertThat(schemas.next()).isTrue();
                assertThat(schemas.getString("TABLE_SCHEM")).isEqualTo("public");
                assertThat(schemas.getString("TABLE_CATALOG")).isEqualTo("lakehouse");
                assertThat(schemas.next()).isFalse();
            }
        }
    }

    @ParameterizedTest
    @MethodSource("patterns")
    void patternsAreJdbcWildcardsNotRegularExpressions(String pattern, List<String> expected) throws Exception {
        try (var peer = new MetadataProtocolPeer()) {
            String entries = Stream.of("a.b", "axb", "a_b", "a%b", "a[b]", "a😀b")
                    .map(name -> "{\"namespace\":[\"lakehouse\",\"public\"],\"name\":\"" + name + "\"}")
                    .collect(java.util.stream.Collectors.joining(","));
            peer.response(MetadataProtocolPeer.TABLES, 200, "{\"identifiers\":[" + entries + "]}");
            try (var connection = connect(peer); var tables = connection.getMetaData()
                    .getTables("lakehouse", "public", pattern, new String[]{"TABLE"})) {
                assertThat(names(tables, "TABLE_NAME")).containsExactlyInAnyOrderElementsOf(expected);
            }
        }
    }

    static Stream<Arguments> patterns() {
        return Stream.of(Arguments.of("a.b", List.of("a.b")), Arguments.of("a\\_b", List.of("a_b")),
                Arguments.of("a\\%b", List.of("a%b")), Arguments.of("a[b]", List.of("a[b]")),
                Arguments.of("a_b", List.of("a.b", "axb", "a_b", "a%b", "a😀b")), Arguments.of("", List.of()),
                Arguments.of((String) null, List.of("a.b", "axb", "a_b", "a%b", "a[b]", "a😀b")));
    }

    @Test
    void filteredColumnKeepsSourceOrdinalAndParameterizedType() throws Exception {
        try (var peer = new MetadataProtocolPeer(); var connection = connect(peer);
             var columns = connection.getMetaData().getColumns("lakehouse", "public", "orders", "amount")) {
            assertThat(columns.getMetaData().getColumnCount()).isEqualTo(24);
            assertThat(columns.next()).isTrue();
            assertThat(columns.getString("TABLE_SCHEM")).isEqualTo("public");
            assertThat(columns.getInt("ORDINAL_POSITION")).isEqualTo(3);
            assertThat(columns.getInt(17)).isEqualTo(3);
            assertThat(columns.getInt("DATA_TYPE")).isEqualTo(Types.DECIMAL);
            assertThat(columns.getString("TYPE_NAME")).isEqualTo("decimal(12,2)");
            assertThat(columns.getInt("COLUMN_SIZE")).isEqualTo(12);
            assertThat(columns.getInt("DECIMAL_DIGITS")).isEqualTo(2);
            assertThat(columns.getInt("NULLABLE")).isEqualTo(DatabaseMetaData.columnNullable);
            assertThat(columns.getString("IS_NULLABLE")).isEqualTo("YES");
            assertThat(columns.next()).isFalse();
        }
    }

    @Test
    void bigintIsNotIntegerAndRequiredColumnIsNotNullable() throws Exception {
        try (var peer = new MetadataProtocolPeer(); var connection = connect(peer);
             var columns = connection.getMetaData().getColumns("lakehouse", "public", "orders", "order\\_id")) {
            assertThat(columns.next()).isTrue();
            assertThat(columns.getInt("DATA_TYPE")).isEqualTo(Types.BIGINT);
            assertThat(columns.getInt("COLUMN_SIZE")).isEqualTo(19);
            assertThat(columns.getInt("NULLABLE")).isEqualTo(DatabaseMetaData.columnNoNulls);
            assertThat(columns.getString("IS_NULLABLE")).isEqualTo("NO");
            assertThat(columns.getInt("ORDINAL_POSITION")).isEqualTo(1);
            assertThat(columns.next()).isFalse();
        }
    }

    @Test
    void emptyCatalogAndUnsupportedTableTypeDoNotSearchAllCatalogs() throws Exception {
        try (var peer = new MetadataProtocolPeer(); var connection = connect(peer)) {
            try (var rows = connection.getMetaData().getTables("", null, null, null)) { assertThat(rows.next()).isFalse(); }
            try (var rows = connection.getMetaData().getTables("lakehouse", null, null, new String[]{"VIEW"})) {
                assertThat(rows.next()).isFalse();
            }
        }
    }

    @Test
    void validEmptyResponsesRemainEmpty() throws Exception {
        try (var peer = new MetadataProtocolPeer()) {
            peer.response(MetadataProtocolPeer.NAMESPACES, 200, "{\"namespaces\":[]}");
            peer.response(MetadataProtocolPeer.TABLES, 200, "{\"identifiers\":[]}");
            peer.response(MetadataProtocolPeer.NAMESPACES + "?parent=lakehouse", 200, "{\"namespaces\":[]}");
            try (var connection = connect(peer)) {
                try (var catalogs = connection.getMetaData().getCatalogs()) { assertThat(catalogs.next()).isFalse(); }
                try (var schemas = connection.getMetaData().getSchemas("lakehouse", null)) { assertThat(schemas.next()).isFalse(); }
                try (var tables = connection.getMetaData().getTables("lakehouse", null, null, null)) { assertThat(tables.next()).isFalse(); }
                try (var columns = connection.getMetaData().getColumns("lakehouse", null, null, null)) { assertThat(columns.next()).isFalse(); }
            }
        }
    }

    @ParameterizedTest
    @MethodSource("failures")
    void failuresAreSqlExceptionsWithOriginalCause(String path, int status, String body, String operation) throws Exception {
        try (var peer = new MetadataProtocolPeer()) {
            peer.response(path, status, body);
            try (var connection = connect(peer)) {
                assertThatThrownBy(() -> discover(connection.getMetaData(), operation)).isInstanceOf(SQLException.class)
                        .hasCauseInstanceOf(org.pangrova.common.client.exception.PangrovaClientException.class);
            }
        }
    }

    static Stream<Arguments> failures() {
        String denied = "{\"error_code\":\"FORBIDDEN\",\"message\":\"metadata denied\"}";
        return Stream.of(Arguments.of(MetadataProtocolPeer.NAMESPACES, 403, denied, "catalogs"),
                Arguments.of(MetadataProtocolPeer.NAMESPACES + "?parent=lakehouse", 500, denied, "schemas"),
                Arguments.of(MetadataProtocolPeer.TABLES, 401, denied, "tables"),
                Arguments.of(MetadataProtocolPeer.DETAILS, 404, denied, "columns"),
                Arguments.of(MetadataProtocolPeer.NAMESPACES, 200, "{}", "catalogs"),
                Arguments.of(MetadataProtocolPeer.TABLES, 200, "{\"identifiers\":null}", "tables"),
                Arguments.of(MetadataProtocolPeer.DETAILS, 200, "{\"metadata\":{\"currentSchemaId\":9,\"schemas\":[]}}", "columns"),
                Arguments.of(MetadataProtocolPeer.DETAILS, 200, MetadataProtocolPeer.fields("bigint", "varchar", "double")
                        .replace("\"required\":false", "\"required\":\"false\""), "columns"));
    }

    private static void discover(DatabaseMetaData metadata, String operation) throws SQLException {
        try (var rows = switch (operation) {
            case "catalogs" -> metadata.getCatalogs();
            case "schemas" -> metadata.getSchemas("lakehouse", null);
            case "tables" -> metadata.getTables("lakehouse", null, null, null);
            case "columns" -> metadata.getColumns("lakehouse", "public", "orders", null);
            default -> throw new IllegalArgumentException("Unknown operation");
        }) { rows.next(); }
    }

    private static PangrovaConnection connect(MetadataProtocolPeer peer) {
        peer.start();
        return new PangrovaConnection(PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                .coordinatorPort(peer.port()).retryPolicy(RetryPolicy.NONE).build(), null);
    }

    private static List<String> names(ResultSet rows, String column) throws SQLException {
        var result = new ArrayList<String>();
        while (rows.next()) result.add(rows.getString(column));
        return result;
    }
}
