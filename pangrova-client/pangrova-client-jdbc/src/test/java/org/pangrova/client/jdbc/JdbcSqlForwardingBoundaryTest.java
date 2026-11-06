package org.pangrova.client.jdbc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.transport.client.http.PangrovaRestClient;
import org.pangrova.client.session.RetryPolicy;
import org.pangrova.common.json.jackson.JsonMapperFactory;
import org.pangrova.client.error.ErrorDetail;
import org.pangrova.client.protocol.SubmitQueryRequest;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual JDBC/HTTP forwarding boundaries; the peer rejects SQL and never claims engine execution. */
class JdbcSqlForwardingBoundaryTest {
    private static final String SQL =
            "/* 原样 overload */\nINSERT INTO t VALUES ('a''b', 'x\\y', '雪') -- unchanged\n";
    private static final String TEMPLATE =
            "/* literal ? */\nSELECT \"col?\", '?' FROM t WHERE id = ? AND name = ?";
    private static final String BOUND_SQL =
            "/* literal ? */\nSELECT \"col?\", '?' FROM t WHERE id = 7 AND name = 'a''b雪'";

    @ParameterizedTest(name = "{0}")
    @MethodSource("statementOverloads")
    void statementOverloadTransmitsTheExactCallerSql(String name, StatementInvocation invocation) throws Exception {
        var received = new ConcurrentLinkedQueue<String>();
        try (var peer = new MetadataProtocolPeer()) {
            peer.server().createContext("/v1/query/submit", exchange -> {
                try (exchange) {
                    var request = JsonMapperFactory.create().readValue(
                            exchange.getRequestBody().readAllBytes(), SubmitQueryRequest.class);
                    received.add(request.query());
                    byte[] body = JsonMapperFactory.create().writeValueAsBytes(new ErrorDetail(
                            "FORWARDING_TEST_REJECTED", "ValidationException",
                            "SQL forwarding boundary rejected execution"));
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(400, body.length);
                    exchange.getResponseBody().write(body);
                }
            });
            peer.start();
            try (var connection = connect(peer); var statement = connection.createStatement()) {
                assertThatThrownBy(() -> invocation.execute(statement, SQL))
                        .as(name)
                        .isInstanceOf(SQLException.class)
                        .hasMessage("[FORWARDING_TEST_REJECTED] SQL forwarding boundary rejected execution");
                assertThat(received).as(name).containsExactly(SQL);
                assertThat(statement.getResultSet()).isNull();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("connectionOverloads")
    void connectionOverloadPreservesTheExactTemplateBeforeBinding(String name, Preparation preparation)
            throws Exception {
        try (var peer = new MetadataProtocolPeer()) {
            peer.start();
            try (var connection = connect(peer);
                 var statement = (PangrovaPreparedStatement) preparation.prepare(connection, TEMPLATE)) {
                assertThat(statement.getParameterMetaData().getParameterCount()).as(name).isEqualTo(2);
                statement.setInt(1, 7);
                statement.setString(2, "a'b雪");
                assertThat(statement.substituteParameters()).as(name).isEqualTo(BOUND_SQL);
                statement.clearParameters();
                assertThatThrownBy(statement::substituteParameters)
                        .as(name)
                        .isInstanceOf(SQLException.class)
                        .hasMessageContaining("Parameter 1 not set");
            }
        }
    }

    private static Stream<Arguments> statementOverloads() {
        return Stream.of(
                Arguments.of("execute(String,int)",
                        (StatementInvocation) (statement, sql) -> statement.execute(sql, Statement.NO_GENERATED_KEYS)),
                Arguments.of("execute(String,int[])",
                        (StatementInvocation) (statement, sql) -> statement.execute(sql, new int[] {1})),
                Arguments.of("execute(String,String[])",
                        (StatementInvocation) (statement, sql) -> statement.execute(sql, new String[] {"id"})),
                Arguments.of("executeUpdate(String,int)", (StatementInvocation) (statement, sql) ->
                        statement.executeUpdate(sql, Statement.NO_GENERATED_KEYS)),
                Arguments.of("executeUpdate(String,int[])",
                        (StatementInvocation) (statement, sql) -> statement.executeUpdate(sql, new int[] {1})),
                Arguments.of("executeUpdate(String,String[])",
                        (StatementInvocation) (statement, sql) -> statement.executeUpdate(sql, new String[] {"id"})));
    }

    private static Stream<Arguments> connectionOverloads() {
        return Stream.of(
                Arguments.of("prepareStatement(String,int)", (Preparation) (connection, sql) ->
                        connection.prepareStatement(sql, Statement.NO_GENERATED_KEYS)),
                Arguments.of("prepareStatement(String,int,int)", (Preparation) (connection, sql) ->
                        connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)),
                Arguments.of("prepareStatement(String,int,int,int)", (Preparation) (connection, sql) ->
                        connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                                ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                Arguments.of("prepareStatement(String,int[])",
                        (Preparation) (connection, sql) -> connection.prepareStatement(sql, new int[] {1})),
                Arguments.of("prepareStatement(String,String[])",
                        (Preparation) (connection, sql) -> connection.prepareStatement(sql, new String[] {"id"})));
    }

    private static PangrovaConnection connect(MetadataProtocolPeer peer) {
        var client = PangrovaRestClient.builder().coordinatorHost("127.0.0.1")
                .coordinatorPort(peer.port()).retryPolicy(RetryPolicy.NONE).build();
        return new PangrovaConnection(client, null);
    }

    @FunctionalInterface
    private interface StatementInvocation {
        void execute(Statement statement, String sql) throws SQLException;
    }

    @FunctionalInterface
    private interface Preparation {
        PreparedStatement prepare(PangrovaConnection connection, String sql) throws SQLException;
    }
}
