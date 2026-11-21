package org.pangrova.client.jdbc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.client.http.PangrovaRestClient;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests real prepared-statement binding admission, not SQL execution or distributed acceptance. */
class PreparedStatementBindingAdmissionTest {
    @ParameterizedTest(name = "{0} rejects indices outside actual markers")
    @MethodSource("supportedSetters")
    void supportedSettersRejectInvalidIndicesWithoutChangingTheBinding(String name, Setter setter) throws Exception {
        try (var connection = connection(); var statement = statement(connection, "SELECT ?")) {
            statement.setInt(1, 17);
            for (int index : new int[] {Integer.MIN_VALUE, -1, 0, 2, Integer.MAX_VALUE}) {
                assertThatThrownBy(() -> setter.bind(statement, index))
                        .as(name + " index=" + index)
                        .isInstanceOfSatisfying(SQLException.class,
                                failure -> assertThat(failure.getSQLState()).isEqualTo("07009"));
                assertThat(statement.substituteParameters()).isEqualTo("SELECT 17");
            }
        }
    }

    @ParameterizedTest(name = "{0} rejects a closed statement")
    @MethodSource("supportedSetters")
    void supportedSettersRejectClosedStatements(String name, Setter setter) throws Exception {
        try (var connection = connection(); var statement = statement(connection, "SELECT ?")) {
            statement.close();
            assertThatThrownBy(() -> setter.bind(statement, 1))
                    .as(name)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    @Test
    void quotedAndCommentedQuestionMarksDoNotAdmitBindings() throws Exception {
        try (var connection = connection();
                var statement = statement(connection, "SELECT '?' AS \"?\" /* ? */ -- ?\n")) {
            assertThat(statement.getParameterMetaData().getParameterCount()).isZero();
            assertThatThrownBy(() -> statement.setString(1, "not a parameter"))
                    .isInstanceOfSatisfying(SQLException.class,
                            failure -> assertThat(failure.getSQLState()).isEqualTo("07009"));
        }
    }

    @Test
    void actualMarkersStayConsistentWithMetadataAndCanBeReboundAndCleared() throws Exception {
        try (var connection = connection();
                var statement = statement(connection, "SELECT '?', ?, \"?\", ? /* ? */")) {
            assertThat(statement.getParameterMetaData().getParameterCount()).isEqualTo(2);
            statement.setInt(1, 7);
            statement.setString(2, "it's bound");
            assertThat(statement.substituteParameters())
                    .isEqualTo("SELECT '?', 7, \"?\", 'it''s bound' /* ? */");
            statement.setInt(1, 19);
            assertThat(statement.substituteParameters())
                    .isEqualTo("SELECT '?', 19, \"?\", 'it''s bound' /* ? */");
            statement.clearParameters();
            assertThatThrownBy(statement::substituteParameters)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("Parameter 1 not set");
        }
    }

    @Test
    void clearingClosedStatementIsRejected() throws Exception {
        try (var connection = connection(); var statement = statement(connection, "SELECT ?")) {
            statement.close();
            assertThatThrownBy(statement::clearParameters)
                    .isInstanceOf(SQLException.class)
                    .hasMessageContaining("closed");
        }
    }

    private static PangrovaConnection connection() {
        return new PangrovaConnection(PangrovaRestClient.builder().build(), null);
    }

    private static PangrovaPreparedStatement statement(PangrovaConnection connection, String sql) {
        return new PangrovaPreparedStatement(connection, sql);
    }

    private static Stream<Arguments> supportedSetters() {
        return Stream.of(
                setter("null", (s, i) -> s.setNull(i, Types.VARCHAR)),
                setter("boolean", (s, i) -> s.setBoolean(i, true)),
                setter("byte", (s, i) -> s.setByte(i, (byte) 1)),
                setter("short", (s, i) -> s.setShort(i, (short) 1)),
                setter("int", (s, i) -> s.setInt(i, 1)),
                setter("long", (s, i) -> s.setLong(i, 1L)),
                setter("float", (s, i) -> s.setFloat(i, 1.5F)),
                setter("double", (s, i) -> s.setDouble(i, 1.5D)),
                setter("decimal", (s, i) -> s.setBigDecimal(i, BigDecimal.ONE)),
                setter("string", (s, i) -> s.setString(i, "bound")),
                setter("date", (s, i) -> s.setDate(i, Date.valueOf("2026-01-02"))),
                setter("time", (s, i) -> s.setTime(i, Time.valueOf("03:04:05"))),
                setter("timestamp", (s, i) -> s.setTimestamp(i, Timestamp.valueOf("2026-01-02 03:04:05"))),
                setter("object", (s, i) -> s.setObject(i, "bound")),
                setter("typedObject", (s, i) -> s.setObject(i, "bound", Types.VARCHAR)),
                setter("scaledObject", (s, i) -> s.setObject(i, BigDecimal.ONE, Types.DECIMAL, 2)),
                setter("calendarDate", (s, i) -> s.setDate(i, Date.valueOf("2026-01-02"), Calendar.getInstance())),
                setter("calendarTime", (s, i) -> s.setTime(i, Time.valueOf("03:04:05"), Calendar.getInstance())),
                setter("calendarTimestamp", (s, i) -> s.setTimestamp(i,
                        Timestamp.valueOf("2026-01-02 03:04:05"), Calendar.getInstance())),
                setter("namedNull", (s, i) -> s.setNull(i, Types.VARCHAR, "VARCHAR")),
                setter("nationalString", (s, i) -> s.setNString(i, "bound")));
    }

    private static Arguments setter(String name, Setter setter) {
        return Arguments.of(name, setter);
    }

    @FunctionalInterface
    private interface Setter {
        void bind(PangrovaPreparedStatement statement, int index) throws SQLException;
    }
}
