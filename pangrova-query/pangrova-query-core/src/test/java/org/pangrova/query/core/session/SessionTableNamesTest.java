package org.pangrova.query.core.session;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.core.statement.StatementTableTargets;
import org.pangrova.parser.core.parse.AntlrSqlParser;
import org.pangrova.observability.api.tracing.ExecutionTraceContext;
import org.pangrova.observability.api.tracing.Scope;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SessionTableNamesTest {
    @Test
    void omittedComponentsUseTheExactSessionDefaults() {
        var session = session(Map.of("catalog", "Lake.X", "schema", "Db.Y"));
        assertThat(SessionTableNames.resolveTableRef(List.of("Rows.Z"), session))
                .isEqualTo(new QualifiedTableName("Lake.X", "Db.Y", "Rows.Z"));
        assertThat(SessionTableNames.resolveTableRef(List.of("Other.Db", "Rows.Z"), session))
                .isEqualTo(new QualifiedTableName("Lake.X", "Other.Db", "Rows.Z"));
        assertThat(SessionTableNames.resolveTableRef(List.of("C", "S", "T"), session))
                .isEqualTo(new QualifiedTableName("C", "S", "T"));
        assertThat(SessionTableNames.resolveTableRef(List.of("T"), session(Map.of())))
                .isEqualTo(new QualifiedTableName("default", "default", "T"));
    }

    @Test
    void standaloneBranchAndAlterTableFormsBindTheSameQuotedTarget() {
        var session = session(Map.of("catalog", "Lake.X", "schema", "Db.Y"));
        var parser = new AntlrSqlParser();
        var expected = new QualifiedTableName("Lake.X", "Db.Y", "Rows.Z");
        for (String sql : List.of("CREATE BRANCH dev ON \"Rows.Z\"", "DROP BRANCH dev ON \"Rows.Z\"",
                "CREATE TAG release ON \"Rows.Z\"", "DROP TAG release ON \"Rows.Z\"",
                "ALTER TABLE \"Rows.Z\" CREATE BRANCH dev", "ALTER TABLE \"Rows.Z\" CREATE TAG release")) {
            assertThat(StatementTableTargets.resolve(parser.parse(sql), session)).as(sql).isEqualTo(expected);
        }
    }

    private static QuerySession session(Map<String, String> properties) {
        return new QuerySession("q", properties, null, null, Optional.empty(), new FixtureTrace());
    }

    /** The target binding test does not exercise telemetry recording or claim authenticated identity. */
    private record FixtureTrace() implements ExecutionTraceContext {
        public String traceId() { return "00000000000000000000000000000001"; }
        public String spanId() { return "0000000000000001"; }
        public int traceFlags() { return 0; }
        public String traceState() { return ""; }
        public Scope makeCurrent() { return Scope.NOOP; }
    }
}
