package org.pangrova.query.statement;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.statement.maintenance.CallStatement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableProcedureCallTest {
    @Test
    void warmupPreservesLiteralDotsCaseAndItsProcedureCatalog() {
        var call = bind("CALL \"Lake.A\".system.WARMUP_TABLE('Schema.B', 'Table.C')");
        assertThat(call.isWarmup()).isTrue();
        assertThat(call.target()).isEqualTo(new QualifiedTableName("Lake.A", "Schema.B", "Table.C"));
        assertThat(call.arguments()).containsEntry("arg0", "Schema.B").containsEntry("arg1", "Table.C");
        assertThatThrownBy(() -> call.arguments().put("arg0", "other")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void bothSupportedWarmupFormsBindTheSameTarget() {
        assertThat(bind("CALL lake.warmup_table('db.rows')").target())
                .isEqualTo(bind("CALL lake.warmup_table('db', 'rows')").target());
    }

    @Test
    void connectorProcedureRetainsLiteralArgumentsAndIsNotReadOnlyWarmup() {
        var call = bind("CALL lake.compact('db.rows', 17, 1.25, TRUE)");
        assertThat(call.isWarmup()).isFalse();
        assertThat(call.target()).isEqualTo(new QualifiedTableName("lake", "db", "rows"));
        assertThat(call.arguments()).containsEntry("arg1", "17").containsEntry("arg2", "1.25")
                .containsEntry("arg3", "true");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "CALL lake.warmup_table()", "CALL lake.warmup_table('db.rows', 'other', 'ignored')",
            "CALL lake.warmup_table('')", "CALL lake.warmup_table('db', '')",
            "CALL lake.warmup_table('db', '   ')", "CALL lake.warmup_table('db', 17)",
            "CALL lake.warmup_table('db.rows.more')", "CALL lake.warmup_table('.rows')",
            "CALL lake.warmup_table('db.')", "CALL lake.warmup_table('rows')",
            "CALL warmup_table('db.rows')", "CALL a.b.c.warmup_table('db.rows')",
            "CALL lake.compact(17)", "CALL lake.compact('db.rows', current_time)"
    })
    void invalidTargetsAndNonliteralArgumentsAreRejectedBeforeExecution(String sql) {
        var parsed = (CallStatement) new AntlrSqlParser().parse(sql);
        assertThatThrownBy(() -> TableProcedureCall.bind(parsed)).isInstanceOf(PangrovaException.class)
                .satisfies(failure -> assertThat(((PangrovaException) failure).getErrorCode())
                        .isEqualTo(org.pangrova.common.enums.PangrovaErrorCode.PLANNER_VALIDATION_FAILURE));
    }

    private static TableProcedureCall bind(String sql) {
        return TableProcedureCall.bind((CallStatement) new AntlrSqlParser().parse(sql));
    }
}
