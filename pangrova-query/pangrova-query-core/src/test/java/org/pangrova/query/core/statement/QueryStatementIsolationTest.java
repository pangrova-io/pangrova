package org.pangrova.query.core.statement;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real statement binding needs contracts, not parser/compiler/metadata/executor implementations. */
class QueryStatementIsolationTest {
    @Test
    void bindsAProcedureUsingOnlyPublicContractsAndItsOwnCoreClasses() throws Exception {
        URL[] urls = {
                location(TableProcedureCall.class), location(org.pangrova.query.api.session.QuerySession.class),
                location(org.pangrova.parser.api.ast.Statement.class),
                location(org.pangrova.connector.api.metadata.table.QualifiedTableName.class),
                location(org.pangrova.types.Type.class), location(org.pangrova.common.api.annotation.Internal.class),
                location(org.pangrova.security.api.identity.UserIdentity.class),
                location(org.pangrova.observability.api.tracing.ExecutionTraceContext.class)
        };
        try (var loader = new URLClassLoader(urls, ClassLoader.getPlatformClassLoader())) {
            for (String name : List.of("org.pangrova.query.core.statement.StatementClassifier",
                    "org.pangrova.query.core.statement.StatementTableTargets",
                    "org.pangrova.query.core.statement.TableProcedureCall",
                    "org.pangrova.query.core.session.SessionTableNames")) {
                var type = Class.forName(name, true, loader);
                assertThat(type.getClassLoader()).isSameAs(loader);
                type.getDeclaredMethods();
                type.getDeclaredFields();
                type.getDeclaredConstructors();
                type.getRecordComponents();
            }
            var identifier = loader.loadClass("org.pangrova.parser.api.ast.Identifier");
            Object name = identifier.getConstructor(List.class).newInstance(List.of("Lake.A", "warmup_table"));
            var literal = loader.loadClass("org.pangrova.parser.api.ast.Literal");
            Object schema = literal.getMethod("ofString", String.class).invoke(null, "Schema.B");
            Object table = literal.getMethod("ofString", String.class).invoke(null, "Table.C");
            var callType = loader.loadClass("org.pangrova.parser.api.ast.statement.maintenance.CallStatement");
            Object ast = callType.getConstructor(identifier, List.class).newInstance(name, List.of(schema, table));
            var boundType = loader.loadClass("org.pangrova.query.core.statement.TableProcedureCall");
            Object bound = boundType.getMethod("bind", callType).invoke(null, ast);
            Object target = boundType.getMethod("target").invoke(bound);
            assertThat(target.getClass().getMethod("catalog").invoke(target)).isEqualTo("Lake.A");
            assertThat(target.getClass().getMethod("schema").invoke(target)).isEqualTo("Schema.B");
            assertThat(target.getClass().getMethod("table").invoke(target)).isEqualTo("Table.C");
            for (String forbidden : List.of("org.pangrova.parser.core.parse.AntlrSqlParser",
                    "org.antlr.v4.runtime.Parser", "org.apache.calcite.rel.RelNode",
                    "org.pangrova.coordinator.query.statement.TableWarmupExecutor",
                    "org.pangrova.metadata.core.resolution.QualifiedTableNames",
                    "org.pangrova.server.coordinator.bootstrap.CoordinatorBootstrap", "org.postgresql.Driver")) {
                assertThatThrownBy(() -> loader.loadClass(forbidden)).isInstanceOf(ClassNotFoundException.class);
            }
        }
    }

    private static URL location(Class<?> type) {
        return type.getProtectionDomain().getCodeSource().getLocation();
    }
}
