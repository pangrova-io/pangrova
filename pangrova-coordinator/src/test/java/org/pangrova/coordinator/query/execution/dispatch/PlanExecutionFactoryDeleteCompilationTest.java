package org.pangrova.coordinator.query.execution.dispatch;

import java.util.List;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.coordinator.metadata.read.MetadataSchemaBuilder;
import org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.Fixture;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.service.MetadataService;
import org.pangrova.metadata.service.MetadataSessionContext;
import org.pangrova.metadata.service.ResolvedTable;
import org.pangrova.planner.DmlWriteTarget;
import org.pangrova.planner.PangrovaPlanner;
import org.pangrova.planner.PlannedQuery;
import org.pangrova.planner.SqlCompiler;
import org.pangrova.planner.substrait.SubstraitConverter;
import org.pangrova.planner.substrait.SubstraitPlanSerializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.CATALOG;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.SCHEMA;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.TABLE_NAME;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.assertDmlResult;
import static org.pangrova.coordinator.query.execution.dispatch.PlanExecutionFactoryDeleteTestSupport.deleteAll;

/** Compiles the original DELETE AST through the domain-facing planner and canonical protocol. */
class PlanExecutionFactoryDeleteCompilationTest {

    @ParameterizedTest(name = "empty survivor compilation, partitioned={0}")
    @ValueSource(booleans = {false, true})
    void deleteAllCompilesAsProtectedDeleteWithEmptySurvivors(boolean partitioned) {
        var fixture = new Fixture(partitioned);
        fixture.metadata.deleteFailure = new UnsupportedOperationException("Metadata DELETE is unsupported");
        SqlCompiler compiler = buildCompiler(fixture);
        fixture.onDispatch = statement -> {
            PlannedQuery write = compiler.compileStatementToPlannedQuery(statement);
            var connectorSession = org.pangrova.coordinator.query.session.SessionContextConverter.toConnectorSession(
                    fixture.session, CATALOG, SCHEMA);
            var actual = fixture.metadata.getTableMetadata(connectorSession,
                    fixture.metadata.getTableHandle(connectorSession, PlanExecutionFactoryDeleteTestSupport.TABLE));
            var definitions = org.pangrova.protocol.GeneratedColumnMetadataCodec.decode(actual.properties());
            assertThat(write.dmlWriteTarget()).contains(
                    new DmlWriteTarget(TABLE_NAME.parts(), DmlWriteTarget.DmlOperation.DELETE,
                            java.util.Optional.of(org.pangrova.spi.coordination.WriteDeclarationSnapshot.capture(
                                    new org.pangrova.spi.connector.metadata.table.QualifiedTableName(CATALOG, SCHEMA,
                                            PlanExecutionFactoryDeleteTestSupport.TABLE.table()),
                                    actual, definitions.toByteArray(), false))));
            try {
                var proto = io.substrait.proto.Plan.parseFrom(write.serializeValidated(compiler));
                var input = proto.getRelations(0).getRoot().getInput();
                assertThat(input.hasFilter()).isTrue();
                assertThat(input.getFilter().getCondition().hasLiteral()).isTrue();
                assertThat(input.getFilter().getCondition().getLiteral().getBoolean()).isFalse();
            } catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new AssertionError("DELETE must serialize to the canonical protocol", failure);
            }
        };

        StatementResult result = fixture.execute(deleteAll());

        assertDmlResult(result);
        fixture.assertNativePending("dispatch");
    }

    static SqlCompiler buildCompiler(Fixture fixture) {
        var schemaBuilder = new MetadataSchemaBuilder(new DeleteSchemaListing(), fixture.registry);
        return new SqlCompiler(new PangrovaPlanner(schemaBuilder.buildRootSchema(
                org.pangrova.coordinator.metadata.MetadataPlanningTestSessions.session())),
                new SubstraitConverter(), new SubstraitPlanSerializer());
    }

    /** Only catalog/schema listing is allowed while building the real planner schema. */
    private static final class DeleteSchemaListing implements MetadataService {
        @Override
        public List<String> listCatalogs(MetadataSessionContext session) {
            return List.of(CATALOG);
        }

        @Override
        public List<String> listSchemas(MetadataSessionContext session, String catalogName) {
            assertThat(catalogName).isEqualTo(CATALOG);
            return List.of(SCHEMA);
        }

        @Override
        public void createCatalog(MetadataSessionContext session, CatalogDescriptor descriptor) {
            throw new AssertionError("DELETE compilation must not create catalogs");
        }

        @Override
        public void dropCatalog(MetadataSessionContext session, String catalogName, boolean cascade) {
            throw new AssertionError("DELETE compilation must not drop catalogs");
        }

        @Override
        public ResolvedTable resolveTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            throw new AssertionError("Table schemas must be resolved through the guarded connector");
        }

        @Override
        public void createTable(MetadataSessionContext session, QualifiedTableRef tableRef, TableMetadata metadata) {
            throw new AssertionError("DELETE compilation must not create tables");
        }

        @Override
        public void dropTable(MetadataSessionContext session, QualifiedTableRef tableRef) {
            throw new AssertionError("DELETE compilation must not drop tables");
        }

        @Override
        public void alterTable(MetadataSessionContext session, QualifiedTableRef tableRef, List<SchemaChange> changes) {
            throw new AssertionError("DELETE compilation must not alter tables");
        }

        @Override
        public void beginQuery(org.pangrova.metadata.service.MetadataSessionContext context) {
            String queryId = context.queryId();
            throw new AssertionError("Compilation must not begin a metadata transaction");
        }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) {
            String queryId = authority.queryId();
            throw new AssertionError("Compilation must not end a metadata transaction");
        }
    }
}
