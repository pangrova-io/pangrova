package org.pangrova.coordinator.generation.read;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.testinfra.ModelAuthorityFixtures;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SetRel;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.iceberg.IcebergConnector;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.exception.ConnectorException;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.security.api.identity.UserIdentity;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK/PG read-selection boundary tests; not public SQL or Native overlay execution proof. */
class GeneratedValueReadResolverIntegrationTest {
    private static final String SOURCE = "lake.db.documents";
    private static final QuerySession ACTOR = new QuerySession("read-history", Map.of(),
            new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant", java.util.Optional.empty(), new org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory().capture());

    @TempDir
    Path warehouse;

    @Test
    void stageUsesAlreadyAdmittedDeclarationAndRejectsSplitsFromAnotherSnapshot() throws Exception {
        try (var fixture = fixture()) {
            long version = fixture.table().currentSnapshot().snapshotId();
            var selected = source(fixture, null);
            var declaration = selected.get(0).readBinding().orElseThrow().declaration().requireExactDeclaration();
            byte[] compiled = plan(declaration);
            fixture.table().updateSchema().addColumn("later", Types.StringType.get()).commit();

            assertThat(bind(fixture.resolver(), compiled, ACTOR, splits(version), selected)
                    .operatorBindings()).isEmpty();
            assertThatThrownBy(() -> bind(fixture.resolver(), compiled, ACTOR,
                    splits(version == Long.MAX_VALUE ? version - 1 : version + 1), selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("admitted snapshot");
        }
    }

    @Test
    void retrievalIdentityMayExtendOnlyTheExactDeclaredScanSchema() throws Exception {
        try (var fixture = fixture()) {
            long version = fixture.table().currentSnapshot().snapshotId();
            var declared = metadata(fixture, version);
            var base = NativeDatasetSchemaCompiler.compile(declared.typedColumns()).schema();
            var required = io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED;
            var schema = base.toBuilder().addAllNames(
                    org.pangrova.protocol.retrieval.RetrievalCandidateIdentity.PHYSICAL_FIELDS);
            var struct = base.getStruct().toBuilder();
            for (int index = 0; index < 3; index++) {
                struct.addTypes(io.substrait.proto.Type.newBuilder().setString(
                        io.substrait.proto.Type.String.newBuilder().setNullability(required)));
            }
            struct.addTypes(io.substrait.proto.Type.newBuilder().setI64(
                    io.substrait.proto.Type.I64.newBuilder().setNullability(required)));
            var exact = schema.setStruct(struct).build();
            assertThat(bind(fixture.resolver(), plan(exact), ACTOR,
                    splits(version), source(fixture, null)).operatorBindings()).isEmpty();
            var wrong = exact.toBuilder().setStruct(exact.getStruct().toBuilder().setTypes(
                    exact.getStruct().getTypesCount() - 1,
                    io.substrait.proto.Type.newBuilder().setI64(io.substrait.proto.Type.I64.newBuilder()
                            .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))).build();
            assertThatThrownBy(() -> bind(fixture.resolver(), plan(wrong), ACTOR,
                    splits(version), source(fixture, null))).hasMessageContaining("declared SDK schema");
        }
    }

    @Test
    void timeTravelStillConsumesHistoricalGeneratedDeclarationAfterCurrentDefinitionRemoval() throws Exception {
        try (var fixture = fixture()) {
            long version = fixture.table().currentSnapshot().snapshotId();
            var historical = metadata(fixture, version);
            fixture.table().updateProperties().remove(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY)
                    .remove(GeneratedColumnMetadataCodec.SHA256_PROPERTY).commit();
            fixture.table().updateSchema().addColumn("later", Types.StringType.get()).commit();
            var splits = splits(version);
            byte[] protectedRead = plan(historical);
            assertThat(bind(fixture.resolver(), protectedRead, ACTOR, splits, source(fixture, version))
                    .operatorBindings()).isEmpty(); // No publication exists yet; this ASYNC generation is pending.
            var incomplete = new ConnectorTableMetadata(historical.table(), List.of("id"),
                    List.of(historical.typedColumns().get(0)), historical.properties());
            assertThatThrownBy(() -> bind(fixture.resolver(), plan(incomplete), ACTOR,
                    splits, source(fixture, version)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("declared SDK schema");
            assertThat(bind(fixture.resolver(), plan(currentMetadata(fixture)), ACTOR, splits, source(fixture, null))
                    .operatorBindings()).isEmpty(); // FULL uses current absence, not the old data snapshot declaration.
            assertThatThrownBy(() -> bind(fixture.resolver(), plan(incomplete), ACTOR, splits, source(fixture, null)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("declared SDK schema");
        }
    }

    @Test
    void expiredHistoricalEvidenceIsUnsupportedWhileFullUsesNewCurrentDeclarationAndSchema() throws Exception {
        try (var fixture = fixture()) {
            long version = fixture.table().currentSnapshot().snapshotId();
            byte[] historicalRead = plan(metadata(fixture, version));
            fixture.table().updateProperties().set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1")
                    .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true").commit();
            var updated = definitions().toBuilder().setDefinitions(0,
                    definitions().getDefinitions(0).toBuilder().setLogicalModelName("new-model")).build();
            var update = fixture.table().updateProperties();
            GeneratedColumnMetadataCodec.encode(updated).forEach(update::set);
            update.commit();
            fixture.table().updateSchema().addColumn("later", Types.StringType.get()).commit();
            fixture.table().updateProperties().set("irrelevant", "new-revision").commit();
            assertThatThrownBy(() -> bind(fixture.resolver(), historicalRead, ACTOR,
                    splits(version), source(fixture, version)))
                    .isInstanceOf(ConnectorException.class).hasMessageContaining("declaration evidence is unavailable");
            assertThat(bind(fixture.resolver(), plan(currentMetadata(fixture)), ACTOR,
                    splits(version), source(fixture, null))
                    .operatorBindings()).isEmpty();
        }
    }

    @Test
    void repeatedPhysicalReadsShareOnePublicationLookupPerStage() throws Exception {
        try (var fixture = fixture()) {
            var dataSource = fixture.dataSource();
            try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
                statement.execute("CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public");
            }
            long version = fixture.table().currentSnapshot().snapshotId();
            var selected = source(fixture, version);
            var compiled = Plan.parseFrom(plan(metadata(fixture, version)));
            var root = compiled.getRelations(0).getRoot();
            var reads = SetRel.newBuilder().setOp(SetRel.SetOp.SET_OP_UNION_ALL);
            for (int ordinal = 0; ordinal < 8; ordinal++) {
                reads.addInputs(root.getInput());
            }
            byte[] stage = compiled.toBuilder().setRelations(0, PlanRel.newBuilder()
                    .setRoot(root.toBuilder().setInput(Rel.newBuilder().setSet(reads))))
                    .build().toByteArray();
            for (int execution = 0; execution < 3; execution++) {
                long before = publicationReads(dataSource);
                var bound = bind(fixture.resolver(), stage, ACTOR, splits(version), selected);
                assertThat(bound.operatorBindings()).isEmpty();
                assertThat(bound.planBytes()).containsExactly(stage);
                assertThat(publicationReads(dataSource) - before).as("publication SELECTs per stage").isOne();
            }

            var read = root.getInput().getRead();
            var incompatible = read.getBaseSchema().toBuilder().setNames(0, "different_column").build();
            reads.setInputs(7, root.getInput().toBuilder()
                    .setRead(read.toBuilder().setBaseSchema(incompatible)).build());
            byte[] invalidStage = compiled.toBuilder().setRelations(0, PlanRel.newBuilder()
                    .setRoot(root.toBuilder().setInput(Rel.newBuilder().setSet(reads))))
                    .build().toByteArray();
            assertThatThrownBy(() -> bind(fixture.resolver(),
                    invalidStage, ACTOR, splits(version), selected))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("declared SDK schema");
        }
    }

    private static long publicationReads(DataSource dataSource) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                SELECT COALESCE(SUM(calls), 0) FROM public.pg_stat_statements
                WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                  AND query LIKE 'SELECT tenant_id,artifact_id,generation,source_id,source_snapshot,target_column%'
                  AND query LIKE '%FROM generated_value_generation_manifests%'
                """); var rows = statement.executeQuery()) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private Fixture fixture() throws Exception {
        var configuration = new Configuration();
        configuration.set("fs.file.impl", org.apache.hadoop.fs.RawLocalFileSystem.class.getName());
        var catalog = new HadoopCatalog(configuration, warehouse.toString());
        var schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "title", Types.StringType.get()),
                Types.NestedField.optional(3, "embedding", Types.ListType.ofOptional(4, Types.FloatType.get())));
        var properties = new LinkedHashMap<>(GeneratedColumnMetadataCodec.encode(definitions()));
        properties.put("pangrova.vector.dimension.embedding", "3");
        var table = catalog.createTable(TableIdentifier.of("db", "documents"), schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(), properties);
        append(table);
        var dataSource = SharedPostgresContainer.migratedDataSource("generated_read_history");
        var connector = new IcebergConnector("lake", catalog);
        var registry = new CatalogRegistry(memoryTableStores(), new PostgresCatalogDeploymentRepository(dataSource),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.register(new CatalogDescriptor("lake", CatalogType.CONNECTOR, "iceberg", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant")));
        var resolver = new GeneratedValueReadResolver(ModelAuthorityFixtures.artifacts().builds(dataSource),
                ModelAuthorityFixtures.artifacts().generatedPublications(dataSource));
        return new Fixture(catalog, table, registry, resolver, dataSource);
    }

    private static List<org.pangrova.planner.analysis.SplitSourceDescriptor> source(
            Fixture fixture, Long historicalVersion) {
        var metadata = fixture.registry().getCatalog("lake").getMetadata(session());
        var handle = metadata.getTableHandle(session(), new SchemaTableName("db", "documents"));
        if (historicalVersion != null) {
            handle = handle.withTimeTravelSnapshot(historicalVersion);
        }
        var binding = metadata.bindRead(session(), handle);
        return List.of(new org.pangrova.planner.analysis.SplitSourceDescriptor(
                "lake", "db", "documents", org.pangrova.connector.api.metadata.Constraint.EMPTY,
                binding.declaration().metadata().columns(), java.util.Optional.of(binding)));
    }

    private static ConnectorTableMetadata metadata(Fixture fixture, long snapshot) {
        var session = session();
        var metadata = fixture.registry().getCatalog("lake").getMetadata(session);
        var handle = metadata.getTableHandle(session, new SchemaTableName("db", "documents"));
        return metadata.getTableReadMetadata(session, handle.withTimeTravelSnapshot(snapshot)).requireExactDeclaration();
    }

    private static ConnectorTableMetadata currentMetadata(Fixture fixture) {
        var metadata = fixture.registry().getCatalog("lake").getMetadata(session());
        return metadata.getTableMetadata(session(),
                metadata.getTableHandle(session(), new SchemaTableName("db", "documents")));
    }

    private static ConnectorSession session() {
        return SessionContextConverter.toConnectorSession(ACTOR, "lake", "db");
    }

    private static org.pangrova.protocol.execution.BoundExecutionPlan bind(
            GeneratedValueReadResolver resolver, byte[] plan, QuerySession actor, List<DataSplit> splits,
            List<org.pangrova.planner.analysis.SplitSourceDescriptor> sources)
            throws com.google.protobuf.InvalidProtocolBufferException {
        return resolver.bindReadOverlays(
                org.pangrova.protocol.substrait.SubstraitPlanCodec.parse(plan), actor, splits, sources);
    }

    private static byte[] plan(ConnectorTableMetadata metadata) {
        return plan(NativeDatasetSchemaCompiler.compile(metadata.typedColumns()).schema());
    }

    private static byte[] plan(io.substrait.proto.NamedStruct schema) {
        var read = ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(List.of("lake", "db", "documents")));
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setRead(read)))).build().toByteArray();
    }

    private static List<DataSplit> splits(long snapshot) {
        var split = DataSplit.newBuilder().setSplitId("actual-sdk-file").setConnectorId("iceberg")
                .setSourceId(SOURCE).setSourceSnapshot(Long.toString(snapshot)).setDataObjectId("row.parquet")
                .setOpaqueLocation("row.parquet").build();
        return List.of(split);
    }

    private static void append(Table table) throws Exception {
        var output = table.io().newOutputFile(table.location() + "/data/row.parquet");
        var writer = Parquet.writeData(output).schema(table.schema()).withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create).build();
        try (writer) {
            var record = GenericRecord.create(table.schema());
            record.setField("id", 1L);
            record.setField("title", "title");
            writer.write(record);
        }
        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private static GeneratedColumnSet definitions() {
        return GeneratedColumnSet.newBuilder().setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(GeneratedColumnDefinition.newBuilder().setTargetColumn("embedding")
                        .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef()).setLogicalModelName("embedding")
                        .addSourceColumns("title").setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                        .setOutputType(VectorLogicalType.create(3)))
                .build();
    }

    private record Fixture(HadoopCatalog catalog, Table table, CatalogRegistry registry,
                           GeneratedValueReadResolver resolver, DataSource dataSource) implements AutoCloseable {
        @Override public void close() throws Exception {
            registry.shutdown();
            catalog.close();
        }
    }
}
