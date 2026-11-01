package org.pangrova.coordinator.artifact;

import org.pangrova.indexing.store.jdbc.ArtifactCurrentAuthority;
import org.pangrova.metadata.core.TableControlDeferredPolicy;

import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
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
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.store.jdbc.JdbcGeneratedValueArtifactPublicationStore;
import org.pangrova.indexing.store.jdbc.JdbcArtifactBuildStore;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;
import org.pangrova.planner.substrait.NativeDatasetSchemaCompiler;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.DataSplit;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.base.ConnectorException;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.security.UserIdentity;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK/PG read-selection boundary tests; not public SQL or Native overlay execution proof. */
class GeneratedValueReadResolverIntegrationTest {
    private static final String SOURCE = "lake.db.documents";
    private static final CoordinatorSessionContext ACTOR = new CoordinatorSessionContext("read-history", Map.of(),
            new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant");

    @TempDir
    Path warehouse;

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
            assertThat(fixture.resolver().bindReadOverlays(plan(exact), ACTOR,
                    splits(version), Map.of()).operatorBindings()).isEmpty();
            var wrong = exact.toBuilder().setStruct(exact.getStruct().toBuilder().setTypes(
                    exact.getStruct().getTypesCount() - 1,
                    io.substrait.proto.Type.newBuilder().setI64(io.substrait.proto.Type.I64.newBuilder()
                            .setNullability(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE)))).build();
            assertThatThrownBy(() -> fixture.resolver().bindReadOverlays(plan(wrong), ACTOR,
                    splits(version), Map.of())).hasMessageContaining("declared SDK schema");
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
            assertThat(fixture.resolver().bindReadOverlays(protectedRead, ACTOR, splits, Map.of(SOURCE, version))
                    .operatorBindings()).isEmpty(); // No publication exists yet; this ASYNC generation is pending.
            var incomplete = new ConnectorTableMetadata(historical.table(), List.of("id"),
                    List.of(historical.typedColumns().get(0)), historical.properties());
            assertThatThrownBy(() -> fixture.resolver().bindReadOverlays(plan(incomplete), ACTOR,
                    splits, Map.of(SOURCE, version)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("declared SDK schema");
            assertThat(fixture.resolver().bindReadOverlays(plan(currentMetadata(fixture)), ACTOR, splits, Map.of())
                    .operatorBindings()).isEmpty(); // FULL uses current absence, not the old data snapshot declaration.
            assertThatThrownBy(() -> fixture.resolver().bindReadOverlays(plan(incomplete), ACTOR, splits, Map.of()))
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
            assertThatThrownBy(() -> fixture.resolver().bindReadOverlays(historicalRead, ACTOR,
                    splits(version), Map.of(SOURCE, version)))
                    .isInstanceOf(ConnectorException.class).hasMessageContaining("declaration evidence is unavailable");
            assertThat(fixture.resolver().bindReadOverlays(plan(currentMetadata(fixture)), ACTOR,
                    splits(version), Map.of())
                    .operatorBindings()).isEmpty();
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
        var registry = new CatalogRegistry(new JdbcMetadataStore(dataSource),
                descriptor -> new ConnectorCatalogHandle(descriptor, connector), null, null);
        registry.register(new CatalogDescriptor("lake", CatalogType.CONNECTOR, "iceberg", CatalogState.ACTIVE,
                Map.of(), "", Set.of("tenant")));
        var resolver = new GeneratedValueReadResolver(registry, new JdbcArtifactBuildStore(dataSource, new ArtifactCurrentAuthority(TableControlDeferredPolicy::requireCurrent)),
                new JdbcGeneratedValueArtifactPublicationStore(dataSource, new ArtifactCurrentAuthority(TableControlDeferredPolicy::requireCurrent)));
        return new Fixture(catalog, table, registry, resolver);
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

    private static byte[] plan(ConnectorTableMetadata metadata) {
        return plan(NativeDatasetSchemaCompiler.compile(metadata.typedColumns()).schema());
    }

    private static byte[] plan(io.substrait.proto.NamedStruct schema) {
        var read = ReadRel.newBuilder().setBaseSchema(schema)
                .setNamedTable(ReadRel.NamedTable.newBuilder().addAllNames(List.of("lake", "db", "documents")));
        return Plan.newBuilder().addRelations(PlanRel.newBuilder().setRoot(RelRoot.newBuilder()
                .setInput(Rel.newBuilder().setRead(read)))).build().toByteArray();
    }

    private static List<String> splits(long snapshot) {
        var split = DataSplit.newBuilder().setSplitId("actual-sdk-file").setConnectorId("iceberg")
                .setSourceId(SOURCE).setSourceSnapshot(Long.toString(snapshot)).setDataObjectId("row.parquet")
                .setOpaqueLocation("row.parquet").build();
        return List.of(Base64.getEncoder().encodeToString(split.toByteArray()));
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
                           GeneratedValueReadResolver resolver) implements AutoCloseable {
        @Override public void close() throws Exception {
            registry.shutdown();
            catalog.close();
        }
    }
}
