package org.pangrova.connector.paimon;

import java.nio.file.Path;
import java.io.ByteArrayInputStream;
import java.nio.channels.Channels;
import java.security.MessageDigest;
import java.util.stream.Stream;

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.pangrova.connector.paimon.handle.PaimonOutputHandleCodec;
import org.pangrova.connector.paimon.handle.PaimonOutputTableHandle;
import org.pangrova.connector.paimon.handle.PaimonTableHandleFactory;
import org.pangrova.connector.paimon.schema.PaimonPhysicalSchemas;
import org.pangrova.protocol.type.DeclaredLogicalTypes;
import org.pangrova.protocol.type.VectorLogicalType;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Handles preserve schemas obtained from a real persisted Paimon catalog. */
class PaimonHandleSchemaIntegrationTest {
    @TempDir Path directory;

    @Test
    void persistedSdkSchemaSurvivesReadAndOutputHandleRoundTripsWithoutParameterLoss() throws Exception {
        try (var catalog = catalog()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "typed");
            catalog.createTable(id, schema()
                    .option("file.format", "lance").option("bucket", "1")
                    .option("bucket-key", "id").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            assertRoundTrips(table, false);
            assertPhysicalSchema(table, false);
        }
    }

    @Test
    void actualMixedSdkStoragePreservesCompleteMapAndVectorMetadataWithoutClaimingOnePhysicalFile() throws Exception {
        try (var catalog = catalog()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "typed");
            catalog.createTable(id, schema()
                    .column("attributes", DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()))
                    .option("file.format", "parquet").option("vector.file.format", "lance")
                    .option("data-evolution.enabled", "true").option("row-tracking.enabled", "true")
                    .option("bucket", "-1").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            assertThat(table.options()).containsEntry("file.format", "parquet")
                    .containsEntry("vector.file.format", "lance");
            assertRoundTrips(table, true);
            assertPhysicalSchema(table, true);
            assertThatThrownBy(() -> org.pangrova.connector.paimon.write.PaimonPhysicalWriteAllocations
                    .append(table, -1)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void realSdkRejectsMapAndLocalZonedTimestampInAnOrdinaryLanceFile() throws Exception {
        try (var catalog = catalog()) {
            catalog.createDatabase("db", false);
            for (var type : java.util.List.of(DataTypes.MAP(DataTypes.STRING().notNull(), DataTypes.BIGINT()),
                    DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6))) {
                assertThatThrownBy(() -> catalog.createTable(Identifier.create("db", "invalid"),
                        Schema.newBuilder().column("id", DataTypes.BIGINT()).column("unsupported", type)
                                .option("file.format", "lance").option("bucket", "1")
                                .option("bucket-key", "id").build(), false))
                        .hasRootCauseInstanceOf(UnsupportedOperationException.class)
                        .hasStackTraceContaining("Lance file format does not support type");
                assertThatThrownBy(() -> catalog.getTable(Identifier.create("db", "invalid")))
                        .isInstanceOf(Catalog.TableNotExistException.class);
            }
        }
    }

    /** Java metadata only: the SDK retains LIST storage; this does not prove Native VECTOR writes. */
    @ParameterizedTest
    @MethodSource("propertyVectorDeclarations")
    void propertyVectorJavaSchemaRetainsDimensionAndSdkListWithoutClaimingNativeExecution(
            boolean doubled, boolean nullable, boolean elementNullable) throws Exception {
        var element = doubled ? new org.apache.paimon.types.DoubleType(elementNullable)
                : new org.apache.paimon.types.FloatType(elementNullable);
        var vectorStorage = new org.apache.paimon.types.ArrayType(nullable, element);
        try (var catalog = catalog()) {
            catalog.createDatabase("db", false);
            var id = Identifier.create("db", "property_vector");
            catalog.createTable(id, Schema.newBuilder().column("id", DataTypes.BIGINT().notNull())
                    .column("embedding", vectorStorage)
                    .column("history", DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.DECIMAL(21, 7).notNull())))
                    .option("pangrova.vector.dimension.embedding", "7")
                    .option("file.format", "lance").option("bucket", "1")
                    .option("bucket-key", "id").build(), false);
            var table = (FileStoreTable) catalog.getTable(id);
            assertThat(table.schema().fields().get(1).type()).isEqualTo(vectorStorage);
            var output = PaimonTableHandleFactory.outputFromTable("db", "property_vector", table);
            var codec = new PaimonOutputHandleCodec();
            var restored = (PaimonOutputTableHandle) codec.deserialize(codec.codecVersion(), codec.serialize(output));
            assertThat(PaimonTableHandleFactory.parseRowType(restored)).isEqualTo(table.rowType());
            assertThat(codec.serialize(restored)).isEqualTo(codec.serialize(output));
            assertPropertyVectorSchema(table, doubled, nullable, elementNullable);
        }
    }

    static Stream<Arguments> propertyVectorDeclarations() {
        return Stream.of(false, true).flatMap(doubled -> Stream.of(false, true).flatMap(nullable ->
                Stream.of(false, true).map(elementNullable -> Arguments.of(doubled, nullable, elementNullable))));
    }

    private static void assertPropertyVectorSchema(
            FileStoreTable table, boolean doubled, boolean nullable, boolean elementNullable) throws Exception {
        var physical = PaimonPhysicalSchemas.forAppend(table.schema());
        var column = physical.columns().get(1);
        assertThat(physical.schemaId()).isEqualTo(table.schema().id());
        assertThat(column.fieldId()).isEqualTo(table.schema().fields().get(1).id());
        assertThat(column.name()).isEqualTo("embedding");
        assertThat(column.logicalType()).isEqualTo(new org.pangrova.types.VectorType(7,
                doubled ? org.pangrova.types.VectorType.ElementType.DOUBLE
                        : org.pangrova.types.VectorType.ElementType.FLOAT).getTypeSignature());
        assertThat(column.nullable()).isEqualTo(nullable);
        assertThat(column.declaredType().children()).singleElement().satisfies(child -> {
            assertThat(child.nullable()).isEqualTo(elementNullable);
            assertThat(child.signature().base()).isEqualTo(doubled ? "DOUBLE" : "REAL");
        });
        assertThat(DeclaredLogicalTypes.logicalType(column.declaredType())).isEqualTo(VectorLogicalType.create(7,
                doubled ? VectorLogicalType.FLOAT64 : VectorLogicalType.FLOAT32, nullable, elementNullable));
        var history = DeclaredLogicalTypes.logicalType(physical.columns().get(2).declaredType())
                .getSubstraitType().getList().getType().getList().getType().getDecimal();
        assertThat(history.getPrecision()).isEqualTo(21);
        assertThat(history.getScale()).isEqualTo(7);
        assertThat(history.getNullability()).isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertPropertyVectorArrowList(table, physical, doubled);
    }

    private static void assertPropertyVectorArrowList(FileStoreTable table,
            org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation.Schema physical,
            boolean doubled) throws Exception {
        var storage = (org.apache.paimon.types.ArrayType) table.schema().fields().get(1).type();
        try (var channel = new ReadChannel(Channels.newChannel(new ByteArrayInputStream(physical.physicalSchema())))) {
            var schema = MessageSerializer.deserializeSchema(channel);
            assertThat(schema.getFields()).extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "embedding", "history");
            var field = schema.getFields().get(1);
            assertThat(field.getType()).isEqualTo(ArrowType.List.INSTANCE);
            assertThat(field.isNullable()).isEqualTo(storage.isNullable());
            assertThat(field.getMetadata()).containsEntry("PARQUET:field_id",
                    Integer.toString(table.schema().fields().get(1).id()));
            assertThat(field.getChildren()).singleElement().satisfies(child -> {
                assertThat(child.isNullable()).isEqualTo(storage.getElementType().isNullable());
                assertThat(child.getType()).isEqualTo(new ArrowType.FloatingPoint(doubled
                        ? org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE
                        : org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE));
            });
        }
    }

    private Catalog catalog() {
        Options options = new Options();
        options.setString("warehouse", directory.toString());
        return CatalogFactory.createCatalog(CatalogContext.create(options));
    }

    private static Schema.Builder schema() {
        return Schema.newBuilder().column("id", DataTypes.BIGINT().notNull())
                .column("amount", DataTypes.DECIMAL(38, 17))
                .column("embedding", DataTypes.VECTOR(3, DataTypes.DOUBLE()))
                .column("items", DataTypes.ARRAY(DataTypes.ROW(DataTypes.FIELD(0, "label", DataTypes.VARCHAR(27)),
                        DataTypes.FIELD(1, "numbers", DataTypes.ARRAY(DataTypes.INT().notNull())))));
    }

    private static void assertRoundTrips(FileStoreTable table, boolean withMap) {
        var read = PaimonTableHandleFactory.fromTable("db", "typed", table);
        var output = PaimonTableHandleFactory.outputFromTable("db", "typed", table);
        assertThat(PaimonTableHandleFactory.parseRowType(read)).isEqualTo(table.rowType());
        assertThat(PaimonTableHandleFactory.parseRowType(output)).isEqualTo(table.rowType());
        var codec = new PaimonOutputHandleCodec();
        var decoded = (PaimonOutputTableHandle) codec.deserialize(codec.codecVersion(), codec.serialize(output));
        assertThat(decoded).isEqualTo(output);
        assertThat(PaimonTableHandleFactory.parseRowType(decoded)).isEqualTo(table.rowType());
        if (withMap) assertThat(codec.writeColumnNames(decoded)).containsExactly("id", "amount", "embedding", "items", "attributes");
        else assertThat(codec.writeColumnNames(decoded)).containsExactly("id", "amount", "embedding", "items");
        assertThatThrownBy(() -> codec.deserialize(2, codec.serialize(output)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("version");
    }

    private static void assertPhysicalSchema(FileStoreTable table, boolean withMap) throws Exception {
        var physical = PaimonPhysicalSchemas.forAppend(table.schema());
        assertThat(physical.schemaId()).isEqualTo(table.schema().id());
        assertThat(physical.sha256()).isEqualTo(MessageDigest.getInstance("SHA-256")
                .digest(physical.physicalSchema()));
        try (var channel = new ReadChannel(Channels.newChannel(
                new ByteArrayInputStream(physical.physicalSchema())))) {
            var actual = MessageSerializer.deserializeSchema(channel);
            if (withMap) assertThat(actual.getFields()).extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "amount", "embedding", "items", "attributes");
            else assertThat(actual.getFields()).extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("id", "amount", "embedding", "items");
            assertThat(actual.getFields().get(0).isNullable()).isFalse();
            assertThat(actual.getFields().get(2).getType()).isEqualTo(new ArrowType.FixedSizeList(3));
            assertThat(actual.getFields().get(2).getMetadata()).containsEntry("PARQUET:field_id",
                    Integer.toString(table.schema().fields().get(2).id()));
            assertThat(actual.getFields().get(3).getChildren().get(0).getChildren())
                    .extracting(org.apache.arrow.vector.types.pojo.Field::getName)
                    .containsExactly("label", "numbers");
            assertThat(physical.columns().get(2).logicalType().parameters().get(0).longValue()).isEqualTo(3L);
            assertThat(physical.columns().get(2).logicalType().parameters().get(1).typeSignature().base())
                    .isEqualTo("DOUBLE");
            var logical = DeclaredLogicalTypes.logicalType(physical.columns().get(2).declaredType());
            assertThat(VectorLogicalType.requireStorageDimension(logical)).isEqualTo(3);
            assertThat(logical.getSubstraitType().getList().getType().getFp64().getNullability())
                    .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
            var nested = DeclaredLogicalTypes.logicalType(physical.columns().get(3).declaredType());
            assertThat(nested.getSubstraitType().getList().getType().getStruct().getTypes(1)
                    .getList().getType().getI32().getNullability())
                    .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
            if (withMap) assertMapTypes(physical, actual);
        }
    }

    private static void assertMapTypes(org.pangrova.connector.api.metadata.NativePhysicalWriteAllocation.Schema schema,
                                       org.apache.arrow.vector.types.pojo.Schema physical) {
        var key = physical.getFields().get(4).getChildren().get(0).getChildren().get(0);
        assertThat(key.isNullable()).isFalse();
        var logical = DeclaredLogicalTypes.logicalType(schema.columns().get(4).declaredType());
        assertThat(logical.getSubstraitType().getMap().getKey().getString().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(logical.getSubstraitType().getMap().getValue().getI64().getNullability())
                .isEqualTo(io.substrait.proto.Type.Nullability.NULLABILITY_NULLABLE);
    }
}
