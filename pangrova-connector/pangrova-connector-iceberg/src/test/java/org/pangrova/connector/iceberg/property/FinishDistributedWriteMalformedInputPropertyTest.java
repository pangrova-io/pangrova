package org.pangrova.connector.iceberg.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import net.jqwik.api.lifecycle.AfterProperty;
import net.jqwik.api.lifecycle.BeforeProperty;

import org.pangrova.connector.iceberg.HadoopTestGuard;
import org.pangrova.connector.iceberg.IcebergMetadata;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Property-based test for malformed input handling in
 * {@link IcebergMetadata#finishDistributedWrite}.
 *
 * <p>Verifies that for any random byte array (not a valid serialization format),
 * calling {@code finishDistributedWrite()} throws an exception with a clear error
 * message, and never throws {@link NullPointerException} or
 * {@link ArrayIndexOutOfBoundsException}.
 *
 * <p><b>Validates: Requirements 10.1, 10.2, 10.3</b></p>
 */
class FinishDistributedWriteMalformedInputPropertyTest {

    private static final String CATALOG_NAME = "malformed_test_catalog";
    private static final String NAMESPACE = "malformed_db";
    private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    private static final ConnectorSession SESSION = new ConnectorSession() {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public String getUser() { return "test"; }
        @Override public Optional<String> getCatalog() { return Optional.of(CATALOG_NAME); }
        @Override public Optional<String> getSchema() { return Optional.empty(); }
        @Override public Map<String, String> getProperties() { return Map.of(); }
    };

    private Path warehouse;
    private Catalog catalog;
    private IcebergMetadata metadata;

    @BeforeProperty
    void setUp() {
        if (!HadoopTestGuard.isHadoopCompatible()) return;
        try {
            warehouse = Files.createTempDirectory("malformed_input_property_test_");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, String> props = Map.of(
                "type", "hadoop",
                "warehouse", warehouse.toString());
        catalog = org.apache.iceberg.CatalogUtil.buildIcebergCatalog(
                CATALOG_NAME, props, new org.apache.hadoop.conf.Configuration());
        if (catalog instanceof SupportsNamespaces nsCatalog) {
            try {
                nsCatalog.createNamespace(Namespace.of(NAMESPACE));
            } catch (org.apache.iceberg.exceptions.AlreadyExistsException ignored) {
                // namespace may already exist across property tries
            }
        }
        metadata = new IcebergMetadata(catalog, CATALOG_NAME);
    }

    @AfterProperty
    void tearDown() {
        if (warehouse != null) {
            deleteRecursively(warehouse.toFile());
        }
    }

    /**
     * For any random byte array (not a valid serialization format), calling
     * {@code finishDistributedWrite()} with the random bytes as a fragment
     * should throw an exception, but never {@link NullPointerException} or
     * {@link ArrayIndexOutOfBoundsException}. The exception message must
     * not be null or empty.
     *
     * <p><b>Validates: Requirements 10.1, 10.2, 10.3</b></p>
     */
    @Property(tries = 200)
    @Tag("Feature_data-pipeline-post-fix-review")
    @Tag("Property_finishDistributedWrite_malformed_input")
    void malformedFragment_throwsDescriptiveException_neverNpeOrArrayOob(
            @ForAll("randomByteArrays") byte[] malformedFragment) {
        if (!HadoopTestGuard.isHadoopCompatible()) return;

        String tableName = "malformed_test_" + TABLE_COUNTER.incrementAndGet();
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, tableName);
        catalog.createTable(tableId, TEST_SCHEMA, PartitionSpec.unpartitioned());

        var tableHandle = metadata.getTableHandle(
                SESSION, new SchemaTableName(NAMESPACE, tableName));
        IcebergOutputTableHandle outputHandle =
                (IcebergOutputTableHandle) metadata.beginInsert(SESSION, tableHandle);

        Throwable thrown = catchThrowable(() ->
                metadata.finishDistributedWrite(SESSION, outputHandle,
                        List.of(malformedFragment)));

        assertThat(thrown)
                .as("malformed fragment must cause an exception")
                .isNotNull();
        assertThat(thrown)
                .as("exception must not be NullPointerException")
                .isNotInstanceOf(NullPointerException.class);
        assertThat(thrown)
                .as("exception must not be ArrayIndexOutOfBoundsException")
                .isNotInstanceOf(ArrayIndexOutOfBoundsException.class);
        assertThat(thrown.getMessage())
                .as("exception message must not be null or empty")
                .isNotNull()
                .isNotEmpty();
    }

    /**
     * Generates random byte arrays of various sizes (1-1000 bytes) that are
     * unlikely to be valid JSON serialization formats. Includes edge cases
     * like single-byte arrays, all-zeros, and high-entropy random data.
     */
    @Provide
    Arbitrary<byte[]> randomByteArrays() {
        Arbitrary<byte[]> randomBytes = Arbitraries.bytes()
                .array(byte[].class)
                .ofMinSize(1)
                .ofMaxSize(1000);

        Arbitrary<byte[]> edgeCases = Arbitraries.of(
                new byte[]{0},
                new byte[]{1},
                new byte[]{-1},
                new byte[]{0x7F},
                new byte[]{(byte) 0xFF},
                new byte[]{(byte) 0xFF, (byte) 0xFE},
                "not json at all".getBytes(),
                "{{{{".getBytes(),
                "]]]]".getBytes(),
                "<xml>not json</xml>".getBytes(),
                "\0\0\0\0\0".getBytes()
        );

        return Arbitraries.frequencyOf(
                net.jqwik.api.Tuple.of(7, randomBytes),
                net.jqwik.api.Tuple.of(3, edgeCases)
        );
    }

    private static void deleteRecursively(java.io.File file) {
        if (file.isDirectory()) {
            java.io.File[] children = file.listFiles();
            if (children != null) {
                for (java.io.File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
