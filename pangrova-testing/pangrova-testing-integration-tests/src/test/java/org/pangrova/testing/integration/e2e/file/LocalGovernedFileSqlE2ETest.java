package org.pangrova.testing.integration.e2e.file;

import java.net.URI;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.spi.connector.data.value.ColumnValue;
import org.pangrova.filesystem.api.blob.BlobObjectId;
import org.pangrova.filesystem.local.blob.FileSystemBlobStorage;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.testing.integration.e2e.config.ConnectorTestConfig;
import org.pangrova.testing.testkit.E2ETest;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual LOCAL FILE SQL/JNI/publication; owns a separate Native JVM from the S3 acceptance suite. */
@E2ETest
@Tag("native-runtime-owner")
@ResourceLock("pangrova-packaged-native-runtime")
class LocalGovernedFileSqlE2ETest extends PangrovaE2ETestBase {
    @BeforeAll
    static void requireLocalDeployment() {
        sharedLocalFileStorage().requireConfiguredDeployment();
    }

    @Override
    protected List<ConnectorTestConfig> connectorConfigs() {
        return List.of(ConnectorTestConfig.icebergLocal("local_file_catalog", "default",
                icebergWarehouse().toString()));
    }

    @Test
    void nativeFilePublicationPreservesNullsAndCanBeReadByAnIndependentProvider() throws Exception {
        byte[] content = payload();
        var before = sharedLocalFileStorage().publishedObjects();
        String hex = HexFormat.of().formatHex(content);
        var rows = executeQuery("SELECT \"id\", FILE_FROM_BYTES(\"bytes\", \"mime\") AS \"file\" FROM ("
                + "SELECT 1 AS \"id\", X'" + hex + "' AS \"bytes\", 'image/png' AS \"mime\" "
                + "UNION ALL SELECT 2, CAST(NULL AS VARBINARY), 'image/png' "
                + "UNION ALL SELECT 3, X'" + hex + "', CAST(NULL AS VARCHAR)) t ORDER BY \"id\"");
        assertThat(rows).hasSize(3);
        assertPublished(rows.get(0).get("file"), content);
        assertThat(ColumnValue.toRaw(rows.get(1).get("file"))).isNull();
        assertThat(ColumnValue.toRaw(rows.get(2).get("file"))).isNull();
        assertThat(sharedLocalFileStorage().publishedObjects()).hasSize(before.size() + 1).containsAll(before);
    }

    @Test
    void unselectedMimeFailureDoesNotInvokeTheProducerAndSelectedFailurePublishesNothing() throws Exception {
        var before = sharedLocalFileStorage().publishedObjects();
        var rows = executeQuery("SELECT CASE WHEN \"id\"=1 THEN NULL "
                + "ELSE FILE_FROM_BYTES(X'010203', 'application/not-allowed') END AS \"file\" "
                + "FROM (SELECT 1 AS \"id\") t");
        assertThat(rows).hasSize(1);
        assertThat(ColumnValue.toRaw(rows.get(0).get("file"))).isNull();
        assertThat(executeExpectingError("SELECT FILE_FROM_BYTES(X'010203', 'application/not-allowed')"))
                .containsIgnoringCase("mime");
        assertThat(sharedLocalFileStorage().publishedObjects()).containsExactlyElementsOf(before);
    }

    @Test
    void independentNativeInvocationsKeepVolatileResultsButShareOneContentIdentity() throws Exception {
        byte[] content = payload();
        String expression = "FILE_FROM_BYTES(X'" + HexFormat.of().formatHex(content) + "', 'image/png')";
        var before = sharedLocalFileStorage().publishedObjects();
        var rows = executeQuery("SELECT " + expression + " AS \"first\", " + expression + " AS \"second\"");
        assertThat(rows).hasSize(1);
        Map<?, ?> first = assertPublished(rows.get(0).get("first"), content);
        Map<?, ?> second = assertPublished(rows.get(0).get("second"), content);
        assertThat(first.get("uri")).isEqualTo(second.get("uri"));
        assertThat(first.get("created_at")).isNotEqualTo(second.get("created_at"));
        assertThat(sharedLocalFileStorage().publishedObjects()).hasSize(before.size() + 1).containsAll(before);
    }

    @Test
    void fileColumnSurvivesNativeWriteAndLaterTableScan() throws Exception {
        String table = "local_file_catalog.default.files_" + UUID.randomUUID().toString().replace("-", "");
        byte[] content = payload();
        executeDdl("CREATE TABLE " + table + " (id BIGINT, attachment FILE)");
        try {
            executeDml("INSERT INTO " + table + " SELECT CAST(7 AS BIGINT), FILE_FROM_BYTES(X'"
                    + HexFormat.of().formatHex(content) + "', 'image/png')");
            var first = executeQuery("SELECT id, attachment FROM " + table);
            var second = executeQuery("SELECT id, attachment FROM " + table);
            assertThat(first).hasSize(1);
            assertThat(second).hasSize(1);
            assertThat(ColumnValue.toRaw(first.get(0).get("id"))).isEqualTo(7L);
            Map<?, ?> file = assertPublished(first.get(0).get("attachment"), content);
            assertThat(ColumnValue.toRaw(second.get(0).get("attachment"))).isEqualTo(file);
        } finally {
            executeDdl("DROP TABLE " + table);
        }
    }

    private static Map<?, ?> assertPublished(ColumnValue value, byte[] expected) throws Exception {
        Object raw = ColumnValue.toRaw(value);
        assertThat(raw).isInstanceOf(Map.class);
        Map<?, ?> fields = (Map<?, ?>) raw;
        String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(expected));
        assertThat(fields.get("checksum")).isEqualTo(checksum);
        assertThat(fields.get("size")).isEqualTo((long) expected.length);
        assertThat(fields.get("mime_type")).isEqualTo("image/png");
        URI uri = URI.create(fields.get("uri").toString());
        assertThat(uri.getScheme()).isEqualTo("pangrova-blob");
        assertThat(uri.getHost()).isEqualTo("v2");
        String[] parts = uri.getRawPath().split("/");
        assertThat(parts).hasSize(4);
        assertThat(parts[3]).isEqualTo(checksum);
        Path root = sharedLocalFileStorage().objectRoot();
        try (var independent = new FileSystemBlobStorage(root);
             var input = independent.open(new BlobObjectId(parts[1], checksum, expected.length))) {
            assertThat(input.readAllBytes()).containsExactly(expected);
        }
        return fields;
    }

    private static byte[] payload() {
        return UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
