package org.pangrova.testing.integration.e2e.file;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.connector.spi.data.value.ColumnValue;
import org.pangrova.connector.spi.data.value.TypedRow;
import org.pangrova.testing.testkit.E2ETest;
import org.pangrova.testing.integration.e2e.PangrovaE2ETestBase;
import org.pangrova.worker.nativebridge.jni.NativeCapabilityLoader;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;

import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual SQL, shared production callbacks, JNI, Arrow results and HTTPS/SSE S3 publication. */
@E2ETest
@Tag("governed-file")
@ResourceLock(S3FileDeployment.CREDENTIAL_LOCK)
@DisplayName("Governed FILE SQL through the actual Native and S3 deployment")
class GovernedFileSqlE2ETest extends PangrovaE2ETestBase {
    @BeforeAll
    static void requireActualFileRegistration() {
        sharedS3FileStorage().requireConfiguredDeployment();
        var actual = NativeCapabilityLoader.loadInventory().inventory();
        var registration = actual.getFunctionCatalog().getRegisteredFunctionsList().stream()
                .filter(value -> value.getImplementationName().equals("file_from_bytes"))
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "The actual published Native catalogue must contain production FILE_FROM_BYTES"));
        assertThat(registration.getImplementationsList()).isNotEmpty();
        for (var entry : registration.getImplementationsList()) {
            assertThat(entry.hasMetadata()).isTrue();
            assertThat(entry.getMetadata().hasDeterministic()).isTrue();
            assertThat(entry.getMetadata().getDeterministic()).isFalse();
            assertThat(entry.getMetadata().hasDefaultNullBehavior()).isTrue();
            assertThat(entry.getMetadata().getDefaultNullBehavior()).isFalse();
        }
    }

    @Test
    void nullableBytesAndMimeRetainRowsWithoutPublishingNullObjects() throws Exception {
        byte[] expected = payload();
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        String hex = HexFormat.of().formatHex(expected);
        var rows = executeQuery("SELECT \"id\", FILE_FROM_BYTES(\"content\", \"mime\") AS \"file\" FROM ("
                + "SELECT 1 AS \"id\", X'" + hex + "' AS \"content\", 'image/png' AS \"mime\" "
                + "UNION ALL SELECT 2, CAST(NULL AS VARBINARY), 'image/png' "
                + "UNION ALL SELECT 3, X'" + hex + "', CAST(NULL AS VARCHAR)) AS source ORDER BY \"id\"");
        assertThat(rows).hasSize(3);
        assertIds(rows);
        assertPublishedFile(rows.get(0).get("file"), expected, before);
        assertThat(ColumnValue.toRaw(rows.get(1).get("file"))).isNull();
        assertThat(ColumnValue.toRaw(rows.get(2).get("file"))).isNull();
        assertThat(sharedS3FileStorage().publishedKeys()).hasSize(before.size() + 1);
        assertPutRequests(requestsBefore, 1);
    }

    @Test
    void caseAvoidsUnselectedMimeErrorAndPreservesBothPrimaryNullInputs() throws Exception {
        byte[] expected = payload();
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        String hex = HexFormat.of().formatHex(expected);
        var rows = executeQuery("SELECT \"id\", CASE WHEN \"id\"=2 THEN NULL "
                + "ELSE FILE_FROM_BYTES(\"content\", \"mime\") END AS \"file\" FROM ("
                + "SELECT 1 AS \"id\", X'" + hex + "' AS \"content\", 'image/png' AS \"mime\" "
                + "UNION ALL SELECT 2, X'" + hex + "', 'application/not-allowed' "
                + "UNION ALL SELECT 3, CAST(NULL AS VARBINARY), 'image/png' "
                + "UNION ALL SELECT 4, X'" + hex + "', CAST(NULL AS VARCHAR)) AS source ORDER BY \"id\"");
        assertThat(rows).hasSize(4);
        assertIds(rows);
        assertPublishedFile(rows.get(0).get("file"), expected, before);
        for (int ordinal = 1; ordinal < rows.size(); ordinal++) {
            assertThat(ColumnValue.toRaw(rows.get(ordinal).get("file"))).isNull();
        }
        assertThat(sharedS3FileStorage().publishedKeys()).hasSize(before.size() + 1);
        assertPutRequests(requestsBefore, 1);
    }

    @Test
    void aliasReferencesShareTheSameFileProducerIncludingCreationTimestamp() throws Exception {
        byte[] expected = payload();
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        var rows = executeQuery("SELECT \"file\" AS \"first\", \"file\" AS \"second\" FROM ("
                + "SELECT FILE_FROM_BYTES(X'" + HexFormat.of().formatHex(expected)
                + "', 'image/png') AS \"file\") AS source");
        assertThat(rows).hasSize(1);
        assertThat(ColumnValue.toRaw(rows.get(0).get("first")))
                .isEqualTo(ColumnValue.toRaw(rows.get(0).get("second")));
        assertPublishedFile(rows.get(0).get("first"), expected, before);
        assertThat(sharedS3FileStorage().publishedKeys()).hasSize(before.size() + 1);
        assertPutRequests(requestsBefore, 1);
    }

    @Test
    void independentIdenticalFileCallsRetainIndependentCreationTimestamps() throws Exception {
        byte[] expected = payload();
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        String invocation = "FILE_FROM_BYTES(X'" + HexFormat.of().formatHex(expected) + "', 'image/png')";
        var rows = executeQuery("SELECT " + invocation + " AS \"first\", "
                + invocation + " AS \"second\"");
        assertThat(rows).hasSize(1);
        assertPublishedFile(rows.get(0).get("first"), expected, before);
        assertPublishedFile(rows.get(0).get("second"), expected, before);
        var first = (Map<?, ?>) ColumnValue.toRaw(rows.get(0).get("first"));
        var second = (Map<?, ?>) ColumnValue.toRaw(rows.get(0).get("second"));
        assertThat(first.get("checksum")).isEqualTo(second.get("checksum"));
        assertThat(first.get("size")).isEqualTo(second.get("size"));
        // Each synchronous production S3 publication completes before FileFromBytesFunction calls now().
        // Reusing one Native expression would preserve the same timestamp and violates its declared volatility.
        assertThat(first.get("created_at")).isNotEqualTo(second.get("created_at"));
        assertThat(sharedS3FileStorage().publishedKeys()).hasSize(before.size() + 1);
        assertPutRequests(requestsBefore, 2);
    }

    @Test
    void independentDifferentPayloadsProduceTwoSuccessfulObjects() throws Exception {
        byte[] firstPayload = payload();
        byte[] secondPayload = payload();
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        var rows = executeQuery("SELECT FILE_FROM_BYTES(X'" + HexFormat.of().formatHex(firstPayload)
                + "', 'image/png') AS \"first\", FILE_FROM_BYTES(X'"
                + HexFormat.of().formatHex(secondPayload) + "', 'image/png') AS \"second\"");
        assertThat(rows).hasSize(1);
        assertPublishedFile(rows.get(0).get("first"), firstPayload, before, 2);
        assertPublishedFile(rows.get(0).get("second"), secondPayload, before, 2);
        assertThat(sharedS3FileStorage().publishedKeys()).hasSize(before.size() + 2);
        assertPutRequests(requestsBefore, 2);
    }

    @Test
    void selectedMimeErrorFailsTheQueryWithoutAnyPublishedObject() throws Exception {
        var before = sharedS3FileStorage().publishedKeys();
        long requestsBefore = sharedS3FileStorage().putRequestCount();
        String hex = HexFormat.of().formatHex(payload());
        assertThat(executeExpectingError("SELECT FILE_FROM_BYTES(X'" + hex
                + "', 'application/not-allowed')")).containsIgnoringCase("mime");
        assertThat(sharedS3FileStorage().publishedKeys()).containsExactlyElementsOf(before);
        assertPutRequests(requestsBefore, 0);
    }

    private static void assertPutRequests(long before, long expectedDelta) {
        // One full refresh window prevents a cached sample from hiding an additional producer invocation.
        await().pollInterval(Duration.ofMillis(200)).during(Duration.ofSeconds(11))
                .atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                        assertThat(sharedS3FileStorage().putRequestCount()).isEqualTo(before + expectedDelta));
    }

    private static void assertIds(List<TypedRow> rows) {
        for (int ordinal = 0; ordinal < rows.size(); ordinal++) {
            assertThat(((Number) ColumnValue.toRaw(rows.get(ordinal).get("id"))).intValue())
                    .isEqualTo(ordinal + 1);
        }
    }

    private static void assertPublishedFile(ColumnValue value, byte[] expected, List<String> before)
            throws Exception {
        assertPublishedFile(value, expected, before, 1);
    }

    private static void assertPublishedFile(
            ColumnValue value, byte[] expected, List<String> before, int expectedObjects) throws Exception {
        Object raw = ColumnValue.toRaw(value);
        assertThat(raw).isInstanceOf(Map.class);
        var fields = (Map<?, ?>) raw;
        assertThat(fields.keySet().stream().map(Object::toString).toList())
                .containsExactlyInAnyOrder("uri", "mime_type", "size", "checksum", "created_at");
        assertThat(fields.get("uri").toString()).startsWith("pangrova-blob://v2/");
        assertThat(fields.get("mime_type").toString()).isEqualTo("image/png");
        assertThat(fields.get("size")).isEqualTo((long) expected.length);
        assertThat(fields.get("created_at")).isNotNull();
        String checksum = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(expected));
        assertThat(fields.get("checksum").toString()).isEqualTo(checksum);
        var keys = sharedS3FileStorage().publishedKeys();
        assertThat(keys).hasSize(before.size() + expectedObjects).containsAll(before);
        var added = keys.stream().filter(key -> !before.contains(key) && key.endsWith("/" + checksum)).toList();
        assertThat(added).hasSize(1);
        String key = added.get(0);
        assertThat(key).endsWith("/" + checksum);
        assertThat(sharedS3FileStorage().readPublishedObject(key)).containsExactly(expected);
        var metadata = sharedS3FileStorage().publishedObjectMetadata(key);
        assertThat(metadata.contentLength()).isEqualTo((long) expected.length);
        assertThat(metadata.contentType()).isEqualTo("application/octet-stream");
        assertThat(metadata.metadata()).containsEntry("sha256", checksum)
                .containsEntry("content-length", Integer.toString(expected.length));
        assertThat(metadata.serverSideEncryption()).isEqualTo(ServerSideEncryption.AES256);
    }

    private static byte[] payload() {
        return UUID.randomUUID().toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }
}
