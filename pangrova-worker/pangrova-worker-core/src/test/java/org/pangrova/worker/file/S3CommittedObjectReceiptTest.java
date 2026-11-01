package org.pangrova.worker.file;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.file.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.file.api.object.ObjectLocation;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual SDK/HTTP receipt fault injection, separate from the real MinIO write cases. */
class S3CommittedObjectReceiptTest {
    private HttpServer provider;
    private S3Client client;
    private ObjectLocation location;
    private final AtomicInteger puts = new AtomicInteger();
    private final AtomicInteger heads = new AtomicInteger();
    private String writeVersion = "committed-v1";
    // Keep the SDK's real PUT checksum validation enabled for this four-zero-byte body.
    private String writeEtag = "\"f1d3ff8443297732862df21dc4e57262\"";
    private String headVersion = "committed-v1";
    private String headEtag = writeEtag;
    private boolean includeModified = true;
    private int headStatus = 200;
    private volatile String headQuery;
    private volatile String headCondition;

    @BeforeEach
    void start() throws IOException {
        provider = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        provider.createContext("/", this::respond);
        provider.start();
        String endpoint = "http://127.0.0.1:" + provider.getAddress().getPort();
        client = S3Client.builder().region(Region.US_EAST_1).endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test-secret")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build()).build();
        location = new ObjectLocation(new StorageProfile("receipt", StorageType.S3, "bucket", "",
                endpoint, "us-east-1", Map.of(), Map.of("path-style-access", "true")), "space");
    }

    @AfterEach
    void close() {
        try {
            if (client != null) client.close();
        } finally {
            if (provider != null) provider.stop(0);
        }
    }

    @Test
    void immutableReceiptPinsBothVersionAndReturnedEntityTag() throws IOException {
        var result = write();
        assertThat(result.providerVersion()).isEqualTo("committed-v1");
        assertThat(headQuery).isEqualTo("versionId=committed-v1");
        assertThat(headCondition).isEqualTo(writeEtag);
        assertThat(puts.get()).isEqualTo(1);
        assertThat(heads.get()).isEqualTo(1);
    }

    @Test
    void mutableNullVersionUsesTheEntityTagCondition() throws IOException {
        writeVersion = "null";
        headVersion = "null";
        assertThat(write().providerVersion()).isEqualTo(writeEtag);
        assertThat(headQuery).isNull();
        assertThat(headCondition).isEqualTo(writeEtag);
    }

    @Test
    void missingReceiptIdentityDoesNotFallBackToCurrentHead() {
        writeVersion = null;
        writeEtag = null;
        assertUnverified();
        assertThat(heads.get()).isZero();
    }

    @Test
    void mismatchedVersionEvidenceCannotProduceAReceipt() {
        headVersion = "another-v2";
        assertUnverified();
        assertThat(heads.get()).isEqualTo(1);
    }

    @Test
    void mismatchedEntityTagCannotProduceAReceipt() {
        headEtag = "\"other\"";
        assertUnverified();
    }

    @Test
    void missingCommittedMetadataIsNotReportedAsAnUnwrittenObjectOrReplayed() {
        headStatus = 404;
        assertUnverified();
        assertThat(heads.get()).isEqualTo(1);
    }

    @Test
    void incompleteCommittedMetadataCannotProduceASuccessReceipt() {
        includeModified = false;
        assertUnverified();
    }

    private org.pangrova.file.api.directory.FileStat write() throws IOException {
        return new S3ObjectStoreAccessor(client).put(location, "item", new ByteArrayInputStream(new byte[4]),
                4, "application/octet-stream");
    }

    private void assertUnverified() {
        assertThatThrownBy(this::write).isInstanceOfSatisfying(PangrovaException.class, failure -> {
            assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.FILE_STORAGE_UNAVAILABLE);
            assertThat(failure).hasMessageContaining("committed");
        });
        assertThat(puts.get()).isEqualTo(1);
    }

    private void respond(HttpExchange exchange) throws IOException {
        try (exchange) {
            var headers = exchange.getResponseHeaders();
            if (exchange.getRequestMethod().equals("PUT")) {
                puts.incrementAndGet();
                exchange.getRequestBody().transferTo(java.io.OutputStream.nullOutputStream());
                if (writeVersion != null) headers.set("x-amz-version-id", writeVersion);
                if (writeEtag != null) headers.set("ETag", writeEtag);
                exchange.sendResponseHeaders(200, -1);
            } else {
                heads.incrementAndGet();
                headQuery = exchange.getRequestURI().getRawQuery();
                headCondition = exchange.getRequestHeaders().getFirst("If-Match");
                headers.set("Content-Length", "4");
                if (includeModified) headers.set("Last-Modified", "Tue, 1 Jan 2030 00:00:00 GMT");
                if (headVersion != null) headers.set("x-amz-version-id", headVersion);
                if (headEtag != null) headers.set("ETag", headEtag);
                exchange.sendResponseHeaders(headStatus, -1);
            }
        }
    }
}
