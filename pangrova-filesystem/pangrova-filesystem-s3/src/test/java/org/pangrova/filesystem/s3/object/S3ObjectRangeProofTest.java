package org.pangrova.filesystem.s3.object;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.storage.StorageType;
import org.pangrova.filesystem.api.object.ObjectLocation;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** SDK response fault injection only; real-provider positive cases live in the MinIO suite. */
class S3ObjectRangeProofTest {
    private final ObjectLocation location = new ObjectLocation(new StorageProfile("test", StorageType.S3,
            "bucket", "", "", "us-east-1", Map.of(), Map.of()), "");

    @Test
    void rejectsMissingWrongOrOverflowingByteWindowEvidenceAndAbortsTheBody() throws IOException {
        for (String range : List.of("", "bytes 0-2/10", "bytes 2-4/11", "bytes 2-5/10",
                "bytes 2-4/9999999999999999999999999999")) {
            var client = client();
            var aborted = new AtomicInteger();
            var response = GetObjectResponse.builder().eTag("etag").contentLength(3L)
                    .contentRange(range.isEmpty() ? null : range).build();
            client.reply = new ResponseInputStream<>(response,
                    AbortableInputStream.create(new ByteArrayInputStream(new byte[]{2, 3, 4}),
                            aborted::incrementAndGet));
            assertThatThrownBy(() -> new S3ObjectStoreAccessor(client).openRange(location, "item", 2, 3, null))
                    .isInstanceOf(PangrovaException.class).hasMessageContaining("byte window");
            assertThat(aborted.get()).isEqualTo(1);
        }
    }

    @Test
    void rejectsAnIgnoredRangeEvenWhenTheFullObjectRetainsTheExpectedEtag() {
        var client = client();
        var aborted = new AtomicInteger();
        var response = GetObjectResponse.builder().eTag("etag").contentLength(10L).build();
        client.reply = new ResponseInputStream<>(response,
                AbortableInputStream.create(new ByteArrayInputStream(new byte[10]), aborted::incrementAndGet));
        assertThatThrownBy(() -> new S3ObjectStoreAccessor(client).openRange(location, "item", 2, 3, null))
                .isInstanceOf(PangrovaException.class).hasMessageContaining("byte window");
        assertThat(aborted.get()).isEqualTo(1);
    }

    @Test
    void acceptsTheExactWindowAndPreservesTheConditionalRangeRequest() throws IOException {
        var client = client();
        var response = GetObjectResponse.builder().eTag("etag").contentLength(3L)
                .contentRange("BYTES 0002-0004/0010").build();
        client.reply = new ResponseInputStream<>(response,
                AbortableInputStream.create(new ByteArrayInputStream(new byte[]{2, 3, 4})));
        try (var input = new S3ObjectStoreAccessor(client).openRange(location, "item", 2, 3, null)) {
            assertThat(input.readAllBytes()).containsExactly((byte) 2, (byte) 3, (byte) 4);
        }
        assertThat(client.gets).isEqualTo(1);
        assertThat(client.request.range()).isEqualTo("bytes=2-4");
        assertThat(client.request.ifMatch()).isEqualTo("etag");
    }

    @Test
    void zeroLengthDoesNotIssueAnInvalidGetAndInvalidNegativeLengthsAreRejected() throws IOException {
        var client = client();
        var accessor = new S3ObjectStoreAccessor(client);
        try (var input = accessor.openRange(location, "item", 10, 0, null)) {
            assertThat(input.read()).isEqualTo(-1);
        }
        assertThatThrownBy(() -> accessor.openRange(location, "item", 0, -2, null))
                .isInstanceOf(PangrovaException.class);
        assertThat(client.gets).isZero();
    }

    private static FaultClient client() { return new FaultClient(); }

    private static final class FaultClient implements S3Client {
        private ResponseInputStream<GetObjectResponse> reply;
        private GetObjectRequest request;
        private int gets;

        @Override public HeadObjectResponse headObject(HeadObjectRequest ignored) {
            return HeadObjectResponse.builder().contentLength(10L).eTag("etag")
                    .lastModified(java.time.Instant.EPOCH).build();
        }

        @Override public ResponseInputStream<GetObjectResponse> getObject(GetObjectRequest supplied) {
            gets++;
            request = supplied;
            return java.util.Objects.requireNonNull(reply, "Unexpected GET in a zero-length test");
        }

        @Override public String serviceName() { return "s3"; }
        @Override public void close() { }
    }
}
