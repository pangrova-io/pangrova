package org.pangrova.filesystem.s3.object;

import org.junit.jupiter.api.Test;
import org.pangrova.filesystem.api.profile.StorageProfile;
import org.pangrova.common.enums.StorageType;
import org.pangrova.filesystem.api.object.ObjectLocation;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** SDK metadata policy cases; these do not qualify a real encrypted S3 provider. */
class S3ObjectChecksumEvidenceTest {
    private static final String OPAQUE_ETAG = "\"0123456789abcdef0123456789abcdef\"";
    private final ObjectLocation location = new ObjectLocation(new StorageProfile("test", StorageType.S3,
            "bucket", "", "", "us-east-1", Map.of(), Map.of()), "");

    @Test
    void md5LookingEtagDoesNotProveAContentChecksumForAnyEncryptionDeclaration() {
        for (String encryption : List.of("", "AES256", "aws:kms", "aws:kms:dsse")) {
            var response = head().serverSideEncryption(encryption.isEmpty() ? null : encryption).build();
            var result = new S3ObjectStoreAccessor(new MetadataClient(response)).head(location, "item");
            assertThat(result.md5()).isNull();
            assertThat(result.etag()).isEqualTo(OPAQUE_ETAG);
            assertThat(result.providerVersion()).isEqualTo("version-1");
        }
    }

    @Test
    void customerEncryptionDoesNotTurnItsEtagIntoMd5() {
        var response = head().sseCustomerAlgorithm("AES256").build();
        var result = new S3ObjectStoreAccessor(new MetadataClient(response)).head(location, "item");
        assertThat(result.md5()).isNull();
        assertThat(result.etag()).isEqualTo(OPAQUE_ETAG);
    }

    @Test
    void listingPreservesOpaqueRecoveryIdentityWithoutInventingMd5() {
        var result = new S3ObjectStoreAccessor(new MetadataClient(head().build()))
                .list(location, "", null, 10);
        assertThat(result.files()).singleElement().satisfies(file -> {
            assertThat(file.md5()).isNull();
            assertThat(file.etag()).isEqualTo(OPAQUE_ETAG);
            assertThat(file.providerVersion()).isEqualTo(OPAQUE_ETAG);
        });
    }

    private static HeadObjectResponse.Builder head() {
        return HeadObjectResponse.builder().contentLength(10L).lastModified(Instant.EPOCH)
                .eTag(OPAQUE_ETAG).versionId("version-1");
    }

    private record MetadataClient(HeadObjectResponse response) implements S3Client {
        @Override public HeadObjectResponse headObject(HeadObjectRequest ignored) { return response; }
        @Override public ListObjectsV2Response listObjectsV2(ListObjectsV2Request ignored) {
            return ListObjectsV2Response.builder().isTruncated(false).contents(S3Object.builder()
                    .key("item").size(10L).lastModified(Instant.EPOCH).eTag(OPAQUE_ETAG).build()).build();
        }
        @Override public String serviceName() { return "s3"; }
        @Override public void close() { }
    }
}
