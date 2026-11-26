package org.pangrova.server.coordinator.http.request;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.pangrova.common.client.query.SubmitQueryRequest;

import static org.assertj.core.api.Assertions.assertThat;

class QueryRequestFingerprintTest {
    @Test
    void keyDigestIsFixedSizeAndKeySpecific() {
        String first = QueryRequestFingerprint.keyDigest("key-a");
        String second = QueryRequestFingerprint.keyDigest("key-b");
        assertThat(first).matches("[0-9a-f]{64}");
        assertThat(second).matches("[0-9a-f]{64}");
        assertThat(first).isNotEqualTo(second);
        assertThat(QueryRequestFingerprint.keyDigest("key-a")).isEqualTo(first);
    }

    @Test
    void requestFingerprintCanonicalizesPropertiesWithoutConcatenationCollisions() {
        var first = QueryRequestFingerprint.request(
                new SubmitQueryRequest("SELECT 1", Map.of("ab", "c")));
        var reordered = QueryRequestFingerprint.request(
                new SubmitQueryRequest("SELECT 1", Map.of("z", "last", "ab", "c")));
        var collision = QueryRequestFingerprint.request(
                new SubmitQueryRequest("SELECT 1", Map.of("a", "bc")));
        assertThat(first).matches("[0-9a-f]{64}");
        assertThat(first).isNotEqualTo(reordered);
        assertThat(first).isNotEqualTo(collision);
    }
}
