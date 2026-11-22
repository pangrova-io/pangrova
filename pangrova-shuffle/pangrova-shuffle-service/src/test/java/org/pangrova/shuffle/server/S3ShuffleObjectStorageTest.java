package org.pangrova.shuffle.server;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.core.retry.RetryPolicyContext;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.assertj.core.api.Assertions.assertThat;

class S3ShuffleObjectStorageTest {
    @Test
    void retriesSafeReadsButSurfacesUnconfirmedPutToTheReservationOwner() {
        var unavailable = RetryPolicyContext.builder().httpStatusCode(503).retriesAttempted(0)
                .exception(S3Exception.builder().statusCode(503).build());
        var condition = S3ShuffleObjectStorage.requestSafeRetries().retryCondition();

        assertThat(condition.shouldRetry(unavailable.originalRequest(GetObjectRequest.builder().build()).build()))
                .isTrue();
        assertThat(condition.shouldRetry(unavailable.originalRequest(PutObjectRequest.builder().build()).build()))
                .isFalse();
    }
}
