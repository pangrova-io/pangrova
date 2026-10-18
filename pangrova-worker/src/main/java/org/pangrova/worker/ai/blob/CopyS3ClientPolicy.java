package org.pangrova.worker.ai.blob;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.time.Duration;
import java.util.Objects;

/** Network limits for COPY's dedicated source and managed-object S3 clients. */
public final class CopyS3ClientPolicy {
    private static final Duration CALL_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SOCKET_TIMEOUT = Duration.ofSeconds(25);

    private CopyS3ClientPolicy() {}

    /** Applies the COPY-only API and socket time limits to an S3 client builder. */
    public static void configure(S3ClientBuilder builder) {
        Objects.requireNonNull(builder, "builder");
        builder.overrideConfiguration(config -> config
                .apiCallTimeout(CALL_TIMEOUT)
                .apiCallAttemptTimeout(ATTEMPT_TIMEOUT));
        builder.httpClientBuilder(ApacheHttpClient.builder()
                .connectionTimeout(CONNECT_TIMEOUT)
                .connectionAcquisitionTimeout(CONNECT_TIMEOUT)
                .socketTimeout(SOCKET_TIMEOUT));
    }
}
