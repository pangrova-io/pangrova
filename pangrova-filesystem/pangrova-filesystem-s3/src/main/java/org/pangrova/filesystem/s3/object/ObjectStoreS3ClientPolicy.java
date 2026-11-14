package org.pangrova.filesystem.s3.object;

import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.time.Duration;

/**
 * Network limits for the object-store accessor used by table scans and writes.
 *
 * <p>The SDK defaults cap the Apache pool at 50 connections with no explicit
 * call/socket timeouts, which throttles parallel split reads on large tables and
 * lets a stalled connection hang a scan indefinitely. These values are separate
 * from {@code CopyS3ClientPolicy}, whose 2-minute call timeout targets COPY.</p>
 */
public final class ObjectStoreS3ClientPolicy {

    // Whole-call budget: a single large object read may legitimately take minutes.
    private static final Duration CALL_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration ATTEMPT_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration SOCKET_TIMEOUT = Duration.ofMinutes(1);
    private static final Duration CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(10);
    // One per concurrent split read with headroom; the SDK default of 50 is the
    // binding constraint when many splits are read in parallel.
    private static final int MAX_CONNECTIONS = 200;

    private ObjectStoreS3ClientPolicy() {}

    public static void configure(S3ClientBuilder builder) {
        builder.overrideConfiguration(config -> config
                .apiCallTimeout(CALL_TIMEOUT)
                .apiCallAttemptTimeout(ATTEMPT_TIMEOUT));
        builder.httpClientBuilder(ApacheHttpClient.builder()
                .maxConnections(MAX_CONNECTIONS)
                .connectionTimeout(CONNECT_TIMEOUT)
                .connectionAcquisitionTimeout(CONNECTION_ACQUISITION_TIMEOUT)
                .socketTimeout(SOCKET_TIMEOUT));
    }
}