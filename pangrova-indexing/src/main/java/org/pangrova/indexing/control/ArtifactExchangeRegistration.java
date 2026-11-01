package org.pangrova.indexing.control;

import org.pangrova.common.util.DigestUtils;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/** Immutable external allocation identity. This contract deliberately contains no access capabilities. */
public record ArtifactExchangeRegistration(
        String exchangeId, String executionId, String shuffleId, int producerStageId,
        long planEpoch, String graphSha256, String stagePlanSha256, String sourceSnapshot,
        int producerCount, String producerSetSha256, int partitionCount,
        List<Endpoint> endpoints, Address commitAddress, ArtifactExecutionOwner registeredOwner) {
    private static final int MAX_PORT = 65535;

    public ArtifactExchangeRegistration {
        requireText(exchangeId);
        requireText(executionId);
        requireText(shuffleId);
        requireText(sourceSnapshot);
        requireSha256(graphSha256);
        requireSha256(stagePlanSha256);
        requireSha256(producerSetSha256);
        Objects.requireNonNull(commitAddress, "commitAddress");
        Objects.requireNonNull(registeredOwner, "registeredOwner");
        if (producerStageId < 0 || planEpoch <= 0 || producerCount <= 0 || partitionCount <= 0) {
            throw new IllegalArgumentException("Invalid durable exchange cohort");
        }
        endpoints = endpoints.stream().sorted(Comparator.comparingInt(Endpoint::partitionStart)).toList();
        int cursor = 0;
        for (var endpoint : endpoints) {
            if (endpoint.partitionStart() != cursor || endpoint.partitionEnd() > partitionCount) {
                throw new IllegalArgumentException("Exchange endpoint coverage is not exact");
            }
            cursor = endpoint.partitionEnd();
        }
        if (cursor != partitionCount) throw new IllegalArgumentException("Exchange endpoint coverage is incomplete");
    }

    public record Address(String host, int port, String tlsIdentity) {
        public Address {
            requireText(host);
            Objects.requireNonNull(tlsIdentity, "tlsIdentity");
            if (port <= 0 || port > MAX_PORT) throw new IllegalArgumentException("Invalid exchange port");
        }
    }

    public record Endpoint(Address address, int partitionStart, int partitionEnd) {
        public Endpoint {
            Objects.requireNonNull(address, "address");
            if (partitionStart < 0 || partitionEnd <= partitionStart) {
                throw new IllegalArgumentException("Invalid exchange endpoint range");
            }
        }
    }

    public byte[] canonicalBytes() { return encode(this); }
    public String sha256() { return DigestUtils.sha256Hex(canonicalBytes()); }

    public static ArtifactExchangeRegistration decode(byte[] bytes, String sha256) {
        return decode(bytes, sha256, ArtifactExchangeRegistration.class);
    }

    static byte[] encode(Object value) {
        try {
            return IndexJsonMapper.writeValueAsBytes(value);
        } catch (com.fasterxml.jackson.core.JsonProcessingException failure) {
            throw new IllegalStateException("Cannot encode durable exchange evidence", failure);
        }
    }

    static <T> T decode(byte[] bytes, String sha256, Class<T> type) {
        requireSha256(sha256);
        if (bytes == null || bytes.length == 0 || bytes.length > 16 * 1024 * 1024
                || !DigestUtils.matchesSha256(bytes, sha256)) {
            throw new IllegalArgumentException("Durable exchange evidence checksum or size is invalid");
        }
        try {
            T value = IndexJsonMapper.readValue(bytes, type);
            if (!java.util.Arrays.equals(bytes, encode(value))) {
                throw new IllegalArgumentException("Durable exchange evidence is not canonical");
            }
            return value;
        } catch (java.io.IOException failure) {
            throw new IllegalArgumentException("Durable exchange evidence is invalid", failure);
        }
    }

    static void requireText(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Exchange identity is required");
    }

    static void requireSha256(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Canonical exchange SHA-256 is required");
        }
    }
}
