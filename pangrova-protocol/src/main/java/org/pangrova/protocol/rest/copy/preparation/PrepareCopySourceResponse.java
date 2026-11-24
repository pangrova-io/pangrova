package org.pangrova.protocol.rest.copy.preparation;

import org.pangrova.common.util.DigestUtils;

import org.pangrova.protocol.checksum.ProtocolChecksums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.SourcePreparationResult;

import java.util.Base64;
import java.util.Objects;

/** Canonical receipt, including failure usage and actual I/O join state. */
public final class PrepareCopySourceResponse {
    private static final int MAX_TEXT_CHARS = 4096;
    private final SourcePreparationResult result;

    /** Decodes a bounded receipt and rejects incomplete or malformed source and error identities. */
    @JsonCreator
    public PrepareCopySourceResponse(@JsonProperty(value = "canonical_result", required = true) String encoded) {
        if (encoded == null || encoded.length() > 64 * 1024) {
            throw new IllegalArgumentException("Source preparation receipt exceeds its control bound");
        }
        try {
            result = SourcePreparationResult.parseFrom(Base64.getDecoder().decode(encoded));
        } catch (InvalidProtocolBufferException | IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Malformed source preparation receipt", invalid);
        }
        validateReceipt(result);
        if (result.hasSource()) validateSource(result.getSource());
        if (result.hasError()) validateError(result.getError());
    }

    private static void validateReceipt(SourcePreparationResult result) {
        if (!result.getUnknownFields().asMap().isEmpty() || !result.hasAttempt()
                || !result.hasRequestSha256() || !result.hasUsage()
                || !result.getUsage().getUnknownFields().asMap().isEmpty()
                || result.hasSource() == result.hasError()
                || result.hasSource() && (!result.getIoJoined()
                        || !result.getSource().getUnknownFields().asMap().isEmpty())
                || result.hasError() && !result.getError().getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Incomplete source preparation receipt");
        }
    }

    private static void validateError(org.pangrova.protocol.v2.ExecutionError error) {
        if (error.getErrorClass() == org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_UNSPECIFIED
                || error.getErrorClass() == org.pangrova.protocol.v2.ErrorClass.UNRECOGNIZED
                || error.getCode().isBlank() || error.getCode().length() > 256
                || error.getFailedComponent().isBlank() || error.getFailedComponent().length() > 256
                || error.getMessage().isBlank() || error.getMessage().length() > MAX_TEXT_CHARS) {
            throw new IllegalArgumentException("Invalid source preparation error identity");
        }
    }

    private static void validateSource(org.pangrova.protocol.v2.PreparedFileSource source) {
        java.net.URI uri;
        try {
            uri = java.net.URI.create(source.getManagedObjectUri());
        } catch (IllegalArgumentException invalid) {
            throw new IllegalArgumentException("Invalid prepared source URI", invalid);
        }
        if (source.getSourceVersionId().isBlank() || source.getSourceVersionId().length() > MAX_TEXT_CHARS
                || source.getContentSize() < 0 || !DigestUtils.isSha256Hex(source.getContentSha256())
                || !DigestUtils.isSha256Hex(source.getTenantScope()) || !"s3".equals(uri.getScheme())
                || uri.getHost() == null || uri.getUserInfo() != null || uri.getPort() != -1
                || uri.getRawQuery() != null || uri.getRawFragment() != null
                || uri.getRawPath() == null || uri.getRawPath().length() <= 1) {
            throw new IllegalArgumentException("Invalid prepared source identity");
        }
    }

    public static PrepareCopySourceResponse from(SourcePreparationResult result) {
        return new PrepareCopySourceResponse(Base64.getEncoder().encodeToString(
                Objects.requireNonNull(result, "result").toByteArray()));
    }

    @JsonProperty("canonical_result")
    public String encodedResult() { return Base64.getEncoder().encodeToString(result.toByteArray()); }

    /** Confirms exact issuance and grant before the Coordinator consumes proof or usage. */
    public SourcePreparationResult resultFor(PrepareCopySourceRequest request) {
        if (!result.getAttempt().equals(request.preparation().getAttempt())
                || !result.getRequestSha256().equals(ProtocolChecksums.sha256(request.canonicalBytes()))
                || !CanonicalTaskRequests.usageWithinGrant(result.getUsage(),
                        request.preparation().getResourceGrant())) {
            throw new IllegalArgumentException("Source preparation receipt differs from its exact issued request");
        }
        return result;
    }

    @Override public String toString() { return "PrepareCopySourceResponse[REDACTED]"; }
}
