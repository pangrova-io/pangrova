package org.pangrova.worker.result;

import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ResultReadGrant;
import org.pangrova.protocol.v2.ResultReadScope;
import org.pangrova.protocol.v2.TaskRequest;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HexFormat;

/** Issues scoped result authority using the same producer-local transport key. */
public final class ResultReadTickets {
    private static final String DOMAIN = "pangrova.result.read.v1:";
    private final byte[] key;

    public ResultReadTickets(byte[] key) {
        if (key == null || key.length < 32) {
            throw new IllegalArgumentException("Result tickets require a producer-local 256-bit key");
        }
        this.key = key.clone();
    }

    public org.pangrova.protocol.rest.query.ResultEndpoint endpoint(
            TaskRequest request, String host, int port, String protocol) {
        return new org.pangrova.protocol.rest.query.ResultEndpoint(host, port, issue(request), protocol);
    }

    /** A completed task remains readable only while its exact live registration is retained. */
    public String issue(TaskRequest request) {
        CanonicalTaskRequests.validateForAdmission(request);
        if (!request.getExecutionOptions().getDirectResultOutput()
                || request.getDeadlineEpochMs() <= System.currentTimeMillis()) {
            throw new IllegalArgumentException("Result ticket requires a live admitted result task deadline");
        }
        ResultReadGrant grant = ResultReadGrant.newBuilder()
                .setSecurity(request.getSecurity()).setAttempt(request.getAttempt())
                .setScope(ResultReadScope.RESULT_READ_SCOPE_RESULT_STREAM)
                .setExpiresAtEpochMs(request.getDeadlineEpochMs()).build();
        String payload = DOMAIN + Base64.getEncoder().encodeToString(grant.toByteArray());
        String message = payload + "|" + grant.getExpiresAtEpochMs();
        return message + "|" + signature(message);
    }

    private String signature(String message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return HexFormat.of().formatHex(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException failure) {
            throw new IllegalStateException("Result ticket authentication is unavailable", failure);
        }
    }
}
