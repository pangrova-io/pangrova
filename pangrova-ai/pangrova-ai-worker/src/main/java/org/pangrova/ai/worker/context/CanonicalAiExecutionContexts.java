package org.pangrova.ai.worker.context;

import com.google.protobuf.InvalidProtocolBufferException;
import org.pangrova.protocol.task.CanonicalTaskRequests;
import org.pangrova.protocol.v2.ExecutionCallbackContext;
import org.pangrova.ai.api.error.AiInvocationException;

/** Maps the validated canonical authority without a second schema or identity decoder. */
final class CanonicalAiExecutionContexts {
    private CanonicalAiExecutionContexts() {}

    static AiExecutionContext decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw invalidAuthority("Native callback task authority is absent", null);
        }
        try {
            var callback = ExecutionCallbackContext.parseFrom(bytes);
            CanonicalTaskRequests.validateCallback(callback);
            return map(callback);
        } catch (InvalidProtocolBufferException | IllegalArgumentException failure) {
            throw invalidAuthority("Native callback task authority is invalid: " + failure.getMessage(), failure);
        }
    }

    private static AiExecutionContext map(ExecutionCallbackContext callback) {
        var security = callback.getSecurity();
        var attempt = callback.getAttempt();
        var grant = callback.getResourceGrant();
        return new AiExecutionContext(security.getTenantId(), security.getPrincipal(),
                attempt.getQueryOrJobId(), attempt.getTaskId(), callback.getTraceContext().getTraceId(),
                callback.getDeadlineEpochMs(), grant.getProviderConcurrency(), grant.getProviderItems(),
                grant.getProviderAttempts(), grant.getProviderRequestBytes(), grant.getProviderResponseBytes(),
                grant.getMediaBytes(), attempt);
    }

    private static AiInvocationException invalidAuthority(String message, Throwable cause) {
        return new AiInvocationException(AiInvocationException.Category.AUTHORIZATION, false, message, cause);
    }
}
