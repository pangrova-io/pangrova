package org.pangrova.ai.worker.governance.observer;

import org.pangrova.spi.ai.error.AiInvocationException;
import org.pangrova.spi.ai.invocation.AiInvocationRequest;
import org.pangrova.spi.ai.invocation.AiInvocationResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Structured terminal audit record emitted for every production AI provider call. */
public final class LoggingAiInvocationObserver implements AiInvocationObserver {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingAiInvocationObserver.class);

    @Override
    public void onSuccess(AiInvocationRequest request, AiInvocationResult result) {
        LOG.info("event=ai_invocation_completed requestId={} tenantId={} principal={}"
                        + " queryId={} taskId={} traceId={} model={} modelVersion={}"
                        + " modelSignature={} capabilityId={} capabilityVersion={} operation={}"
                        + " providerRequestId={} finishReason={}",
                request.requestId(), request.tenantId(), request.principal(),
                request.queryId(), attribute(request, "task_id"), request.traceId(),
                request.modelName(), request.modelVersion(),
                java.util.Base64.getEncoder().encodeToString(
                        request.modelSignatureSha256()),
                attribute(request, "capability_id"), request.capabilityVersion(),
                request.operation(),
                result.providerRequestId(),
                result.finishReason());
    }

    @Override
    public void onFailure(AiInvocationRequest request, AiInvocationException failure) {
        LOG.warn("event=ai_invocation_failed requestId={} tenantId={} principal={}"
                        + " queryId={} taskId={} traceId={} model={} modelVersion={}"
                        + " modelSignature={} capabilityId={} capabilityVersion={} operation={}"
                        + " category={} retryable={} error={}",
                request.requestId(), request.tenantId(), request.principal(),
                request.queryId(), attribute(request, "task_id"), request.traceId(),
                request.modelName(), request.modelVersion(),
                java.util.Base64.getEncoder().encodeToString(
                        request.modelSignatureSha256()),
                attribute(request, "capability_id"), request.capabilityVersion(),
                request.operation(), failure.category(),
                failure.retryable(), failure.getMessage());
    }

    private static String attribute(AiInvocationRequest request, String name) {
        return request.attributes().getOrDefault(name, "");
    }
}
