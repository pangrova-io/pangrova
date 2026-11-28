package org.pangrova.worker.jni;

import org.pangrova.protocol.descriptor.CanonicalProtocolIdentity;

import com.google.protobuf.Any;
import com.google.protobuf.Message;
import org.pangrova.protocol.function.catalog.FunctionCatalogStartupContracts;
import org.pangrova.protocol.v2.EvictCacheRequest;
import org.pangrova.protocol.task.TaskRuntimeFilterWire;
import org.pangrova.protocol.v2.RuntimeFilterBinding;
import org.pangrova.protocol.v2.PrefetchArtifactsRequest;
import org.pangrova.protocol.v2.ProtocolVersion;
import org.pangrova.protocol.v2.RuntimeCommand;
import org.pangrova.protocol.v2.RuntimeCommandResponse;
import org.pangrova.protocol.v2.RuntimeCommandType;
import org.pangrova.protocol.v2.StageResourceGrant;
import org.pangrova.protocol.v2.TaskAttemptId;
import org.pangrova.protocol.v2.UpdateResourceGrantRequest;

/** Builds the canonical v2 JVM-to-Native control protocol. */
public final class NativeCommandBuilder {

    private static final ProtocolVersion PROTOCOL = CanonicalProtocolIdentity.current();

    private NativeCommandBuilder() {}

    /** Builds a resource-grant update for the native runtime. */
    public static byte[] updateMemoryLimit(long newLimitBytes) {
        if (newLimitBytes <= 0) {
            throw new IllegalArgumentException("newLimitBytes must be positive");
        }
        UpdateResourceGrantRequest payload = UpdateResourceGrantRequest.newBuilder()
                .setResourceGrant(StageResourceGrant.newBuilder()
                        .setNativeMemoryBytes(newLimitBytes))
                .build();
        return buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_UPDATE_RESOURCE_GRANT,
                payload);
    }

    /** Builds a metrics snapshot request. */
    public static byte[] getMetrics() {
        return buildCommand(RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_METRICS, null);
    }

    /** Builds a native capability inventory request. */
    public static byte[] buildGetCapabilities() {
        return buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_CAPABILITIES, null);
    }

    /** Freezes actual startup factories exactly once before any task is accepted. */
    public static byte[] finalizeFunctionCatalog() {
        return buildCommand(RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINALIZE_FUNCTION_CATALOG,
                FunctionCatalogStartupContracts.request());
    }

    /** Builds an artifact prefetch command from a validated request. */
    public static byte[] prefetchArtifacts(PrefetchArtifactsRequest request) {
        if (request == null) {
            throw new IllegalArgumentException("request must not be null");
        }
        return buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_PREFETCH_ARTIFACTS,
                request);
    }

    /** Builds a cache eviction command for one non-blank pattern. */
    public static byte[] evictCache(String pattern) {
        if (pattern == null || pattern.isBlank()) {
            throw new IllegalArgumentException("pattern must not be blank");
        }
        return buildCommand(
                RuntimeCommandType.RUNTIME_COMMAND_TYPE_EVICT_CACHE,
                EvictCacheRequest.newBuilder().setPattern(pattern).build());
    }

    /** Wraps the decoded runtime-filter update in a Native command. */
    public static byte[] injectRuntimeFilter(RuntimeFilterBinding update) {
        if (update == null || !TaskRuntimeFilterWire.hasScanTarget(update) || update.getFiltersCount() == 0) {
            throw new IllegalArgumentException("Runtime filter update must include a scan node and filters");
        }
        return buildCommand(RuntimeCommandType.RUNTIME_COMMAND_TYPE_INJECT_RUNTIME_FILTER, update);
    }

    /** Returns the serialized concrete message carried by response.payload. */
    public static byte[] extractResultPayload(byte[] responseBytes) {
        RuntimeCommandResponse response = requireSuccess(responseBytes, "native command");
        return response.hasPayload()
                ? response.getPayload().getValue().toByteArray()
                : null;
    }

    /** Parses a command response and fails closed on malformed/negative acknowledgements. */
    public static RuntimeCommandResponse requireSuccess(
            byte[] responseBytes, String operation) {
        if (responseBytes == null || responseBytes.length == 0) {
            throw new IllegalStateException(
                    operation + " returned an empty RuntimeCommandResponse");
        }
        try {
            RuntimeCommandResponse response =
                    RuntimeCommandResponse.parseFrom(responseBytes);
            if (!response.getSuccess()) {
                String message = response.hasError()
                        ? response.getError().getMessage()
                        : "unspecified Native command failure";
                throw new IllegalStateException(operation + " failed: " + message);
            }
            return response;
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException(
                    operation + " returned a malformed RuntimeCommandResponse", e);
        }
    }

    /** Builds a v2 envelope with a type-safe protobuf Any payload. */
    public static byte[] buildCommand(
            RuntimeCommandType commandType, Message payload) {
        validateCommandType(commandType);
        if (requiresAttempt(commandType)) {
            throw new IllegalArgumentException(
                    "task-scoped command requires an attempt identity: " + commandType);
        }
        return buildEnvelope(commandType, null, payload);
    }

    /** Builds a task-scoped v2 envelope with an exact fenced attempt identity. */
    public static byte[] buildCommand(
            RuntimeCommandType commandType,
            TaskAttemptId attempt,
            Message payload) {
        validateCommandType(commandType);
        requireAttempt(attempt);
        return buildEnvelope(commandType, attempt, payload);
    }

    /** Embeds the already admitted direct source and consumer identity in the Native command. */
    public static byte[] connectDirectExchange(TaskAttemptId attempt,
            org.pangrova.protocol.v2.ExchangeInputBinding input,
            org.pangrova.protocol.v2.ExchangePartitionSource source) {
        return buildCommand(RuntimeCommandType.RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE, attempt,
                org.pangrova.protocol.v2.ConnectExchangeSourceRequest.newBuilder()
                        .setPlanNodeId(input.getPlanNodeId()).setExchangeId(input.getExchangeId())
                        .setDirect(source).build());
    }

    private static byte[] buildEnvelope(
            RuntimeCommandType commandType,
            TaskAttemptId attempt,
            Message payload) {
        RuntimeCommand.Builder builder = RuntimeCommand.newBuilder()
                .setProtocol(PROTOCOL)
                .setCommandType(commandType);
        if (attempt != null) {
            builder.setAttempt(attempt);
        }
        if (payload != null) {
            builder.setPayload(Any.pack(payload));
        }
        return builder.build().toByteArray();
    }

    private static void validateCommandType(RuntimeCommandType commandType) {
        if (commandType == null
                || commandType == RuntimeCommandType.RUNTIME_COMMAND_TYPE_UNSPECIFIED
                || commandType == RuntimeCommandType.UNRECOGNIZED) {
            throw new IllegalArgumentException(
                    "commandType must be a recognized non-zero v2 command");
        }
    }

    private static boolean requiresAttempt(RuntimeCommandType type) {
        return type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_CONNECT_EXCHANGE_SOURCE
                || type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_NO_MORE_EXCHANGE_SOURCES
                || type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_PUSH_EXCHANGE_BATCH
                || type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_FINISH_EXCHANGE_SOURCE
                || type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_GET_RESOURCE_DRAIN_EVIDENCE
                || type == RuntimeCommandType.RUNTIME_COMMAND_TYPE_CANCEL_ATTEMPT;
    }

    private static void requireAttempt(TaskAttemptId attempt) {
        if (attempt == null
                || attempt.getQueryOrJobId().isBlank()
                || attempt.getTaskId().isBlank()
                || attempt.getPlanEpoch() == 0
                || attempt.getFencingToken() == 0
                || attempt.getControlEpoch() == 0
                || !attempt.hasExecutionOwner()) {
            throw new IllegalArgumentException(
                    "task-scoped command attempt identity is incomplete");
        }
    }
}
