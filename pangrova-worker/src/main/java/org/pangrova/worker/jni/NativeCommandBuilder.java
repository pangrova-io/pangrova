package org.pangrova.worker.jni;

import com.google.protobuf.ByteString;
import org.pangrova.worker.jni.proto.ConnectExchangeSourcePayload;
import org.pangrova.worker.jni.proto.EvictCachePayload;
import org.pangrova.worker.jni.proto.InjectDynamicFilterPayload;
import org.pangrova.worker.jni.proto.NativeCommand;
import org.pangrova.worker.jni.proto.NativeCommandResponse;
import org.pangrova.worker.jni.proto.UpdateMemoryLimitPayload;

/**
 * Builds serialized NativeCommand proto bytes for the unified command channel.
 * Uses protobuf-generated Java classes from {@code command.proto}.
 *
 * <p>This class replaces the former hand-written wire-format encoding with
 * type-safe generated builders, eliminating manual field number/wire type management.</p>
 */
public final class NativeCommandBuilder {

    private NativeCommandBuilder() {}

    /**
     * Builds a NativeCommand proto for UPDATE_MEMORY_LIMIT.
     */
    public static byte[] updateMemoryLimit(long executorHandle, long newLimitBytes) {
        byte[] payload = UpdateMemoryLimitPayload.newBuilder()
                .setNewLimitBytes(newLimitBytes)
                .build()
                .toByteArray();
        return buildCommand(executorHandle, NativeCommandType.UPDATE_MEMORY_LIMIT, payload);
    }

    /**
     * Builds a NativeCommand proto for GET_METRICS (global, handle=0).
     */
    public static byte[] getMetrics() {
        return buildCommand(0, NativeCommandType.GET_METRICS, null);
    }

    /**
     * Builds a NativeCommand proto for GET_CACHE_STATS (global, handle=0).
     */
    public static byte[] getCacheStats() {
        return buildCommand(0, NativeCommandType.GET_CACHE_STATS, null);
    }

    /**
     * Builds a NativeCommand proto for GET_CAPABILITIES (global, handle=0).
     */
    public static byte[] buildGetCapabilities() {
        return buildCommand(0, NativeCommandType.GET_CAPABILITIES, null);
    }

    /**
     * Builds a NativeCommand proto for PREFETCH_SPLITS (global, handle=0).
     */
    public static byte[] prefetchSplits(byte[] splitInfosProto) {
        return buildCommand(0, NativeCommandType.PREFETCH_SPLITS, splitInfosProto);
    }

    /**
     * Builds a NativeCommand proto for EVICT_CACHE (global, handle=0).
     */
    public static byte[] evictCache(String pattern) {
        byte[] payload = EvictCachePayload.newBuilder()
                .setPattern(pattern)
                .build()
                .toByteArray();
        return buildCommand(0, NativeCommandType.EVICT_CACHE, payload);
    }

    /**
     * Builds a NativeCommand proto for INJECT_DYNAMIC_FILTER.
     */
    public static byte[] injectDynamicFilter(long executorHandle,
                                             String planNodeId,
                                             byte[] filterProto) {
        InjectDynamicFilterPayload.Builder builder = InjectDynamicFilterPayload.newBuilder()
                .setPlanNodeId(planNodeId);
        if (filterProto != null && filterProto.length > 0) {
            builder.setFilterProto(ByteString.copyFrom(filterProto));
        }
        byte[] payload = builder.build().toByteArray();
        return buildCommand(executorHandle, NativeCommandType.INJECT_DYNAMIC_FILTER, payload);
    }

    /**
     * Extracts the result_payload field (field 3) from a NativeCommandResponse.
     * Returns null if the field is not present or empty.
     */
    public static byte[] extractResultPayload(byte[] responseBytes) {
        if (responseBytes == null || responseBytes.length == 0) {
            return null;
        }
        try {
            NativeCommandResponse response = NativeCommandResponse.parseFrom(responseBytes);
            ByteString resultPayload = response.getResultPayload();
            if (resultPayload.isEmpty()) {
                return null;
            }
            return resultPayload.toByteArray();
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to parse NativeCommandResponse", e);
        }
    }

    // --- Command envelope builder ---

    /**
     * Builds a NativeCommand proto envelope wrapping an optional payload.
     * Public for reuse by transport classes.
     */
    public static byte[] buildCommand(long executorHandle, int commandType,
                                       byte[] payload) {
        NativeCommand.Builder builder = NativeCommand.newBuilder()
                .setExecutorHandle(executorHandle)
                .setCommandTypeValue(commandType);
        if (payload != null && payload.length > 0) {
            builder.setPayload(ByteString.copyFrom(payload));
        }
        return builder.build().toByteArray();
    }

    // --- Payload builders for transport classes (public API preserved) ---

    /**
     * Builds a ConnectExchangeSourcePayload proto.
     */
    public static byte[] buildConnectExchangeSourcePayload(
            String sourceId, String host, int port,
            String queryId, int stageId, int partitionId) {
        return ConnectExchangeSourcePayload.newBuilder()
                .setSourceId(sourceId)
                .setHost(host)
                .setPort(port)
                .setQueryId(queryId)
                .setStageId(stageId)
                .setPartitionId(partitionId)
                .build()
                .toByteArray();
    }
}
