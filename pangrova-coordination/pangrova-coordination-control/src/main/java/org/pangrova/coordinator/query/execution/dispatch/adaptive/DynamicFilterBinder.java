package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import java.util.List;
import java.util.Optional;

import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.BloomFilter;
import org.pangrova.protocol.v2.InjectRuntimeFilterRequest;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.RangeFilter;
import org.pangrova.protocol.v2.RuntimeFilterScope;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.protocol.v2.ValueEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds runtime filters from {@link RuntimeFilterRegistry} to target stages
 * by converting {@link RuntimeFilter} instances into {@link DynamicFilterPayload}
 * protocol messages for Worker delivery.
 *
 * <p>The REST envelope carries a canonical typed protobuf payload for both DPP
 * and Native row filtering. SBBF bytes are nested in that protobuf with an
 * explicit value encoding; no consumer infers a type from strings.</p>
 */
public final class DynamicFilterBinder {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicFilterBinder.class);

    private DynamicFilterBinder() {}

    /**
     * Resolves dynamic filters for a given stage from the registry.
     *
     * @param stageId  the target stage identifier (used as registry key)
     * @param queryId  query identifier for structured logging
     * @param registry the runtime filter registry (nullable — DPP disabled when null)
     * @return list of serialized filter payloads (empty if no filters or DPP disabled)
     */
    public static List<DynamicFilterPayload> resolveFilters(
            String stageId, String queryId, RuntimeFilterRegistry registry) {
        if (registry == null) {
            return List.of();
        }
        Optional<RuntimeFilter> filterOpt = registry.getFilter(stageId);
        if (filterOpt.isEmpty()) {
            return List.of();
        }
        RuntimeFilter filter = filterOpt.get();
        DynamicFilterPayload payload = toPayload(stageId, filter);
        LOG.info("event=dpp_filter_bound queryId={} stageId={} filterId={} filterType={}",
                queryId, stageId, payload.filterId(), payload.filterType());
        return List.of(payload);
    }

    /**
     * Converts a {@link RuntimeFilter} into a {@link DynamicFilterPayload} protocol message.
     */
    public static DynamicFilterPayload toPayload(String stageId, RuntimeFilter filter) {
        String filterType = filter.type() == RuntimeFilter.FilterType.BLOOM ? "BLOOM" : "MIN_MAX";
        byte[] filterData = serializeFilterData(filter);
        String filterId = "dpp-" + stageId;
        return new DynamicFilterPayload(filterId, filterType, filterData, stageId,
                filter.column(), 2);
    }

    private static byte[] serializeFilterData(RuntimeFilter filter) {
        org.pangrova.protocol.v2.RuntimeFilter entry = filter.type() == RuntimeFilter.FilterType.MIN_MAX
                ? minMaxEntry(filter)
                : bloomEntry(filter);
        return InjectRuntimeFilterRequest.newBuilder()
                .addFilters(entry)
                .build()
                .toByteArray();
    }

    private static org.pangrova.protocol.v2.RuntimeFilter minMaxEntry(RuntimeFilter filter) {
        if (filter.rawMin() == null || filter.rawMax() == null) {
            throw new IllegalArgumentException("MIN_MAX filter bounds must not be null");
        }
        var type = filter.valueType();
        org.pangrova.protocol.v2.RuntimeFilter.Builder entry =
                org.pangrova.protocol.v2.RuntimeFilter.newBuilder()
                        .setColumnName(filter.column());
        RangeFilter.Builder range = RangeFilter.newBuilder()
                .setLowerInclusive(true)
                .setUpperInclusive(true);
        switch (type) {
            case BIGINT, INTEGER, SMALLINT, TINYINT -> {
                entry.setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH);
                range.setLower(typedInt(type.name(), ((Number) filter.rawMin()).longValue()))
                        .setUpper(typedInt(type.name(), ((Number) filter.rawMax()).longValue()));
            }
            case DOUBLE, FLOAT -> {
                entry.setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH);
                range.setLower(typedDouble(type.name(), ((Number) filter.rawMin()).doubleValue()))
                        .setUpper(typedDouble(type.name(), ((Number) filter.rawMax()).doubleValue()));
            }
            case DECIMAL -> {
                entry.setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_PARTITION_PRUNING);
                range.setLower(typedString(type.name(), filter.rawMin().toString()))
                        .setUpper(typedString(type.name(), filter.rawMax().toString()));
            }
            case VARCHAR, DATE, TIME, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_NS, UUID -> {
                entry.setScope(type == org.pangrova.types.enums.PangrovaType.VARCHAR
                        ? RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH
                        : RuntimeFilterScope.RUNTIME_FILTER_SCOPE_PARTITION_PRUNING);
                range.setLower(typedString(type.name(), filter.rawMin().toString()))
                        .setUpper(typedString(type.name(), filter.rawMax().toString()));
            }
            default -> throw new IllegalArgumentException(
                    "Unsupported runtime range filter type: " + filter.valueType());
        }
        return entry.setRange(range).build();
    }

    private static org.pangrova.protocol.v2.RuntimeFilter bloomEntry(RuntimeFilter filter) {
        if (filter.bloomFilter() == null) {
            throw new IllegalArgumentException("BLOOM filter data must not be null");
        }
        ValueEncoding encoding = switch (filter.valueType()) {
            case BIGINT, INTEGER, SMALLINT, TINYINT ->
                    ValueEncoding.VALUE_ENCODING_INT64_LE;
            case DOUBLE, FLOAT ->
                    ValueEncoding.VALUE_ENCODING_DOUBLE_LE;
            case DECIMAL ->
                    ValueEncoding.VALUE_ENCODING_DECIMAL_UNSCALED;
            case VARCHAR -> ValueEncoding.VALUE_ENCODING_UTF8;
            default -> throw new IllegalArgumentException(
                    "Unsupported runtime bloom filter type: " + filter.valueType());
        };
        return org.pangrova.protocol.v2.RuntimeFilter.newBuilder()
                .setColumnName(filter.column())
                .setScope(RuntimeFilterScope.RUNTIME_FILTER_SCOPE_BOTH)
                .setBloom(BloomFilter.newBuilder()
                        .setAlgorithm("SBBF")
                        .setSerializedFilter(com.google.protobuf.ByteString.copyFrom(
                                filter.bloomFilter().toByteArray()))
                        .setProbeType(LogicalType.newBuilder()
                                .setCanonicalSql(filter.valueType().name()))
                        .setValueEncoding(encoding))
                .build();
    }

    private static TypedValue typedInt(String sqlType, long value) {
        return TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql(sqlType))
                .setInt64Value(value)
                .build();
    }

    private static TypedValue typedDouble(String sqlType, double value) {
        return TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql(sqlType))
                .setDoubleValue(value)
                .build();
    }

    private static TypedValue typedString(String sqlType, String value) {
        return TypedValue.newBuilder()
                .setType(LogicalType.newBuilder().setCanonicalSql(sqlType))
                .setStringValue(value)
                .build();
    }
}
