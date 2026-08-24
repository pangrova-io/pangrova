package org.pangrova.coordinator.query.execution.dispatch.adaptive;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.pangrova.planner.runtimefilter.RuntimeFilter;
import org.pangrova.planner.runtimefilter.RuntimeFilterRegistry;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Binds runtime filters from {@link RuntimeFilterRegistry} to target stages
 * by converting {@link RuntimeFilter} instances into {@link DynamicFilterPayload}
 * protocol messages for Worker delivery.
 *
 * <p>BLOOM filters are serialized via
 * {@code SplitBlockBloomFilter.toByteArray()} (custom binary format).</p>
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
                filter.column(), 1);
    }

    private static byte[] serializeFilterData(RuntimeFilter filter) {
        if (filter.type() == RuntimeFilter.FilterType.MIN_MAX) {
            return serializeMinMax(filter);
        }
        return serializeBloom(filter);
    }

    private static byte[] serializeMinMax(RuntimeFilter filter) {
        String column = filter.column() != null ? filter.column() : "";
        String min = filter.rawMin() != null ? filter.rawMin().toString() : "";
        String max = filter.rawMax() != null ? filter.rawMax().toString() : "";
        String encoded = column + "|" + min + "|" + max;
        return encoded.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] serializeBloom(RuntimeFilter filter) {
        if (filter.bloomFilter() == null) {
            return new byte[0];
        }
        return filter.bloomFilter().toByteArray();
    }
}
