package org.pangrova.worker.execution.filter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.pangrova.common.bloom.SplitBlockBloomFilter;
import org.pangrova.protocol.rest.filter.DynamicFilterPayload;
import org.pangrova.protocol.v2.RuntimeFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies received {@link DynamicFilterPayload} filters to split partition values
 * before splits are passed to the native execution engine.
 *
 * <p>Supports two filter types:</p>
 * <ul>
 *   <li><b>MIN_MAX</b>: prunes splits whose partition value falls outside [min, max]</li>
 *   <li><b>BLOOM</b>: prunes splits whose partition value is definitely not in the SBBF set</li>
 * </ul>
 *
 * <p>Thread safety: stateless — safe to share across threads.</p>
 */
public final class DynamicFilterApplicator {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicFilterApplicator.class);

    /**
     * Result of applying dynamic filters to a set of splits.
     *
     * @param retainedSplits splits that passed all filters
     * @param totalSplits    original split count before filtering
     * @param filteredSplits number of splits pruned by filters
     */
    public record FilterResult(
            List<String> retainedSplits,
            int totalSplits,
            int filteredSplits
    ) {
        /** Freezes the retained split membership for this filtering decision. */
        public FilterResult {
            retainedSplits = List.copyOf(retainedSplits);
        }
    }

    /**
     * Applies all dynamic filters to the given encoded splits.
     *
     * @param encodedSplits Base64-encoded SplitInfo proto bytes
     * @param filters       dynamic filter payloads from Coordinator
     * @param stageId       current stage id for logging
     * @param queryId       current query id for logging
     * @return filter result with retained splits and pruning metrics
     */
    public FilterResult apply(List<String> encodedSplits,
                              List<DynamicFilterPayload> filters,
                              int stageId, String queryId) {
        if (filters == null || filters.isEmpty() || encodedSplits == null || encodedSplits.isEmpty()) {
            return new FilterResult(
                    encodedSplits == null ? List.of() : encodedSplits,
                    encodedSplits == null ? 0 : encodedSplits.size(),
                    0);
        }

        List<ParsedFilter> parsedFilters = parseFilters(filters);
        if (parsedFilters.isEmpty()) {
            return new FilterResult(encodedSplits, encodedSplits.size(), 0);
        }

        List<String> retained = filterSplits(encodedSplits, parsedFilters);
        int filtered = encodedSplits.size() - retained.size();
        LOG.info("event=dpp_filter_applied queryId={} stageId={} totalSplits={} filteredSplits={} retainedSplits={}",
                queryId, stageId, encodedSplits.size(), filtered, retained.size());
        return new FilterResult(List.copyOf(retained), encodedSplits.size(), filtered);
    }

    private List<String> filterSplits(List<String> encodedSplits, List<ParsedFilter> parsedFilters) {
        List<String> retained = new ArrayList<>();
        for (String encoded : encodedSplits) {
            Map<String, String> partitionValues = SplitPartitionExtractor.extractPartitionValues(encoded);
            if (passesAllFilters(partitionValues, parsedFilters)) {
                retained.add(encoded);
            }
        }
        return retained;
    }

    private List<ParsedFilter> parseFilters(List<DynamicFilterPayload> filters) {
        List<ParsedFilter> parsed = new ArrayList<>();
        for (DynamicFilterPayload payload : filters) {
            parsed.add(ParsedFilter.from(payload));
        }
        return parsed;
    }

    private boolean passesAllFilters(Map<String, String> partitionValues,
                                     List<ParsedFilter> filters) {
        for (ParsedFilter filter : filters) {
            if (!filter.matches(partitionValues)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Internal parsed representation of a dynamic filter for efficient evaluation.
     */
    sealed interface ParsedFilter {

        boolean matches(Map<String, String> partitionValues);

        static ParsedFilter from(DynamicFilterPayload payload) {
            RuntimeFilter entry = TypedDynamicFilter.parseSingle(payload);
            return switch (entry.getPredicateCase()) {
                case RANGE -> new RangeFilter(entry);
                case BLOOM -> SbbfFilter.parse(payload, entry);
                default -> throw new IllegalArgumentException(
                        "Unsupported dynamic filter predicate: "
                                + entry.getPredicateCase());
            };
        }
    }

    /**
     * MIN_MAX filter: prunes splits whose partition value is outside [min, max].
     */
    record RangeFilter(RuntimeFilter entry) implements ParsedFilter {

        @Override
        public boolean matches(Map<String, String> partitionValues) {
            return TypedDynamicFilter.rangeMatches(
                    entry, partitionValues.get(entry.getColumnName()));
        }
    }

    /**
     * SBBF filter: prunes splits whose partition value is definitely not in the
     * Split Block Bloom Filter set. Deserializes from SBBF binary format.
     */
    record SbbfFilter(String filterId, String column,
                      SplitBlockBloomFilter sbbf,
                      RuntimeFilter entry) implements ParsedFilter {

        static SbbfFilter parse(DynamicFilterPayload payload, RuntimeFilter entry) {
            return new SbbfFilter(payload.filterId(), entry.getColumnName(),
                    TypedDynamicFilter.decodeBloom(entry), entry);
        }

        @Override
        public boolean matches(Map<String, String> partitionValues) {
            String value = partitionValues.get(column);
            if (value == null) {
                return true;
            }
            try {
                return sbbf.mightContainBytes(TypedDynamicFilter.encodeBloomProbe(entry, value));
            } catch (NumberFormatException failure) {
                return true;
            }
        }
    }
}
