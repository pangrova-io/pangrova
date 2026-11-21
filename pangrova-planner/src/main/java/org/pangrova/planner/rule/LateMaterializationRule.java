package org.pangrova.planner.rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Planner rule that ensures FILE column functions are evaluated after
 * row-level filter predicates have been applied (Late Materialization).
 *
 * <p><b>Optimization principle:</b> FILE columns are stored as lightweight STRUCT
 * metadata (uri, mime_type, size, checksum). The actual blob content resides in
 * external object storage (S3/OSS/GCS). Evaluating AI functions on FILE columns
 * (e.g., {@code ai_describe_image}, {@code ai_embed}) triggers expensive remote
 * blob reads and inference API calls.</p>
 *
 * <p>This rule enforces the following evaluation order:</p>
 * <pre>
 * Velox Scan → read STRUCT metadata (tiny I/O)
 *     ↓
 * Filter evaluation (WHERE predicates on non-FILE columns)
 *     ↓ (only qualifying rows reach here)
 * FILE function evaluation (JNI callback → governed blob store → AI API)
 *     ↓
 * Result (VARCHAR/VECTOR) returned
 * </pre>
 *
 * <p>Without this rule, the optimizer might inadvertently pull FILE functions
 * into filter subexpressions (e.g., if a computed column is used in WHERE),
 * causing blob reads for rows that would otherwise be filtered out.</p>
 *
 * <p>Stateless and thread-safe.</p>
 */
public final class LateMaterializationRule {

    private static final Logger LOG = LoggerFactory.getLogger(LateMaterializationRule.class);

    /** Known FILE-consuming function names that trigger blob content reads. */
    private static final Set<String> FILE_CONSUMING_FUNCTIONS = Set.of(
            "ai_describe_image", "ai_parse_document", "ai_transcribe",
            "ai_ocr", "ai_complete", "ai_embed");

    /**
     * Determines whether a function invocation is a FILE-consuming function
     * that requires blob content materialization.
     *
     * @param functionName SQL function name (case-insensitive)
     * @return true if the function reads blob content from FILE columns
     */
    public static boolean isFileConsumingFunction(String functionName) {
        if (functionName == null) {
            return false;
        }
        return FILE_CONSUMING_FUNCTIONS.contains(functionName.toLowerCase(Locale.ROOT));
    }

    /**
     * Returns the set of known FILE-consuming function names.
     *
     * @return unmodifiable set of function names
     */
    public static Set<String> fileConsumingFunctions() {
        return Set.copyOf(FILE_CONSUMING_FUNCTIONS);
    }

    /**
     * Analyzes a projection list and identifies expressions that should be
     * deferred until after filter evaluation (late materialization candidates).
     *
     * <p>An expression is a late materialization candidate if it:
     * <ol>
     *   <li>Contains a FILE-consuming function call, AND</li>
     *   <li>References a FILE type column as input</li>
     * </ol>
     *
     * @param projectionFunctions list of function names in the projection
     * @param fileColumns         set of column names with FILE type in the table
     * @return indices into projectionFunctions that should be deferred
     */
    public List<Integer> identifyDeferredProjections(
            List<String> projectionFunctions,
            Set<String> fileColumns) {
        if (projectionFunctions == null || projectionFunctions.isEmpty()
                || fileColumns == null || fileColumns.isEmpty()) {
            return List.of();
        }

        var deferred = new java.util.ArrayList<Integer>();
        for (int i = 0; i < projectionFunctions.size(); i++) {
            String fnName = projectionFunctions.get(i);
            if (isFileConsumingFunction(fnName)) {
                deferred.add(i);
                LOG.debug("event=late_materialization_candidate index={} function={}", i, fnName);
            }
        }
        return List.copyOf(deferred);
    }

    /**
     * Hint record attached to the physical plan node to signal that certain
     * projections should be evaluated lazily (after filtering).
     *
     * @param deferredProjectionIndices indices of projections to defer
     * @param fileColumns               FILE columns involved
     */
    public record LateMaterializationHint(
            List<Integer> deferredProjectionIndices,
            Set<String> fileColumns) {

        public LateMaterializationHint {
            deferredProjectionIndices = List.copyOf(deferredProjectionIndices);
            fileColumns = Set.copyOf(fileColumns);
        }

        /** Returns true if any projections are deferred. */
        public boolean hasDeferred() {
            return !deferredProjectionIndices.isEmpty();
        }
    }
}
