package org.pangrova.indexing.api.vector;

import org.pangrova.indexing.api.enums.HybridSearchMode;
import org.pangrova.common.api.annotation.Experimental;

/**
 * Execution strategy for hybrid vector search (pre-filter + ANN).
 *
 * <p>Defined in the indexing contract artifact because the Worker needs to know the
 * execution mode, and the Worker cannot depend on {@code pangrova-planner}.</p>
 *
 * @param mode       execution mode (ANN first then filter, or filter first then brute-force)
 * @param effectiveK effective K value used during search (may differ from user-specified topK
 *                   due to over-fetching in ANN_THEN_FILTER mode)
 */
@Experimental
public record VectorHybridSearchStrategy(HybridSearchMode mode, int effectiveK) {

}
