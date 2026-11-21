package org.pangrova.indexing.api.evaluation;

import org.pangrova.common.api.annotation.Experimental;
import org.pangrova.connector.api.index.NativeIndexCapability;
import org.pangrova.connector.api.index.NativeIndexEvaluator;

import java.util.List;

/** Catalog lookups scoped to the authenticated scan. Access denials propagate to the query. */
@Experimental
public interface CatalogIndexEvaluation {
    /** Borrows the catalog evaluator, or returns null when the connector offers none. */
    NativeIndexEvaluator resolveNativeIndexEvaluator(String catalogName);

    /** Returns the evaluation operations offered by the scoped catalog. */
    List<NativeIndexCapability> resolveNativeCapabilities(String catalogName);
}
