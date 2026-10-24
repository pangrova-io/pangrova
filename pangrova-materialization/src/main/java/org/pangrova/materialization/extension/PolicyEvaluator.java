package org.pangrova.materialization.extension;

import java.util.Set;
import org.pangrova.materialization.domain.PolicySnapshot;
import org.pangrova.materialization.domain.Principal;
import org.pangrova.materialization.domain.SourceVector;
import org.pangrova.spi.materialization.SourceRef;

/**
 * Connector-aware source policy capture and drift detection.
 *
 * <p>A materialization runs as its definer, so the definer's row filters and
 * column masks determine the materialized rows. The evaluator captures those
 * policies at planning time and proves they did not change before publication.
 */
public interface PolicyEvaluator {

    /**
     * Returns a stable evaluator identifier.
     *
     * @return evaluator identifier
     */
    String evaluatorId();

    /**
     * Returns whether this evaluator handles the source.
     *
     * @param source source table
     * @return true when the evaluator can capture the source policy
     */
    boolean supports(SourceRef source);

    /**
     * Captures policy fingerprints for the sources supported by this evaluator.
     *
     * @param definer persisted definer
     * @param sources sources to capture
     * @return policy snapshot
     */
    PolicySnapshot capture(Principal definer, Set<SourceRef> sources);

    /**
     * Requires that observed source snapshots still carry the planned policies.
     *
     * @param planned policy snapshot captured at planning time
     * @param observed source snapshots observed at execution or publication
     */
    void requireUnchanged(PolicySnapshot planned, SourceVector observed);
}
