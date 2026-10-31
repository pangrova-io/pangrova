package org.pangrova.materialization.usecase;

/**
 * Outcome counters of one reconciliation pass.
 *
 * @param examined unsettled attempts inspected
 * @param adopted attempts whose exact commit was recovered and published
 * @param superseded attempts superseded by a newer head or definition
 * @param abandoned attempts proven uncommitted
 * @param quarantined attempts with an unprovable outcome
 * @param failed attempts that could not be reconciled in this pass
 */
public record ReconcileReport(
        int examined,
        int adopted,
        int superseded,
        int abandoned,
        int quarantined,
        int failed) {

    public ReconcileReport {
        if (examined < 0 || adopted < 0 || superseded < 0 || abandoned < 0
                || quarantined < 0 || failed < 0) {
            throw new IllegalArgumentException("reconcile counters must not be negative");
        }
    }
}
