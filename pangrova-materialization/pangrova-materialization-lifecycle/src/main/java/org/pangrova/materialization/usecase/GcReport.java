package org.pangrova.materialization.usecase;

/**
 * Outcome counters of one garbage-collection pass.
 *
 * @param examined eligible generations inspected
 * @param collected generations whose physical table is gone and state advanced
 * @param skipped generations that became protected between listing and collection
 * @param failed generations whose physical drop could not be proven
 */
public record GcReport(int examined, int collected, int skipped, int failed) {

    public GcReport {
        if (examined < 0 || collected < 0 || skipped < 0 || failed < 0) {
            throw new IllegalArgumentException("GC counters must not be negative");
        }
    }
}
