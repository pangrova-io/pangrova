package org.pangrova.connector.paimon.handle;

/**
 * Exact table-owned options that determine how Native interprets persisted retract records.
 *
 * @param ignoreDelete whether the table ignores persisted retract records
 * @param removeRecordOnDelete whether DELETE resets the partial-update or aggregation accumulator
 */
public record PaimonMergePolicy(boolean ignoreDelete, boolean removeRecordOnDelete) {}
