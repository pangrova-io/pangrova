package org.pangrova.materialization.api.enums;

/** Definition shape that can be recomputed one partition at a time. */
public enum PartitionAlignmentShape {
    /** Single-table projection/filter scan. */
    SCAN,
    /** Single-table aggregate whose grouping keys are the partition columns. */
    AGGREGATE,
    /** Shape that cannot be rewritten per partition. */
    UNSUPPORTED
}
