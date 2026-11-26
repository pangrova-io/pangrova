package org.pangrova.query.profiling.enums;

/** A measured phase of Coordinator stage dispatch. */
public enum DispatchPhase {
    SERIALIZE_PLAN, ENUMERATE_SPLITS, VALIDATE_AND_BIND_PLAN, SUBMIT_TASKS, OBSERVE_COHORT_ACK
}
