package org.pangrova.metadata.core.control;

/** Terminal or in-progress state of one maintenance run. */
public enum MaintenanceRunState {
    /** The run is executing. */
    RUNNING,
    /** The connector reported success. */
    SUCCEEDED,
    /** The connector or the control plane rejected the run. */
    FAILED,
    /** The run was abandoned before executing (busy lease, active writer, policy). */
    SKIPPED
}
