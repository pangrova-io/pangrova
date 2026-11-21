package org.pangrova.coordination.api.enums;

/** Result of an exact-incarnation heartbeat, after the registry's atomic ownership check. */
public enum WorkerHeartbeatStatus {
    ACCEPTED,
    /** Registration is absent; conditional repair may insert it, never replace another owner. */
    REGISTRATION_REQUIRED,
    /** Another process owns this logical ID. The caller must permanently stop new admission. */
    FENCED
}
