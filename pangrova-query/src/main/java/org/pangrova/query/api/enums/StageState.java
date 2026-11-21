package org.pangrova.query.api.enums;

import org.pangrova.common.api.annotation.Internal;

/** Observable stage state. Query Core alone owns its transition and retry rules. */
@Internal
public enum StageState {
    PLANNED, SCHEDULING, RUNNING, FLUSHING, FINISHED, FAILED, CANCELLED
}
