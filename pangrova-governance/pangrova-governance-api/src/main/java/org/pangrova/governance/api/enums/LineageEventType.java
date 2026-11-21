package org.pangrova.governance.api.enums;

import org.pangrova.common.api.annotation.Stable;

/** Run lifecycle label recorded in query lineage facts and published to lineage backends. */
@Stable(since = "1.0")
public enum LineageEventType {
    START,
    COMPLETE,
    FAIL,
    ABORT
}
