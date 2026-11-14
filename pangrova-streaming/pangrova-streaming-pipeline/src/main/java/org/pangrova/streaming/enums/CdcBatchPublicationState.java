package org.pangrova.streaming.enums;

import org.pangrova.common.api.annotation.Internal;

/** Local publication phase of one staged CDC batch. */
@Internal
public enum CdcBatchPublicationState {
    PREPARED,
    PUBLISHING,
    PUBLISHED,
    UNKNOWN,
    DISCARDED
}
