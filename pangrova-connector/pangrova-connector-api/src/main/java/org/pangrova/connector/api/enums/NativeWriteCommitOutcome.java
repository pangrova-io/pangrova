package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Mutually exclusive external commit outcomes. */
@Experimental
public enum NativeWriteCommitOutcome { COMMITTED, NOT_COMMITTED, UNKNOWN }
