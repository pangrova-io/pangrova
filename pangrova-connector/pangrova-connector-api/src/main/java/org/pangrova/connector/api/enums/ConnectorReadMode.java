package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

@Experimental
public enum ConnectorReadMode {
    SNAPSHOT,
    TIME_TRAVEL,
    INCREMENTAL,
    CHANGELOG
}
