package org.pangrova.connector.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/** Dataset transaction operation owned by the connector. */
@Experimental
public enum NativeWriteOperation { CREATE, APPEND, OVERWRITE }
