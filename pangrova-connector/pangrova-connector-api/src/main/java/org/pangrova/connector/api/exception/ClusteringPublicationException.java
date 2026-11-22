package org.pangrova.connector.api.exception;

import org.pangrova.common.enums.PangrovaErrorCode;

/** Publication may have occurred and must be reconciled from the connector's durable marker. */
public final class ClusteringPublicationException extends ConnectorException {
    public ClusteringPublicationException(String message, RuntimeException cause) {
        super(PangrovaErrorCode.CONNECTOR_WRITE_ERROR, message, cause);
    }
}
