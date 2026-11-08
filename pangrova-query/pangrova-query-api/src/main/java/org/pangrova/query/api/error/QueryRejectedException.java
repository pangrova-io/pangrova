package org.pangrova.query.api.error;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when coordinator is draining and rejects new queries.
 */
public class QueryRejectedException extends PangrovaException {

    public QueryRejectedException(String message) {
        super(PangrovaErrorCode.COORDINATOR_QUERY_REJECTED, message);
    }
}
