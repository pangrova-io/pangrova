package org.pangrova.protocol.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/** Wire-version rejection, independent of Planner conversion and its exception hierarchy. */
public final class SubstraitVersionMismatchException extends PangrovaException {
    public SubstraitVersionMismatchException(String message) {
        super(PangrovaErrorCode.SUBSTRAIT_TYPE_MAPPING_FAILURE, message);
    }
}
