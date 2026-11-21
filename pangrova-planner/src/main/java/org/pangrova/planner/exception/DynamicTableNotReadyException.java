package org.pangrova.planner.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Thrown when a query targets a Dynamic Table whose initial refresh
 * has not yet completed ({@code enableQueryRewrite == false}).
 */
public final class DynamicTableNotReadyException extends PangrovaException {

    private final String dtName;

    public DynamicTableNotReadyException(String dtName) {
        super(PangrovaErrorCode.DYNAMIC_TABLE_NOT_READY,
                "Dynamic Table not ready: " + dtName + ". Initial refresh has not completed.");
        this.dtName = dtName;
    }

    public String dtName() {
        return dtName;
    }
}
