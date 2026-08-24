package org.pangrova.planner.cache;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

/**
 * Exception for cache-layer failures (e.g., L2 Redis connection).
 */
public class CacheException extends PangrovaException {

    public CacheException(PangrovaErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public CacheException(PangrovaErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
