package org.pangrova.worker.exception;

import org.pangrova.common.enums.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.protocol.v2.ExchangePartitionSource;

/**
 * Thrown when an exchange endpoint connection fails after all retry attempts.
 */
public class ExchangeConnectionException extends PangrovaException {

    private final String sourceTaskId;
    private final int attempts;

    public ExchangeConnectionException(ExchangePartitionSource source, int attempts, Throwable cause) {
        super(PangrovaErrorCode.EXCHANGE_CONNECTION_FAILED, buildMessage(source, attempts), cause);
        this.sourceTaskId = source.getSourceTaskId();
        this.attempts = attempts;
    }

    public String getSourceTaskId() {
        return sourceTaskId;
    }

    public int getAttempts() {
        return attempts;
    }

    private static String buildMessage(ExchangePartitionSource source, int attempts) {
        return "Exchange connection to " + source.getHost() + ":" + source.getPort()
                + " (source=" + source.getSourceTaskId() + ") failed after "
                + (attempts + 1) + " attempts";
    }
}
