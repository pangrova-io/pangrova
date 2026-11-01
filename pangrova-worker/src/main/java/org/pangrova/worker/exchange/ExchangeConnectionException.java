package org.pangrova.worker.exchange;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.shuffle.api.exchange.ExchangeEndpoint;

/**
 * Thrown when an exchange endpoint connection fails after all retry attempts.
 */
public class ExchangeConnectionException extends PangrovaException {

    private final String sourceTaskId;
    private final int attempts;

    public ExchangeConnectionException(ExchangeEndpoint source, int attempts, Throwable cause) {
        super(PangrovaErrorCode.EXCHANGE_CONNECTION_FAILED, buildMessage(source, attempts), cause);
        this.sourceTaskId = source.sourceTaskId();
        this.attempts = attempts;
    }

    public String getSourceTaskId() {
        return sourceTaskId;
    }

    public int getAttempts() {
        return attempts;
    }

    private static String buildMessage(ExchangeEndpoint source, int attempts) {
        return "Exchange connection to " + source.host() + ":" + source.transportPort()
                + " (source=" + source.sourceTaskId() + ") failed after "
                + (attempts + 1) + " attempts";
    }
}
