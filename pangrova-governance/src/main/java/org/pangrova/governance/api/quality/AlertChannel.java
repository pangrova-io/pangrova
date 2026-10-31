package org.pangrova.governance.api.quality;

/**
 * SPI for alert delivery channels.
 */
public interface AlertChannel {

    String name();

    void send(Alert alert);

    boolean supports(Severity severity);
}
