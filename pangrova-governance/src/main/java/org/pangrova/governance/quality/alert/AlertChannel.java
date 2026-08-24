package org.pangrova.governance.quality.alert;

/**
 * SPI for alert delivery channels.
 */
public interface AlertChannel {

    String name();

    void send(Alert alert);

    boolean supports(Severity severity);
}
