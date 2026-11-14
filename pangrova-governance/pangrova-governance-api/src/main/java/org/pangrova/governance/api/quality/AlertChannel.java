package org.pangrova.governance.api.quality;

import org.pangrova.governance.api.enums.Severity;

import org.pangrova.common.api.annotation.Experimental;

/**
 * SPI for alert delivery channels.
 */
@Experimental
public interface AlertChannel {

    String name();

    void send(Alert alert);

    boolean supports(Severity severity);
}
