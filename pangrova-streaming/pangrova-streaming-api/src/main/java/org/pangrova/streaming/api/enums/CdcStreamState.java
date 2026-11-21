package org.pangrova.streaming.api.enums;

/** Durable lifecycle of a registered CDC stream. */
public enum CdcStreamState {
    ACTIVE,
    PAUSED,
    EXPIRED
}
