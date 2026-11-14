package org.pangrova.shuffle.exchange.enums;

/** Visibility of a partition to an exchange reader. */
public enum ExchangeReadMode {
    /** Read after the selected producers have committed the partition. */
    BLOCKING,

    /** Read incrementally as the producer writes. */
    STREAMING
}
