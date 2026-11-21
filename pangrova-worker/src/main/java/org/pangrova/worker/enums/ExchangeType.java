package org.pangrova.worker.enums;

/**
 * Data exchange strategies between stages.
 */
public enum ExchangeType {
    HASH_PARTITION,
    BROADCAST,
    ROUND_ROBIN,
    SINGLE
}
