package org.pangrova.worker.exchange;

/**
 * Data exchange strategies between stages.
 */
public enum ExchangeType {
    HASH_PARTITION,
    BROADCAST,
    ROUND_ROBIN,
    SINGLE
}
