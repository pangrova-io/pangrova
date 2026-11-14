package org.pangrova.generation.api.enums;

/** Native exchange distributions that can be persisted in a durable generation graph. */
public enum GenerationExchangeDistribution {
    HASH,
    ROUND_ROBIN,
    SINGLETON,
    BROADCAST
}
