package org.pangrova.shuffle.api.enums;

import org.pangrova.common.api.annotation.Experimental;

/**
 * Direction of shuffle data transfer. Direction alone says nothing about durability,
 * exact-attempt isolation, retry support, or survival of compute/storage node failure.
 * Native serving commands belong to the Native adapter; external providers implement
 * their own data-plane protocol. Capabilities must describe each effective guarantee.
 *
 */
@Experimental(reason = "ADR-002 D2: push-style shuffle transport for storage-compute separation")
public enum ShuffleTransportMode {

    /** Consumers request data from its serving endpoint; that endpoint need not be the producer. */
    PULL,

    /** Producers send data to a serving endpoint; its lifetime and durability are separate contracts. */
    PUSH
}
