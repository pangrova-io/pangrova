package org.pangrova.testing.chaos.fault;

/**
 * Enumerates the types of faults that can be injected into a distributed system under test.
 */
public enum FaultType {

    /**
     * Simulates a network partition between nodes by disconnecting Docker networks.
     */
    NETWORK_PARTITION,

    /**
     * Simulates a node crash by killing the container process.
     */
    NODE_CRASH,

    /**
     * Simulates clock skew by adjusting the system clock inside a container.
     */
    CLOCK_SKEW,

    /**
     * Simulates disk failure by filling the container's filesystem.
     */
    DISK_FAILURE,

    /**
     * Simulates slow I/O by introducing latency via Toxiproxy.
     */
    SLOW_IO
}
