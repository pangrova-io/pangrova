package org.pangrova.testing.chaos.fault;

import org.testcontainers.containers.GenericContainer;

import java.time.Duration;

/**
 * Injects faults into containerized nodes for chaos testing.
 * Each method targets a specific container and applies a reversible fault condition.
 */
public interface FaultInjector {

    /**
     * Partitions the target container from the network, simulating a network split.
     *
     * @param container the container to isolate
     */
    void partitionNetwork(GenericContainer<?> container);

    /**
     * Restores network connectivity for a previously partitioned container.
     *
     * @param container the container to reconnect
     */
    void healNetwork(GenericContainer<?> container);

    /**
     * Kills the container process, simulating a node crash.
     * The container can be restarted afterward to simulate recovery.
     *
     * @param container the container to crash
     */
    void crashNode(GenericContainer<?> container);

    /**
     * Skews the clock inside the target container by the specified offset.
     *
     * @param container the container whose clock to skew
     * @param offset    positive values advance the clock, negative values retard it
     */
    void skewClock(GenericContainer<?> container, Duration offset);

    /**
     * Resets the clock inside the target container to the host clock.
     *
     * @param container the container whose clock to reset
     */
    void healClock(GenericContainer<?> container);
}
