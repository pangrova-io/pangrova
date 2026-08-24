package org.pangrova.operations.upgrade;

import java.time.Duration;

/**
 * SPI for node lifecycle operations during rolling upgrades.
 * Implementations handle the actual infrastructure interaction.
 */
public interface NodeOperations {

    void drain(NodeInfo node, Duration timeout);

    void waitTasks(NodeInfo node);

    void stop(NodeInfo node);

    void deploy(NodeInfo node, String targetVersion);

    void start(NodeInfo node);

    void verify(NodeInfo node);

    void undrain(NodeInfo node);

    void rollback(NodeInfo node);
}
