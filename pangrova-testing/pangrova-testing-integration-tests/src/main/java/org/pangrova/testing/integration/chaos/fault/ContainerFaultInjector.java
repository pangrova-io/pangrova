package org.pangrova.testing.integration.chaos.fault;

import org.pangrova.testing.integration.exception.FaultInjectionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * Fault injector that uses Docker container primitives to simulate distributed system failures.
 * Network partitions are achieved by disconnecting containers from the shared network.
 * Node crashes are simulated by killing the container process.
 * Clock skew is injected via date command inside the container.
 */
public final class ContainerFaultInjector implements FaultInjector {

    private static final Logger LOG = LoggerFactory.getLogger(ContainerFaultInjector.class);

    private final Network network;

    public ContainerFaultInjector(Network network) {
        this.network = Objects.requireNonNull(network, "network must not be null");
    }

    @Override
    public void partitionNetwork(GenericContainer<?> container) {
        Objects.requireNonNull(container, "container must not be null");
        String containerId = container.getContainerId();
        String networkId = network.getId();
        LOG.info("Injecting network partition: containerId={}, networkId={}", containerId, networkId);
        try {
            container.getDockerClient()
                    .disconnectFromNetworkCmd()
                    .withContainerId(containerId)
                    .withNetworkId(networkId)
                    .exec();
        } catch (Exception e) {
            throw new FaultInjectionException(
                    "Failed to partition network for container=" + containerId, e);
        }
    }

    @Override
    public void healNetwork(GenericContainer<?> container) {
        Objects.requireNonNull(container, "container must not be null");
        String containerId = container.getContainerId();
        String networkId = network.getId();
        LOG.info("Healing network partition: containerId={}, networkId={}", containerId, networkId);
        try {
            container.getDockerClient()
                    .connectToNetworkCmd()
                    .withContainerId(containerId)
                    .withNetworkId(networkId)
                    .exec();
        } catch (Exception e) {
            throw new FaultInjectionException(
                    "Failed to heal network for container=" + containerId, e);
        }
    }

    @Override
    public void crashNode(GenericContainer<?> container) {
        Objects.requireNonNull(container, "container must not be null");
        String containerId = container.getContainerId();
        LOG.info("Crashing node: containerId={}", containerId);
        try {
            container.getDockerClient()
                    .killContainerCmd(containerId)
                    .withSignal("KILL")
                    .exec();
        } catch (Exception e) {
            throw new FaultInjectionException(
                    "Failed to crash node container=" + containerId, e);
        }
    }

    @Override
    public void skewClock(GenericContainer<?> container, Duration offset) {
        Objects.requireNonNull(container, "container must not be null");
        Objects.requireNonNull(offset, "offset must not be null");
        String containerId = container.getContainerId();
        long offsetSeconds = offset.toSeconds();
        LOG.info("Skewing clock: containerId={}, offsetSeconds={}", containerId, offsetSeconds);
        String sign = offsetSeconds >= 0 ? "+" : "";
        String command = String.format("date -s \"$(date -d '%s%d seconds')\"", sign, offsetSeconds);
        execInContainer(container, command);
    }

    @Override
    public void healClock(GenericContainer<?> container) {
        Objects.requireNonNull(container, "container must not be null");
        String containerId = container.getContainerId();
        LOG.info("Healing clock skew: containerId={}", containerId);
        execInContainer(container, "hwclock -s");
    }

    private void execInContainer(GenericContainer<?> container, String command) {
        try {
            var result = container.execInContainer("sh", "-c", command);
            if (result.getExitCode() != 0) {
                throw new FaultInjectionException(
                        "Command failed in container=" + container.getContainerId()
                                + ", exitCode=" + result.getExitCode()
                                + ", stderr=" + result.getStderr());
            }
        } catch (IOException e) {
            throw new FaultInjectionException(
                    "I/O error executing command in container=" + container.getContainerId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FaultInjectionException(
                    "Interrupted while executing command in container=" + container.getContainerId(), e);
        }
    }
}
