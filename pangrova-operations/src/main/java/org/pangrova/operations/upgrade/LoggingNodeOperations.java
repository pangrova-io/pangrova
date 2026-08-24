package org.pangrova.operations.upgrade;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Default {@link NodeOperations} that logs all lifecycle actions.
 * Used when no infrastructure executor (K8s, etc.) is configured.
 */
public final class LoggingNodeOperations implements NodeOperations {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingNodeOperations.class);

    @Override
    public void drain(NodeInfo node, Duration timeout) {
        LOG.info("event=node_drain nodeId={} timeout={}", node.nodeId(), timeout);
    }

    @Override
    public void waitTasks(NodeInfo node) {
        LOG.info("event=node_wait_tasks nodeId={}", node.nodeId());
    }

    @Override
    public void stop(NodeInfo node) {
        LOG.info("event=node_stop nodeId={}", node.nodeId());
    }

    @Override
    public void deploy(NodeInfo node, String targetVersion) {
        LOG.info("event=node_deploy nodeId={} targetVersion={}", node.nodeId(), targetVersion);
    }

    @Override
    public void start(NodeInfo node) {
        LOG.info("event=node_start nodeId={}", node.nodeId());
    }

    @Override
    public void verify(NodeInfo node) {
        LOG.info("event=node_verify nodeId={}", node.nodeId());
    }

    @Override
    public void undrain(NodeInfo node) {
        LOG.info("event=node_undrain nodeId={}", node.nodeId());
    }

    @Override
    public void rollback(NodeInfo node) {
        LOG.warn("event=node_rollback nodeId={}", node.nodeId());
    }
}
