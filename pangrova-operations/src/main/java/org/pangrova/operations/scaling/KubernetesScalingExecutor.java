package org.pangrova.operations.scaling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Scaling executor that delegates to a Kubernetes client.
 */
public final class KubernetesScalingExecutor implements AutoScalingController.ScalingExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesScalingExecutor.class);

    private final KubernetesClient client;
    private final String deploymentName;

    public KubernetesScalingExecutor(KubernetesClient client, String deploymentName) {
        this.client = Objects.requireNonNull(client, "client");
        this.deploymentName = Objects.requireNonNull(deploymentName, "deploymentName");
    }

    @Override
    public void scale(int targetWorkers) {
        LOG.info("event=k8s_scaling deployment={} targetReplicas={}", deploymentName, targetWorkers);
        client.scaleDeployment(deploymentName, targetWorkers);
    }

    /**
     * Abstraction for Kubernetes API interactions.
     */
    public interface KubernetesClient {
        void scaleDeployment(String name, int replicas);
    }
}
