package org.pangrova.worker.clustering;

import org.pangrova.spi.connector.clustering.ClusteringCapability;
import org.pangrova.spi.connector.clustering.ClusteringRequest;
import org.pangrova.spi.connector.clustering.ClusteringResult;
import org.pangrova.spi.connector.clustering.ReclusterTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Executes recluster tasks on the worker node.
 *
 * <p>Resolves {@link ClusteringCapability} per catalog from a registry
 * populated at worker bootstrap. No compile-time dependency on
 * pangrova-indexing — capabilities are injected via SPI.</p>
 */
public final class ReclusterTaskExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(ReclusterTaskExecutor.class);

    private final Map<String, ClusteringCapability> capabilities;

    public ReclusterTaskExecutor() {
        this.capabilities = new ConcurrentHashMap<>();
    }

    /**
     * Registers a clustering capability for a catalog.
     * Called during worker bootstrap when connectors are initialized.
     */
    public void registerCapability(String catalogName, ClusteringCapability capability) {
        capabilities.put(catalogName, capability);
        LOG.info("action=register_clustering_capability catalog={}", catalogName);
    }

    /**
     * Executes a recluster task by delegating to the connector capability.
     *
     * @param task the recluster task dispatched from coordinator
     * @return result with rewritten file counts and paths
     * @throws IllegalStateException if no capability registered for the catalog
     */
    public ClusteringResult execute(ReclusterTask task) {
        ClusteringCapability capability = capabilities.get(task.catalogName());
        if (capability == null) {
            throw new IllegalStateException(
                    "No clustering capability for catalog: " + task.catalogName());
        }

        ClusteringRequest request = task.toRequest();
        long startMs = System.currentTimeMillis();
        ClusteringResult result = capability.recluster(request);
        long durationMs = System.currentTimeMillis() - startMs;

        LOG.info("action=recluster_task_complete taskId={} catalog={} "
                        + "rewritten={} durationMs={}",
                task.taskId(), task.catalogName(),
                result.rewrittenFileCount(), durationMs);
        return result;
    }
}
