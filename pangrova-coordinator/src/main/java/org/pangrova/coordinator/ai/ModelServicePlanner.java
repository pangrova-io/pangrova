package org.pangrova.coordinator.ai;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/**
 * Plans model service execution on the Worker data plane.
 *
 * <p>The Coordinator resolves the model endpoint and returns a Flight ticket
 * pointing to the Worker that will execute the model operation. This ensures
 * all AI/model work runs on the data plane (Workers), not the control plane
 * (Coordinator).</p>
 *
 * <p>Supports: embedding generation, LLM inference, reranking, and training
 * data export. For training data, the Coordinator plans the endpoint and
 * the Worker (or a standalone model-serving process) executes the export
 * via data exchange.</p>
 */
public final class ModelServicePlanner {

    private static final Logger LOG = LoggerFactory.getLogger(ModelServicePlanner.class);

    private ModelServicePlanner() {}

    /**
     * Describes a planned model operation to be executed on a Worker.
     *
     * @param operationType the type of model operation
     * @param modelName     the model to invoke
     * @param workerId      the target Worker (null for any available)
     * @param parameters    serialized operation parameters
     */
    public record ModelOperationPlan(
            OperationType operationType,
            String modelName,
            String workerId,
            String parameters) {

        public ModelOperationPlan {
            Objects.requireNonNull(operationType, "operationType required");
            Objects.requireNonNull(modelName, "modelName required");
        }
    }

    /**
     * Types of model operations supported by the model service.
     */
    public enum OperationType {
        /** Embedding vector generation */
        EMBEDDING,
        /** LLM text completion */
        INFERENCE,
        /** Result reranking */
        RERANK,
        /** Training data export */
        TRAINING_EXPORT
    }

    /**
     * Plans a model operation for Worker-side execution.
     *
     * <p>The Coordinator determines which Worker should execute the operation
     * (e.g., based on data locality or load) and creates a plan descriptor
     * that the scheduler can dispatch.</p>
     *
     * @param operationType the type of operation
     * @param modelName     the model name
     * @param workerId      preferred Worker ID (null for any)
     * @param parameters    operation-specific parameters
     * @return the operation plan
     */
    public static ModelOperationPlan plan(
            OperationType operationType,
            String modelName,
            String workerId,
            String parameters) {

        LOG.info("event=model_operation_planned type={} model={} worker={}",
                operationType, modelName, workerId != null ? workerId : "any");

        return new ModelOperationPlan(operationType, modelName, workerId, parameters);
    }
}
