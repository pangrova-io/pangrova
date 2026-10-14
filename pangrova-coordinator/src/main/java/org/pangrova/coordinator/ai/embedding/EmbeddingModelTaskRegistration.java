package org.pangrova.coordinator.ai.embedding;

import org.pangrova.coordinator.ai.model.ExactModelResolver;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.task.AlterEmbeddingModelTask;
import org.pangrova.coordinator.query.execution.dispatch.task.CreateEmbeddingModelTask;
import org.pangrova.coordinator.query.execution.dispatch.task.DropEmbeddingModelTask;
import org.pangrova.coordinator.query.execution.dispatch.task.ShowEmbeddingModelsTask;
import org.pangrova.coordinator.worker.WorkerManager;
import org.pangrova.parser.ast.statement.AlterEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.CreateEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.DropEmbeddingModelStatement;
import org.pangrova.parser.ast.statement.ShowEmbeddingModelsStatement;

import java.util.Objects;

/** Registers one production SQL catalog/admission path for embedding models. */
public final class EmbeddingModelTaskRegistration {

    private EmbeddingModelTaskRegistration() {}

    /** Registers embedding model metadata tasks with the execution factory. */
    public static void register(
            MetadataExecutionFactory factory,
            EmbeddingModelRegistry registry,
            WorkerManager workers) {
        Objects.requireNonNull(factory, "factory must not be null");
        Objects.requireNonNull(registry, "registry must not be null");
        Objects.requireNonNull(workers, "workers must not be null");
        factory.registerTask(CreateEmbeddingModelStatement.class,
                new CreateEmbeddingModelTask(registry,
                        (model, capability) -> ExactModelResolver.resolve(
                                workers.getActiveWorkers(), model, capability)));
        factory.registerTask(AlterEmbeddingModelStatement.class,
                new AlterEmbeddingModelTask(registry,
                        (model, capability) -> ExactModelResolver.resolve(
                                workers.getActiveWorkers(), model, capability)));
        factory.registerTask(DropEmbeddingModelStatement.class,
                new DropEmbeddingModelTask(registry));
        factory.registerTask(ShowEmbeddingModelsStatement.class,
                new ShowEmbeddingModelsTask(registry));
    }
}
