package org.pangrova.spi.index.eval;

import org.pangrova.spi.annotation.Experimental;

/**
 * Factory SPI for creating {@link IndexFileEvaluator} instances.
 *
 * <p>Discovered via {@link java.util.ServiceLoader} at worker startup.
 * Implementations must provide a no-arg constructor and self-bootstrap
 * all internal dependencies (engine registry, cache, storage, serializer).</p>
 *
 * <p>This factory pattern decouples the worker from the indexing module
 * at compile time while allowing runtime L2 index evaluation when the
 * indexing jar is present on the classpath.</p>
 */
@Experimental
public interface IndexFileEvaluatorFactory {

    /**
     * Creates a fully initialized IndexFileEvaluator.
     *
     * <p>Called once at worker startup. The returned evaluator is reused
     * across all task executions for the lifetime of the worker process.</p>
     *
     * @return a ready-to-use evaluator
     */
    IndexFileEvaluator create();
}
