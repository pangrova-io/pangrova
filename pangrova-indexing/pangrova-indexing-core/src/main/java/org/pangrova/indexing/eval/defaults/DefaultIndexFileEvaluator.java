package org.pangrova.indexing.eval.defaults;

import org.pangrova.indexing.eval.IndexEvaluationService;
import org.pangrova.spi.connector.index.FileSystemAccessor;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.connector.index.IndexEvalResult;
import org.pangrova.spi.indexing.eval.IndexEvalDescriptor;
import org.pangrova.spi.indexing.eval.IndexFileEvaluator;
import org.pangrova.spi.connector.index.predicate.IndexPredicate;

import java.util.Objects;

/**
 * Default implementation of {@link IndexFileEvaluator} that delegates to
 * {@link IndexEvaluationService}.
 *
 * <p>This adapter bridges the SPI contract to the indexing module's internal
 * evaluation engine, enabling both coordinator and (future) worker-side
 * index evaluation through the same interface.</p>
 */
public final class DefaultIndexFileEvaluator implements IndexFileEvaluator {

    private final IndexEvaluationService evaluationService;

    public DefaultIndexFileEvaluator(IndexEvaluationService evaluationService) {
        this.evaluationService = Objects.requireNonNull(evaluationService);
    }

    @Override
    public IndexEvalResult evaluateFile(
            IndexDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor) {
        return evaluationService.evaluateFile(
                descriptor, dataFilePath, indexFilePath, predicate, fsAccessor);
    }

    @Override
    public IndexEvalResult evaluateFile(
            IndexEvalDescriptor descriptor,
            String dataFilePath,
            String indexFilePath,
            IndexPredicate predicate,
            FileSystemAccessor fsAccessor) {
        return evaluationService.evaluateFile(
                descriptor, dataFilePath, indexFilePath, predicate, fsAccessor);
    }
}
