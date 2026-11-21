package org.pangrova.indexing.api.evaluation;

import org.pangrova.common.api.annotation.Experimental;

import org.pangrova.connector.api.metadata.ConnectorSplit;
import org.pangrova.connector.api.metadata.SplitProperties;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Wraps a {@link ConnectorSplit} with pre-computed index evaluation results.
 *
 * <p>The coordinator creates this wrapper after L2 index evaluation. The worker
 * can then call {@link #evalResult()} to check which data files were already
 * evaluated and skip reading files marked as SKIP.</p>
 *
 * <p>All {@link ConnectorSplit} methods delegate to the wrapped split.
 * The {@link #dataFilePaths()} method filters out files that were evaluated
 * as SKIP, providing automatic secondary pruning.</p>
 */
@Experimental
public final class IndexEvaluatedSplit implements ConnectorSplit {

    private final ConnectorSplit delegate;
    private final SplitIndexEvalResult evalResult;

    public IndexEvaluatedSplit(ConnectorSplit delegate, SplitIndexEvalResult evalResult) {
        this.delegate = Objects.requireNonNull(delegate);
        this.evalResult = Objects.requireNonNull(evalResult);
    }

    /** Returns the pre-computed index evaluation result. */
    public SplitIndexEvalResult evalResult() {
        return evalResult;
    }

    /** Returns the original unwrapped split. */
    public ConnectorSplit unwrap() {
        return delegate;
    }

    @Override
    public Map<String, String> splitProperties() {
        return delegate.splitProperties();
    }

    @Override
    public SplitProperties typedSplitProperties() {
        return delegate.typedSplitProperties();
    }

    @Override
    public byte[] toSplitInfoProto() {
        return delegate.toSplitInfoProto();
    }

    /**
     * Returns data file paths with SKIP files filtered out.
     * This provides automatic worker-side secondary pruning.
     */
    @Override
    public List<String> dataFilePaths() {
        List<String> allPaths = delegate.dataFilePaths();
        if (evalResult.fileResults().isEmpty()) {
            return allPaths;
        }
        return evalResult.keepableFiles(allPaths);
    }
}
