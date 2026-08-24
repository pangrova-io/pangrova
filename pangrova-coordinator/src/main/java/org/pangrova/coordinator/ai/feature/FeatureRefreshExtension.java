package org.pangrova.coordinator.ai.feature;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Set;

/**
 * Extends the existing RefreshPipeline to support ai_embed and built-in feature
 * transforms during Dynamic Table refresh cycles for feature-type tables.
 *
 * <p>When a Dynamic Table is created with {@code WITH (type = 'feature')}, its
 * defining query may contain:</p>
 * <ul>
 *   <li>{@code ai_embed('model', col1, col2)} — embedding generation via EmbeddingProvider</li>
 *   <li>{@code TOKENIZE(col, 'tokenizer')} — text tokenization</li>
 *   <li>{@code NORMALIZE(col, 'strategy')} — statistical normalization</li>
 *   <li>{@code ONE_HOT_ENCODE(col, categories)} — categorical encoding</li>
 *   <li>{@code BUCKETIZE(col, boundaries)} — numeric bucketing</li>
 * </ul>
 *
 * <p>This extension wraps a standard {@link RefreshPipeline} and ensures that
 * feature UDFs are recognized during the refresh evaluation phase. It reuses
 * the existing {@code RefreshScheduler} for trigger scheduling,
 * {@code IncrementalMerger} for aggregation support, and
 * {@code DynamicTableRewriter} for transparent query redirection.</p>
 *
 * <p>The extension delegates actual UDF execution to the native worker via
 * the DynamicTableRefreshDispatchFunction. The Substrait plan compiled for
 * the refresh query includes ProjectRel nodes for feature transforms and
 * ai_embed calls, which the worker evaluates in Arrow columnar memory.</p>
 *
 * <p>Thread safety: instances are safe for concurrent use. The wrapped pipeline
 * and registry are both immutable after construction.</p>
 */
@ThreadSafe
public final class FeatureRefreshExtension {

    private static final Logger LOG = LoggerFactory.getLogger(FeatureRefreshExtension.class);

    private static final String AI_EMBED_FUNCTION = "AI_EMBED";

    private final RefreshPipeline delegate;
    private final FeatureTransformRegistry transformRegistry;
    private final Set<String> featureFunctions;

    /**
     * Creates a feature refresh extension wrapping the given pipeline.
     *
     * @param delegate          the underlying refresh pipeline (SNAPSHOT or CDC)
     * @param transformRegistry registry of built-in feature transforms
     */
    public FeatureRefreshExtension(RefreshPipeline delegate, FeatureTransformRegistry transformRegistry) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.transformRegistry = Objects.requireNonNull(transformRegistry, "transformRegistry must not be null");
        this.featureFunctions = buildFeatureFunctionSet(transformRegistry);
        LOG.info("event=feature_refresh_extension_created refresh_mode={} registered_functions={}",
                delegate.refreshMode(), featureFunctions.size());
    }

    /**
     * Detects changes in the source table via the delegate pipeline.
     *
     * <p>Feature transforms do not affect change detection — they only apply
     * during the evaluation phase when computing output rows.</p>
     *
     * @param state current Dynamic Table state
     * @return change detection result from the delegate
     */
    public ChangeResult detectChanges(DynamicTableState state) {
        Objects.requireNonNull(state, "state must not be null");
        return delegate.detectChanges(state);
    }

    /**
     * Evaluates detected changes through the defining query logic, including
     * feature transforms and ai_embed calls.
     *
     * <p>The evaluation is delegated to the underlying pipeline. Feature UDFs
     * (TOKENIZE, NORMALIZE, ONE_HOT_ENCODE, BUCKETIZE, ai_embed) are compiled
     * into the Substrait plan's ProjectRel nodes by the planner and executed
     * on the native worker in Arrow columnar memory.</p>
     *
     * @param state   current Dynamic Table state
     * @param changes detected changes from the source table
     * @return evaluation result with transformed output rows
     */
    public IncrementalResult evaluate(DynamicTableState state, ChangeResult changes) {
        Objects.requireNonNull(state, "state must not be null");
        Objects.requireNonNull(changes, "changes must not be null");
        LOG.debug("event=feature_refresh_evaluate dtName={} change_type={}",
                state.dtName(), changes.getClass().getSimpleName());
        return delegate.evaluate(state, changes);
    }

    /**
     * Checks whether a SQL function name is a recognized feature function
     * (either a built-in transform or ai_embed).
     *
     * @param functionName the SQL function name to check
     * @return true if the function is handled by this extension
     */
    public boolean isFeatureFunction(String functionName) {
        if (functionName == null || functionName.isBlank()) {
            return false;
        }
        return featureFunctions.contains(functionName.toUpperCase());
    }

    /**
     * Returns the set of all recognized feature function names (uppercase).
     *
     * @return unmodifiable set of feature function names including AI_EMBED and built-in transforms
     */
    public Set<String> recognizedFunctions() {
        return featureFunctions;
    }

    /**
     * Returns the underlying transform registry.
     *
     * @return the feature transform registry
     */
    public FeatureTransformRegistry transformRegistry() {
        return transformRegistry;
    }

    /**
     * Returns the underlying refresh pipeline.
     *
     * @return the delegate pipeline
     */
    public RefreshPipeline pipeline() {
        return delegate;
    }

    private static Set<String> buildFeatureFunctionSet(FeatureTransformRegistry registry) {
        Set<String> functions = new java.util.LinkedHashSet<>();
        functions.add(AI_EMBED_FUNCTION);
        for (FeatureTransformType type : registry.registeredTypes()) {
            functions.add(type.functionName().toUpperCase());
        }
        return Set.copyOf(functions);
    }
}
