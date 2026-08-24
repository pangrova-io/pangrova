package org.pangrova.coordinator.ai.feature;

import org.pangrova.coordinator.dynamictable.detect.ChangeDetector;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FeatureRefreshExtension}.
 */
@DisplayName("FeatureRefreshExtension")
class FeatureRefreshExtensionTest {

    private FeatureTransformRegistry registry;
    private FeatureRefreshExtension extension;

    @BeforeEach
    void setUp() {
        registry = FeatureTransformRegistry.createWithBuiltins();
        ChangeDetector detector = state -> ChangeResult.empty();
        IncrementalEvaluator evaluator = (state, changes) -> IncrementalResult.empty();
        RefreshPipeline pipeline = new RefreshPipeline.SnapshotRefreshPipeline(detector, evaluator);
        extension = new FeatureRefreshExtension(pipeline, registry);
    }

    @Test
    @DisplayName("recognizedFunctions includes AI_EMBED and all built-in transforms")
    void recognizedFunctionsIncludesAll() {
        Set<String> functions = extension.recognizedFunctions();
        assertThat(functions).containsExactlyInAnyOrder(
                "AI_EMBED", "TOKENIZE", "NORMALIZE", "ONE_HOT_ENCODE", "BUCKETIZE");
    }

    @Test
    @DisplayName("isFeatureFunction returns true for ai_embed (case-insensitive)")
    void isFeatureFunctionAiEmbed() {
        assertThat(extension.isFeatureFunction("ai_embed")).isTrue();
        assertThat(extension.isFeatureFunction("AI_EMBED")).isTrue();
        assertThat(extension.isFeatureFunction("Ai_Embed")).isTrue();
    }

    @Test
    @DisplayName("isFeatureFunction returns true for built-in transforms")
    void isFeatureFunctionBuiltins() {
        assertThat(extension.isFeatureFunction("TOKENIZE")).isTrue();
        assertThat(extension.isFeatureFunction("normalize")).isTrue();
        assertThat(extension.isFeatureFunction("ONE_HOT_ENCODE")).isTrue();
        assertThat(extension.isFeatureFunction("bucketize")).isTrue();
    }

    @Test
    @DisplayName("isFeatureFunction returns false for unknown functions")
    void isFeatureFunctionUnknown() {
        assertThat(extension.isFeatureFunction("SUM")).isFalse();
        assertThat(extension.isFeatureFunction("COUNT")).isFalse();
        assertThat(extension.isFeatureFunction("")).isFalse();
        assertThat(extension.isFeatureFunction(null)).isFalse();
    }

    @Test
    @DisplayName("pipeline returns the delegate pipeline")
    void pipelineReturnsDelegate() {
        RefreshPipeline pipeline = extension.pipeline();
        assertThat(pipeline).isNotNull();
        assertThat(pipeline.refreshMode()).isEqualTo(DynamicTableRefreshMode.SNAPSHOT);
    }

    @Test
    @DisplayName("transformRegistry returns the injected registry")
    void transformRegistryReturnsInjected() {
        assertThat(extension.transformRegistry()).isSameAs(registry);
    }

    @Test
    @DisplayName("detectChanges delegates to underlying pipeline")
    void detectChangesDelegates() {
        DynamicTableState state = createTestState();
        ChangeResult result = extension.detectChanges(state);
        assertThat(result).isNotNull();
        assertThat(result.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("evaluate delegates to underlying pipeline")
    void evaluateDelegates() {
        DynamicTableState state = createTestState();
        IncrementalResult result = extension.evaluate(state, ChangeResult.empty());
        assertThat(result).isNotNull();
        assertThat(result.records()).isEmpty();
    }

    @Test
    @DisplayName("constructor rejects null delegate")
    void constructorRejectsNullDelegate() {
        assertThatThrownBy(() -> new FeatureRefreshExtension(null, registry))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("delegate must not be null");
    }

    @Test
    @DisplayName("constructor rejects null registry")
    void constructorRejectsNullRegistry() {
        ChangeDetector detector = state -> ChangeResult.empty();
        IncrementalEvaluator evaluator = (state, changes) -> IncrementalResult.empty();
        RefreshPipeline pipeline = new RefreshPipeline.SnapshotRefreshPipeline(detector, evaluator);
        assertThatThrownBy(() -> new FeatureRefreshExtension(pipeline, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("transformRegistry must not be null");
    }

    private static DynamicTableState createTestState() {
        Instant now = Instant.now();
        return DynamicTableState.builder()
                .dtName("test_features")
                .tenantId("tenant-1")
                .state(DynamicTableLifecycleState.ACTIVE)
                .effectiveRefreshMode(DynamicTableRefreshMode.SNAPSHOT)
                .targetLag(Duration.ofMinutes(5))
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
