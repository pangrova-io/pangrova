package org.pangrova.coordinator.property.feature;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.ai.feature.FeatureRefreshExtension;
import org.pangrova.coordinator.ai.feature.FeatureTransformDescriptor;
import org.pangrova.coordinator.ai.feature.FeatureTransformRegistry;
import org.pangrova.coordinator.ai.feature.FeatureTransformType;
import org.pangrova.coordinator.dynamictable.detect.ChangeDetector;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for feature Dynamic Table incremental correctness (P5).
 *
 * <p>Validates that feature transforms produce consistent output for the same input.
 * When the same DynamicTableState is evaluated through the FeatureRefreshExtension,
 * the result must be identical across repeated invocations (deterministic evaluation).
 * Additionally, the transform registry must consistently resolve the same descriptors
 * for the same transform types.</p>
 *
 * <p><b>Validates: Requirements P5</b></p>
 */
class FeatureTransformCorrectnessPropertyTest {

    private static final FeatureTransformRegistry REGISTRY =
            FeatureTransformRegistry.createWithBuiltins();

    @Provide
    Arbitrary<FeatureTransformType> transformTypes() {
        return Arbitraries.of(FeatureTransformType.values());
    }

    @Provide
    Arbitrary<String> functionNames() {
        return Arbitraries.of("TOKENIZE", "NORMALIZE", "ONE_HOT_ENCODE", "BUCKETIZE",
                "tokenize", "normalize", "one_hot_encode", "bucketize");
    }

    @Provide
    Arbitrary<DynamicTableState> featureTableStates() {
        Arbitrary<String> dtNames = Arbitraries.of(
                "product_features", "user_embeddings", "doc_tokens");
        Arbitrary<String> tenantIds = Arbitraries.of("tenant_1", "tenant_2");
        Arbitrary<Long> snapshots = Arbitraries.longs().between(1L, 1000L);

        return Arbitraries.lazy(() ->
                dtNames.flatMap(name ->
                        tenantIds.flatMap(tenant ->
                                snapshots.map(snap -> buildFeatureState(name, tenant, snap))
                        )
                )
        );
    }

    /**
     * P5: The transform registry resolves the same descriptor for the same type
     * across repeated invocations. Transform resolution is deterministic.
     */
    @Property(tries = 100)
    void transformRegistryResolvesConsistently(
            @ForAll("transformTypes") FeatureTransformType type) {

        FeatureTransformDescriptor desc1 = REGISTRY.resolve(type);
        FeatureTransformDescriptor desc2 = REGISTRY.resolve(type);

        assertThat(desc1.type()).isEqualTo(desc2.type());
        assertThat(desc1.inputTypeFamily()).isEqualTo(desc2.inputTypeFamily());
        assertThat(desc1.outputType()).isEqualTo(desc2.outputType());
        assertThat(desc1.argumentCount()).isEqualTo(desc2.argumentCount());
        assertThat(desc1.description()).isEqualTo(desc2.description());
    }

    /**
     * P5: Feature function recognition is consistent — the same function name
     * always produces the same recognition result (true/false).
     */
    @Property(tries = 100)
    void featureFunctionRecognitionIsConsistent(
            @ForAll("functionNames") String functionName) {

        FeatureRefreshExtension extension = buildExtension(IncrementalResult.empty());

        boolean recognized1 = extension.isFeatureFunction(functionName);
        boolean recognized2 = extension.isFeatureFunction(functionName);

        assertThat(recognized1).isEqualTo(recognized2);
        // All provided function names should be recognized
        assertThat(recognized1).isTrue();
    }

    /**
     * P5: Evaluating the same state with the same changes produces identical results.
     * The feature refresh extension does not introduce non-determinism.
     */
    @Property(tries = 50)
    void sameInputProducesSameOutput(
            @ForAll("featureTableStates") DynamicTableState state) {

        // Use a fixed result to verify the extension delegates deterministically
        List<Map<String, Object>> fixedRecords = List.of(
                Map.of("id", 1L, "price_norm", 0.75, "rating_bucket", 3),
                Map.of("id", 2L, "price_norm", -0.5, "rating_bucket", 1));
        IncrementalResult fixedResult = new IncrementalResult(fixedRecords, 2, 0);

        FeatureRefreshExtension extension = buildExtension(fixedResult);

        ChangeResult changes = ChangeResult.empty();

        IncrementalResult result1 = extension.evaluate(state, changes);
        IncrementalResult result2 = extension.evaluate(state, changes);

        assertThat(result1.records()).isEqualTo(result2.records());
        assertThat(result1.processedRows()).isEqualTo(result2.processedRows());
        assertThat(result1.aggregationMerges()).isEqualTo(result2.aggregationMerges());
    }

    /**
     * P5: Change detection on the same state produces identical results.
     * The feature extension's change detection is deterministic.
     */
    @Property(tries = 50)
    void changeDetectionIsDeterministic(
            @ForAll("featureTableStates") DynamicTableState state) {

        FeatureRefreshExtension extension = buildExtension(IncrementalResult.empty());

        ChangeResult changes1 = extension.detectChanges(state);
        ChangeResult changes2 = extension.detectChanges(state);

        assertThat(changes1.isEmpty()).isEqualTo(changes2.isEmpty());
    }

    /**
     * P5: The set of recognized feature functions is stable — it does not change
     * between invocations of the same extension instance.
     */
    @Property(tries = 30)
    void recognizedFunctionsSetIsStable(
            @ForAll @IntRange(min = 1, max = 5) int iterations) {

        FeatureRefreshExtension extension = buildExtension(IncrementalResult.empty());

        var functions1 = extension.recognizedFunctions();

        for (int i = 0; i < iterations; i++) {
            var functionsN = extension.recognizedFunctions();
            assertThat(functionsN).isEqualTo(functions1);
        }

        // Must contain all built-in transforms plus AI_EMBED
        assertThat(functions1).contains("AI_EMBED", "TOKENIZE", "NORMALIZE",
                "ONE_HOT_ENCODE", "BUCKETIZE");
    }

    /**
     * P5: Transform type resolution by function name is case-insensitive and
     * produces the same result regardless of input casing.
     */
    @Property(tries = 100)
    void transformResolutionIsCaseInsensitive(
            @ForAll("transformTypes") FeatureTransformType type) {

        String upperName = type.functionName().toUpperCase();
        String lowerName = type.functionName().toLowerCase();

        FeatureTransformDescriptor fromUpper = REGISTRY.resolveByFunctionName(upperName);
        FeatureTransformDescriptor fromLower = REGISTRY.resolveByFunctionName(lowerName);

        assertThat(fromUpper.type()).isEqualTo(fromLower.type());
        assertThat(fromUpper.type()).isEqualTo(type);
    }

    /**
     * P5: All registered transform types have valid descriptors with non-empty
     * input/output type families and positive argument counts.
     */
    @Property(tries = 50)
    void allRegisteredTransformsHaveValidDescriptors(
            @ForAll("transformTypes") FeatureTransformType type) {

        FeatureTransformDescriptor descriptor = REGISTRY.resolve(type);

        assertThat(descriptor.inputTypeFamily()).isNotBlank();
        assertThat(descriptor.outputType()).isNotBlank();
        assertThat(descriptor.argumentCount()).isGreaterThanOrEqualTo(1);
        assertThat(descriptor.description()).isNotBlank();
        assertThat(descriptor.functionName()).isEqualTo(type.functionName());
    }

    // ── Helpers ──

    private static FeatureRefreshExtension buildExtension(IncrementalResult fixedResult) {
        ChangeDetector detector = state -> ChangeResult.empty();
        IncrementalEvaluator evaluator = (state, changes) -> fixedResult;
        RefreshPipeline pipeline = new RefreshPipeline.SnapshotRefreshPipeline(detector, evaluator);
        return new FeatureRefreshExtension(pipeline, REGISTRY);
    }

    private static DynamicTableState buildFeatureState(String dtName, String tenantId, long snapshotId) {
        Instant now = Instant.now();
        return DynamicTableState.builder()
                .dtName(dtName)
                .tenantId(tenantId)
                .state(DynamicTableLifecycleState.ACTIVE)
                .effectiveRefreshMode(DynamicTableRefreshMode.SNAPSHOT)
                .targetLag(Duration.ofHours(1))
                .checkpoint(String.valueOf(snapshotId))
                .checkpointType("SNAPSHOT")
                .sourceSchemaVersion(1)
                .createdAt(now)
                .updatedAt(now)
                .build();
    }
}
