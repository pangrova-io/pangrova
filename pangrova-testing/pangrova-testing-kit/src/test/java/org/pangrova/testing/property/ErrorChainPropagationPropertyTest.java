package org.pangrova.testing.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying that PangrovaException cause chains preserve
 * PangrovaErrorCode values at every level.
 *
 * <p>Tests two invariants:</p>
 * <ol>
 *   <li>A 2-layer exception chain preserves both the outer and inner errorCodes.</li>
 *   <li>A 3-layer chain preserves the root cause errorCode at any depth.</li>
 * </ol>
 *
 * <p>These properties guard against exception wrapping that silently drops the
 * structured error classification across module boundaries.</p>
 *
 * <p><b>Validates: Requirements 5.8, error chain propagation</b></p>
 */
class ErrorChainPropagationPropertyTest {

    private static final List<PangrovaErrorCode> ALL_CODES =
            List.of(PangrovaErrorCode.values());

    // -----------------------------------------------------------------------
    // Property 15a: 2-layer chain preserves both error codes
    // -----------------------------------------------------------------------

    /**
     * Property 15: Wrapping an PangrovaException in another preserves the outer
     * errorCode and keeps the inner errorCode accessible via {@code getCause()}.
     *
     * <p><b>Validates: Requirements 5.8</b></p>
     */
    @Property(tries = 200)
    @Tag("Feature: full-pipeline-audit-fix, Property 15: Error chain propagation")
    void twoLayerChainPreservesBothErrorCodes(
            @ForAll("errorCodes") PangrovaErrorCode outerCode,
            @ForAll("errorCodes") PangrovaErrorCode innerCode) {
        PangrovaException root = new PangrovaException(innerCode, "root-cause");
        PangrovaException wrapped = new PangrovaException(outerCode, "wrapper", root);

        assertThat(wrapped.getErrorCode())
                .as("Outer PangrovaException must preserve its own errorCode")
                .isEqualTo(outerCode);
        assertThat(wrapped.getCause())
                .as("Wrapped exception must retain its cause")
                .isNotNull()
                .isInstanceOf(PangrovaException.class);
        assertThat(((PangrovaException) wrapped.getCause()).getErrorCode())
                .as("Inner PangrovaException errorCode must survive wrapping")
                .isEqualTo(innerCode);
    }

    // -----------------------------------------------------------------------
    // Property 15b: 3-layer chain preserves root cause errorCode
    // -----------------------------------------------------------------------

    /**
     * Property 15: The root cause errorCode is accessible at the bottom of a
     * 3-layer chain, regardless of intermediate wrapper error codes.
     *
     * <p><b>Validates: Requirements 5.8</b></p>
     */
    @Property(tries = 100)
    @Tag("Feature: full-pipeline-audit-fix, Property 15: Error chain propagation")
    void threeLayerChainPreservesRootCauseCode(
            @ForAll("errorCodes") PangrovaErrorCode rootCode,
            @ForAll("errorCodes") PangrovaErrorCode midCode,
            @ForAll("errorCodes") PangrovaErrorCode outerCode) {
        PangrovaException root = new PangrovaException(rootCode, "root");
        PangrovaException mid = new PangrovaException(midCode, "mid", root);
        PangrovaException outer = new PangrovaException(outerCode, "outer", mid);

        Throwable leaf = outer;
        while (leaf.getCause() != null) {
            leaf = leaf.getCause();
        }
        assertThat(leaf)
                .as("Deepest throwable must be an PangrovaException")
                .isInstanceOf(PangrovaException.class);
        assertThat(((PangrovaException) leaf).getErrorCode())
                .as("Root cause errorCode must be preserved through 3-layer chain")
                .isEqualTo(rootCode);
    }

    // -----------------------------------------------------------------------
    // Property 15c: errorCode is always non-null after construction
    // -----------------------------------------------------------------------

    /**
     * Property 15: Constructing an PangrovaException with any PangrovaErrorCode
     * always yields a non-null errorCode — codes are never silently nullified.
     *
     * <p><b>Validates: Requirements 5.8</b></p>
     */
    @Property(tries = 50)
    @Tag("Feature: full-pipeline-audit-fix, Property 15: Error chain propagation")
    void errorCodeIsNeverNullAfterConstruction(
            @ForAll("errorCodes") PangrovaErrorCode code) {
        PangrovaException ex = new PangrovaException(code, "message");
        assertThat(ex.getErrorCode())
                .as("PangrovaException errorCode must be non-null for any PangrovaErrorCode value")
                .isNotNull()
                .isEqualTo(code);
    }

    // -----------------------------------------------------------------------
    // Provider
    // -----------------------------------------------------------------------

    @Provide
    Arbitrary<PangrovaErrorCode> errorCodes() {
        return Arbitraries.of(ALL_CODES);
    }

    // -----------------------------------------------------------------------
    // Property 15d: errorCode survives cross-thread propagation
    // -----------------------------------------------------------------------

    /**
     * F-COMMON-01: Property 15d — when an {@link PangrovaException} is thrown
     * inside a {@link CompletableFuture} stage, the errorCode must remain
     * recoverable from the {@link CompletionException} wrapper produced on the
     * waiting thread. This guards against losing structured classification
     * across coordinator → worker → coordinator async dispatch.
     */
    @Property(tries = 50)
    @Tag("Feature: full-pipeline-audit-fix, Property 15d: Error chain across threads")
    void errorCodeSurvivesCompletableFutureBoundary(
            @ForAll("errorCodes") PangrovaErrorCode code) {
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                throw new PangrovaException(code, "async-failure");
            }, pool);

            Throwable observed = null;
            try {
                future.get();
            } catch (ExecutionException ee) {
                observed = ee.getCause();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Test thread interrupted", ie);
            }

            assertThat(observed)
                    .as("Async stage failure must surface as PangrovaException")
                    .isInstanceOf(PangrovaException.class);
            assertThat(((PangrovaException) observed).getErrorCode())
                    .as("errorCode must be preserved across CompletableFuture boundary")
                    .isEqualTo(code);
        } finally {
            pool.shutdownNow();
        }
    }

    // -----------------------------------------------------------------------
    // Property 15e: errorCode round-trips through message/cause re-construction
    // -----------------------------------------------------------------------

    /**
     * F-COMMON-01: Property 15e — simulate a serialization round-trip by
     * reconstructing an {@link PangrovaException} from the original errorCode +
     * message + cause. This mirrors what protocol layers do when rebuilding
     * exceptions on the receiving side from wire fields, and verifies the
     * resulting object preserves the structured errorCode end-to-end.
     */
    @Property(tries = 100)
    @Tag("Feature: full-pipeline-audit-fix, Property 15e: Error chain serde round-trip")
    void errorCodeRoundTripsThroughReconstruction(
            @ForAll("errorCodes") PangrovaErrorCode rootCode,
            @ForAll("errorCodes") PangrovaErrorCode wrapperCode) {
        PangrovaException originalRoot = new PangrovaException(rootCode, "root-msg");
        PangrovaException originalWrapper =
                new PangrovaException(wrapperCode, "wrap-msg", originalRoot);

        // Simulated wire-side reconstruction: rebuild from (errorCode, message, cause)
        PangrovaException restoredRoot =
                new PangrovaException(originalRoot.getErrorCode(), originalRoot.getMessage());
        PangrovaException restoredWrapper = new PangrovaException(
                originalWrapper.getErrorCode(), originalWrapper.getMessage(), restoredRoot);

        assertThat(restoredWrapper.getErrorCode()).isEqualTo(wrapperCode);
        assertThat(restoredWrapper.getMessage()).isEqualTo(originalWrapper.getMessage());
        assertThat(restoredWrapper.getCause())
                .isInstanceOf(PangrovaException.class);
        assertThat(((PangrovaException) restoredWrapper.getCause()).getErrorCode())
                .as("Root errorCode must round-trip through wire-side reconstruction")
                .isEqualTo(rootCode);
    }

    // -----------------------------------------------------------------------
    // Property 15f: CompletionException unwrapping reveals PangrovaErrorCode
    // -----------------------------------------------------------------------

    /**
     * F-COMMON-01: Property 15f — when a downstream stage observes a
     * {@link CompletionException} (synchronous join path), unwrapping the
     * cause must yield an {@link PangrovaException} carrying the original code.
     */
    @Property(tries = 50)
    @Tag("Feature: full-pipeline-audit-fix, Property 15f: CompletionException unwrap")
    void completionExceptionUnwrapsToPangrovaErrorCode(
            @ForAll("errorCodes") PangrovaErrorCode code) {
        CompletableFuture<Void> failed = CompletableFuture.failedFuture(
                new PangrovaException(code, "sync-failed"));

        Throwable observed = null;
        try {
            failed.join();
        } catch (CompletionException ce) {
            observed = ce.getCause();
        }

        assertThat(observed).isInstanceOf(PangrovaException.class);
        assertThat(((PangrovaException) observed).getErrorCode()).isEqualTo(code);
    }
}
