package org.pangrova.coordinator.property.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrap;
import org.pangrova.coordinator.bootstrap.CoordinatorBootstrapConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.pangrova.coordinator.testinfra.SecuredCoordinatorFixtures.withAuthority;

/**
 * Property tests for Coordinator graceful shutdown (Property 24).
 *
 * <p>Validates that CoordinatorBootstrap starts and shuts down cleanly
 * for any valid coordinatorId, and that shutdown without prior start is safe.</p>
 *
 * <p><b>Validates: Requirements 48.1</b></p>
 */
class CoordinatorShutdownPropertyTest {

    @Provide
    Arbitrary<String> coordinatorIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(12)
                .map(s -> "coord-" + s);
    }

    /**
     * P24a: For any random coordinatorId, starting and then shutting down
     * CoordinatorBootstrap leaves isStarted() == false with no exceptions.
     *
     * <p><b>Validates: Requirements 48.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 24: Graceful shutdown completeness")
    void coordinatorShutdownTerminatesCleanly(
            @ForAll("coordinatorIds") String coordinatorId) {

        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(coordinatorId, 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(withAuthority(config));

        try {
            bootstrap.start();
            assertThat(bootstrap.isStarted()).isTrue();
        } finally {
            assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        }
        assertThat(bootstrap.isStarted()).isFalse();
    }

    /**
     * P24c (Coordinator): Calling shutdown() without start() is a no-op —
     * no exceptions, isStarted() remains false.
     *
     * <p><b>Validates: Requirements 48.1</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: e2e-pipeline-integration, Property 24: Graceful shutdown completeness")
    void coordinatorShutdownWithoutStartIsNoOp(
            @ForAll("coordinatorIds") String coordinatorId) {

        CoordinatorBootstrapConfig config = CoordinatorBootstrapConfig.withDefaults(coordinatorId, 0);
        CoordinatorBootstrap bootstrap = new CoordinatorBootstrap(config);

        assertThatCode(bootstrap::shutdown).doesNotThrowAnyException();
        assertThat(bootstrap.isStarted()).isFalse();
    }
}
