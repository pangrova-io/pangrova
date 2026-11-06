package org.pangrova.persistence.indexing.api.metadata;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexStateUpdateContractTest {
    private static final Instant EXPECTED = Instant.parse("2026-09-05T00:00:00Z");
    private final AtomicInteger mutations = new AtomicInteger();

    @Test
    void timestampFenceCannotFallBackToUnconditionalUpdate() {
        var store = weakStore(false);
        assertThatThrownBy(() -> store.updateIndexState("index", "STALE", EXPECTED.plusSeconds(1), EXPECTED))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(mutations).hasValue(0);
    }

    @Test
    void stateAndGenerationFenceCannotFallBackToTimestampOnly() {
        var store = weakStore(true);
        assertThatThrownBy(() -> store.updateIndexState("index", "STALE", EXPECTED.plusSeconds(1),
                EXPECTED, "READY", 2)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(mutations).hasValue(0);
    }

    @Test
    void staleReasonCannotBeSilentlyDropped() {
        var store = weakStore(false);
        assertThatThrownBy(() -> store.updateIndexState("index", "STALE", EXPECTED.plusSeconds(1),
                "source changed")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(mutations).hasValue(0);
    }

    @Test
    void fencedReasonUpdateCannotDiscardAnyPartOfItsGuarantee() {
        var store = weakStore(true);
        assertThatThrownBy(() -> store.updateIndexState("index", "STALE", EXPECTED.plusSeconds(1),
                "source changed", EXPECTED, "READY", 2)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(mutations).hasValue(0);
    }

    private IndexMetadataStore weakStore(boolean supportsTimestampFence) {
        return (IndexMetadataStore) Proxy.newProxyInstance(IndexMetadataStore.class.getClassLoader(),
                new Class<?>[]{IndexMetadataStore.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("updateIndexState") && method.getParameterCount() == 3) {
                        mutations.incrementAndGet();
                        return null;
                    }
                    if (supportsTimestampFence && method.getName().equals("updateIndexState")
                            && method.getParameterCount() == 4 && method.getParameterTypes()[3] == Instant.class) {
                        mutations.incrementAndGet();
                        return true;
                    }
                    if (method.isDefault()) return InvocationHandler.invokeDefault(proxy, method, arguments);
                    throw new AssertionError("Unexpected repository call: " + method);
                });
    }
}
