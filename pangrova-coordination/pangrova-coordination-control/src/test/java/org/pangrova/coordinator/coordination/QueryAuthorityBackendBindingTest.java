package org.pangrova.coordinator.coordination;

import org.junit.jupiter.api.Test;
import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.persistence.query.memory.coordination.InMemoryExecutionAnchorStore;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class QueryAuthorityBackendBindingTest {
    @Test
    void haRequiresAnExplicitExecutionBackend() {
        assertThatNullPointerException().isThrownBy(() -> new HighAvailabilityCoordinationService(
                new InMemoryDistributedLease(), null)).withMessageContaining("selected explicitly");
    }

    @Test
    void processBindingCannotReplaceTheSelectedBackend() {
        var selected = new InMemoryExecutionAnchorStore();
        var different = new InMemoryExecutionAnchorStore();
        try (var service = new HighAvailabilityCoordinationService(new InMemoryDistributedLease(), selected);
             var authority = new CoordinatorExecutionAuthority(different, "node", "incarnation", Duration.ofSeconds(30))) {
            assertThatIllegalArgumentException().isThrownBy(() -> service.bindExecutionAuthority(authority))
                    .withMessageContaining("same backend");
            assertThat(service.acquireQueryOwnership("query", "node")).isTrue();
            assertThat(selected.current("query")).isPresent();
            assertThat(different.current("query")).isEmpty();
        }
    }
}
