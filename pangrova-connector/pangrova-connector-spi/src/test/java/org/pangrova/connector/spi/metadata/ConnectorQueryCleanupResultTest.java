package org.pangrova.connector.spi.metadata;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

class ConnectorQueryCleanupResultTest {
    @Test
    void repeatedPropagationDoesNotMutateRetainedFailuresOrTheirOrder() {
        var first = new IllegalStateException("first connector failure");
        var second = new IllegalStateException("second connector failure");
        var source = new LinkedHashMap<String, RuntimeException>();
        source.put("first", first);
        source.put("second", second);
        var result = new ConnectorQueryCleanupResult(List.of("released"), source);
        source.clear();

        var initial = catchThrowable(result::requireSuccess);
        var retry = catchThrowable(result::requireSuccess);

        assertThat(result.failures().keySet()).containsExactly("first", "second");
        assertThat(result.failures().get("first")).isSameAs(first);
        assertThat(result.failures().get("second")).isSameAs(second);
        assertThat(initial).isNotSameAs(retry).hasCause(first);
        assertThat(retry).hasCause(first);
        assertThat(initial.getSuppressed()).containsExactly(second);
        assertThat(retry.getSuppressed()).containsExactly(second);
        assertThat(first.getSuppressed()).isEmpty();
        assertThat(second.getSuppressed()).isEmpty();
    }
}
