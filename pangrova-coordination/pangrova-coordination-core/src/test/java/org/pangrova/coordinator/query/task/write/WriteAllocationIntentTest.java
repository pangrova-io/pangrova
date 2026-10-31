package org.pangrova.coordinator.query.task.write;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WriteAllocationIntentTest {
    @Test
    void exactPartitionsAreImmutableAndSemanticOrderIndependent() {
        var source = new HashMap<>(Map.of("region", "a=b", "day", ""));
        var intent = new WriteAllocationIntent("OVERWRITE", source);
        source.put("day", "changed");

        assertThat(intent).isEqualTo(new WriteAllocationIntent("OVERWRITE", Map.of("day", "", "region", "a=b")));
        assertThatThrownBy(() -> intent.staticPartitions().put("day", "changed"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void unknownModesAndAppendOverwriteScopesAreRejected() {
        assertThatThrownBy(() -> new WriteAllocationIntent("APPEND", Map.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WriteAllocationIntent("INSERT", Map.of("day", "a")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WriteAllocationIntent("OVERWRITE", Map.of("", "a")))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
