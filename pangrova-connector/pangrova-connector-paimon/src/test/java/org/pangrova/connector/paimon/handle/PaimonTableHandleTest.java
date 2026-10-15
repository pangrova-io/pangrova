package org.pangrova.connector.paimon.handle;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PaimonTableHandleTest {

    @Test
    void optionalRestAuditValueDoesNotBreakImmutableHandle() {
        Map<String, String> options = new HashMap<>();
        options.put("owner", null);
        options.put("bucket", "1");

        PaimonTableHandle handle = new PaimonTableHandle(
                "default", "orders", "file:/warehouse/default.db/orders",
                Optional.empty(), "[]", List.of(), List.of(), options,
                PaimonTableHandle.NO_LIMIT);
        options.put("bucket", "2");

        assertThat(handle.properties()).containsEntry("bucket", "1").doesNotContainKey("owner");
    }
}
