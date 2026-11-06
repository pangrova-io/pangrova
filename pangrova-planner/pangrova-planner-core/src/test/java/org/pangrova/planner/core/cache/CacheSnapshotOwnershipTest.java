package org.pangrova.planner.core.cache;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CacheSnapshotOwnershipTest {
    @Test
    void cachedBytesCannotBeModifiedThroughEitherProducerOrReader() {
        byte[] bytes = {1, 2};
        var cached = new CachedResult(bytes, Instant.EPOCH);
        bytes[0] = 9;
        byte[] firstRead = cached.data();
        firstRead[1] = 8;
        assertThat(cached.data()).containsExactly(1, 2);
    }

    @Test
    void queryContextRetainsExactImmutableSessionIdentity() {
        var properties = new HashMap<String, String>();
        properties.put("timezone", "UTC");
        var context = new QueryContext("catalog", "schema", "schema", properties,
                "snapshot", "tenant", "alice", "roles", "policy", "mask");
        properties.put("timezone", "Asia/Shanghai");
        assertThat(context.sessionProperties()).containsEntry("timezone", "UTC");
        assertThatThrownBy(() -> context.sessionProperties().put("timezone", "mutated"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
