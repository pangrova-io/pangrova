package org.pangrova.common.util;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OrderedMapsTest {
    @Test
    void retainsKeyOrderAndNullsAndOwnsAnUnmodifiableSnapshot() {
        var source = new LinkedHashMap<String, String>();
        source.put("z", null);
        source.put(null, "middle");
        source.put("a", "last");

        var snapshot = OrderedMaps.immutableCopy(source);
        source.clear();

        assertThat(snapshot.keySet()).containsExactly("z", null, "a");
        assertThat(snapshot).containsEntry("z", null).containsEntry(null, "middle");
        assertThatThrownBy(() -> snapshot.entrySet().iterator().next().setValue("mutated"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
