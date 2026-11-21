package org.pangrova.indexing.api.data;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompositeIndexKeyTest {
    @Test
    void ownsTheListAndOnlyRetainsKnownImmutableScalarClasses() {
        var input = new ArrayList<Comparable<?>>(List.of(1L, "a"));
        var key = new CompositeIndexKey(input);
        input.set(0, 9L);
        assertThat(key.components()).containsExactly(1L, "a");
        assertThatThrownBy(() -> key.components().set(1, "changed")).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new CompositeIndexKey(List.of(new StringBuilder("mutable"), "a")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompositeIndexKey(List.of(new DecimalSubclass(), "a")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void comparisonIsLexicographicOnlyForCompleteCompatibleKeys() {
        var key = new CompositeIndexKey(List.of(1L, "b"));
        assertThat(key.compareTo(new CompositeIndexKey(List.of(1L, "c")))).isNegative();
        assertThat(key.compareTo(new CompositeIndexKey(List.of(2L, "a")))).isNegative();
        assertThat(key.compareTo(new CompositeIndexKey(List.of(1L, "b")))).isZero();
        assertThatThrownBy(() -> key.compareTo(new CompositeIndexKey(List.of(1, "b"))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> key.compareTo(new CompositeIndexKey(List.of(1L, "b", 3))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNullNestedAndUnboundedKeys() {
        assertThatThrownBy(() -> new CompositeIndexKey(Arrays.asList(1L, null))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CompositeIndexKey(List.of(1L))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompositeIndexKey(Collections.nCopies(1025, 1L)))
                .isInstanceOf(IllegalArgumentException.class);
        var key = new CompositeIndexKey(List.of(1L, "a"));
        assertThatThrownBy(() -> new CompositeIndexKey(List.of(key, 1L))).isInstanceOf(IllegalArgumentException.class);
        assertThat(new CompositeIndexKey(Collections.nCopies(1024, 1L)).components()).hasSize(1024);
    }

    private static final class DecimalSubclass extends BigDecimal {
        private DecimalSubclass() { super("1"); }
    }
}
