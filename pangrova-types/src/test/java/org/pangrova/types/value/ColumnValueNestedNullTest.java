package org.pangrova.types.value;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ColumnValueNestedNullTest {

    @Test
    void arrayPreservesNullElementsAndDetachesFromInput() {
        List<Object> source = new ArrayList<>();
        source.add("value");
        source.add(null);

        ColumnValue.ArrayValue value = new ColumnValue.ArrayValue(source);
        source.set(0, "changed");

        assertEquals("value", value.elements().get(0));
        assertNull(value.elements().get(1));
        assertThrows(UnsupportedOperationException.class,
                () -> value.elements().clear());
    }

    @Test
    void mapPreservesNullValuesAndDetachesFromInput() {
        Map<Object, Object> source = new LinkedHashMap<>();
        source.put(9, null);
        source.put(-7, "typed-key");

        ColumnValue.MapValue value = new ColumnValue.MapValue(source);
        source.put(9, "changed");

        assertEquals(List.of(9, -7), new ArrayList<>(value.entries().keySet()));
        assertNull(value.entries().get(9));
        assertThrows(UnsupportedOperationException.class,
                () -> value.entries().clear());
    }

    @Test
    void mapRejectsNullKeys() {
        Map<Object, Object> source = new LinkedHashMap<>();
        source.put(null, "invalid");

        assertThrows(IllegalArgumentException.class,
                () -> new ColumnValue.MapValue(source));
    }
}
