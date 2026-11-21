package org.pangrova.protocol.substrait;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Names are read from the admitted complete DFS schema, never reconstructed from types. */
class SubstraitNamedFieldPathTest {
    @Test
    void pathsPreserveExactNestedNamesAcrossRowArrayAndBothMapBranches() {
        var schema = schema();
        assertThat(name(schema, 0)).isEqualTo("records");
        assertThat(name(schema, 0, 0, 0)).isEqualTo("");
        assertThat(name(schema, 0, 0, 1)).isEqualTo("nested");
        assertThat(name(schema, 0, 0, 1, 0)).isEqualTo("leaf");
        assertThat(name(schema, 1, 0, 0)).isEqualTo("key_value");
        assertThat(name(schema, 1, 1, 0, 0)).isEqualTo("value_empty");
        assertThat(name(schema, 1, 1, 0, 1, 0)).isEqualTo("value_leaf");
        assertThat(schema.getNamesList()).containsExactly("records", "", "nested", "leaf", "lookup",
                "key_value", "value_empty", "value_nested", "value_leaf", "tail");
    }

    @Test
    void malformedWholeDfsCannotHideBehindAnOtherwiseValidSelectedPath() {
        var missing = schema().toBuilder().clearNames().addAllNames(schema().getNamesList().subList(0, 9)).build();
        assertThatThrownBy(() -> name(missing, 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("missing DFS");
        assertThatThrownBy(() -> name(schema().toBuilder().addNames("unowned").build(), 0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unused DFS");
    }

    @Test
    void pathsRejectMissingOrdinalsInvalidCollectionsAndNonFieldEndpoints() {
        var schema = schema();
        for (List<Integer> path : List.of(List.of(-1), List.of(1), List.of(0), List.of(0, 2),
                List.of(0, 0, 0), List.of(0, 1, 1))) {
            assertThatThrownBy(() -> SubstraitRelationLayout.nameAtTypePath(schema, 0, path))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> name(schema, 1, 2, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> name(schema, 2, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> name(schema, -1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> name(schema, 3)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SubstraitRelationLayout.nameAtTypePath(schema, 0, null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit child ordinals");
        assertThatThrownBy(() -> SubstraitRelationLayout.nameAtTypePath(schema, 0, Arrays.asList(0, null)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("explicit child ordinals");
        assertThatThrownBy(() -> SubstraitRelationLayout.nameAtTypePath(schema, 0,
                java.util.Collections.nCopies(65, 0))).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("depth bound");
    }

    private static String name(NamedStruct schema, int top, Integer... path) {
        return SubstraitRelationLayout.nameAtTypePath(schema, top, List.of(path));
    }

    private static NamedStruct schema() {
        var scalar = Type.newBuilder().setI64(Type.I64.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        var nested = row(scalar);
        var value = row(scalar, nested);
        var array = Type.newBuilder().setList(Type.List.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).setType(value)).build();
        var map = Type.newBuilder().setMap(Type.Map.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).setKey(row(scalar)).setValue(array)).build();
        return NamedStruct.newBuilder().addAllNames(List.of("records", "", "nested", "leaf", "lookup",
                        "key_value", "value_empty", "value_nested", "value_leaf", "tail"))
                .setStruct(Type.Struct.newBuilder().addTypes(array).addTypes(map).addTypes(scalar)).build();
    }

    private static Type row(Type... types) {
        return Type.newBuilder().setStruct(Type.Struct.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED).addAllTypes(List.of(types))).build();
    }
}
