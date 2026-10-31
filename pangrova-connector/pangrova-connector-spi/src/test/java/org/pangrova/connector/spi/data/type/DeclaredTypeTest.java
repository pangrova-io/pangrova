package org.pangrova.connector.spi.data.type;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DeclaredTypeTest {
    @Test
    void snapshotsChildrenWithoutChangingRecursiveNullability() {
        var element = new DeclaredType(new TypeSignature("DOUBLE"), true, List.of());
        var children = new ArrayList<>(List.of(element));
        var signature = new TypeSignature("VECTOR", List.of(TypeSignature.TypeParameter.ofLong(3),
                TypeSignature.TypeParameter.ofType(element.signature())));

        var declaration = new DeclaredType(signature, false, children);
        children.clear();

        assertThat(declaration.signature()).isEqualTo(signature);
        assertThat(declaration.nullable()).isFalse();
        assertThat(declaration.children()).containsExactly(element);
        assertThat(declaration.children().get(0).nullable()).isTrue();
        assertThatThrownBy(() -> declaration.children().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsMissingSignatureChildrenAndChildDeclarations() {
        var signature = new TypeSignature("INTEGER");
        assertThatThrownBy(() -> new DeclaredType(null, true, List.of())).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DeclaredType(signature, true, null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new DeclaredType(signature, true, Collections.singletonList(null)))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void retainsTheExistingNestedDeclarationCountBoundary() {
        var element = new DeclaredType(new TypeSignature("INTEGER"), false, List.of());
        var signature = new TypeSignature("ROW");

        assertThat(new DeclaredType(signature, true, Collections.nCopies(4096, element)).children())
                .hasSize(4096);
        assertThatThrownBy(() -> new DeclaredType(signature, true, Collections.nCopies(4097, element)))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("Too many nested physical types");
    }
}
