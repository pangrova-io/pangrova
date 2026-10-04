package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PhysicalTypeBridgeDeclarationTest {
    @Test
    void allSourceProjectionsResolveAgainstActualCanonicalDescriptors() {
        JsonNode bridge = bridge();
        PhysicalTypeBridgeDeclaration.validate(bridge);
        assertThat(bridge.get("projections").size()).isEqualTo(21);
        assertThat(FunctionBindingDeclarations.anonymousComputedField(17)).isEqualTo("field_17");
    }

    @Test
    void nonexistentTypeFieldAndWrongScalarOrRepeatedCardinalityAreRejected() {
        reject(bridge -> projection(bridge, 0).put("kind", "invented"));
        reject(bridge -> parameter(bridge, 8).put("field", "absent"));
        reject(bridge -> ((ObjectNode) projection(bridge, 17).get("children").get(0))
                .put("repeated", true));
        reject(bridge -> ((ObjectNode) projection(bridge, 17).get("children").get(0))
                .put("field", "nullability"));
    }

    @Test
    void unknownTraitBoundsAndFormulaVariablesCannotBecomeRuntimeDefaults() {
        reject(bridge -> projection(bridge, 0).put("known", "SOMETIMES"));
        reject(bridge -> parameter(bridge, 8).put("minimum", true));
        reject(bridge -> parameter(bridge, 8).put("maximum", Long.MAX_VALUE));
        reject(bridge -> parameter(bridge, 8).put("nativeArgument", 1));
        reject(bridge -> parameter(bridge, 8).put("field", "type_variation_reference"));
        reject(bridge -> ((ObjectNode) projection(bridge, 14).get("variants").get(0)
                .get("predicates").get(0).get("operands").get(0)).put("value", "unbound"));
    }

    private static void reject(Consumer<ObjectNode> mutation) {
        ObjectNode source = (ObjectNode) bridge();
        mutation.accept(source);
        assertThatThrownBy(() -> PhysicalTypeBridgeDeclaration.validate(source))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectNode projection(ObjectNode bridge, int ordinal) {
        return (ObjectNode) bridge.get("projections").get(ordinal);
    }

    private static ObjectNode parameter(ObjectNode bridge, int ordinal) {
        return (ObjectNode) projection(bridge, ordinal).get("parameters").get(0);
    }

    private static JsonNode bridge() {
        return BindingDeclarationJson.load().get("physicalTypeBridge").deepCopy();
    }
}
