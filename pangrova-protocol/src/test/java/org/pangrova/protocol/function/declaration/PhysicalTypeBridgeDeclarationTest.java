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
        assertThat(bridge.get("projections").size()).isEqualTo(22);
        JsonNode uuid = projection((ObjectNode) bridge, "uuid").get("variants").get(0);
        assertThat(uuid.get("nativeName").asText()).isEqualTo("UUID");
        assertThat(uuid.get("bindingKind").asText()).isEqualTo("uuid");
        assertThat(uuid.get("physicalTag").asText()).isEqualTo("HUGEINT");
        assertThat(FunctionBindingDeclarations.anonymousComputedField(17)).isEqualTo("field_17");
    }

    @Test
    void nonexistentTypeFieldAndWrongScalarOrRepeatedCardinalityAreRejected() {
        reject(bridge -> projection(bridge, "bool").put("kind", "invented"));
        reject(bridge -> parameter(bridge, "varchar").put("field", "absent"));
        reject(bridge -> ((ObjectNode) projection(bridge, "list").get("children").get(0))
                .put("repeated", true));
        reject(bridge -> ((ObjectNode) projection(bridge, "list").get("children").get(0))
                .put("field", "nullability"));
    }

    @Test
    void unknownTraitBoundsAndFormulaVariablesCannotBecomeRuntimeDefaults() {
        reject(bridge -> projection(bridge, "bool").put("known", "SOMETIMES"));
        reject(bridge -> parameter(bridge, "varchar").put("minimum", true));
        reject(bridge -> parameter(bridge, "varchar").put("maximum", Long.MAX_VALUE));
        reject(bridge -> parameter(bridge, "varchar").put("nativeArgument", 1));
        reject(bridge -> parameter(bridge, "varchar").put("field", "type_variation_reference"));
        reject(bridge -> ((ObjectNode) projection(bridge, "precision_time").get("variants").get(0)
                .get("predicates").get(0).get("operands").get(0)).put("value", "unbound"));
    }

    private static void reject(Consumer<ObjectNode> mutation) {
        ObjectNode source = (ObjectNode) bridge();
        mutation.accept(source);
        assertThatThrownBy(() -> PhysicalTypeBridgeDeclaration.validate(source))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ObjectNode projection(ObjectNode bridge, String kind) {
        for (JsonNode entry : bridge.get("projections")) {
            if (entry.get("kind").asText().equals(kind)) {
                return (ObjectNode) entry;
            }
        }
        throw new IllegalArgumentException("Missing projection: " + kind);
    }

    private static ObjectNode parameter(ObjectNode bridge, String kind) {
        return (ObjectNode) projection(bridge, kind).get("parameters").get(0);
    }

    private static JsonNode bridge() {
        return BindingDeclarationJson.load().get("physicalTypeBridge").deepCopy();
    }
}
