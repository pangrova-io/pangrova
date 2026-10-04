package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import io.substrait.proto.Type;

import java.util.HashSet;
import java.util.Set;

/** Validates the Native-owned bridge program against the actual canonical descriptor. */
final class PhysicalTypeBridgeDeclaration {
    private static final Set<String> TRAITS = Set.of("ALWAYS", "NEVER", "ALL_CHILDREN");
    private static final java.util.List<String> TRAIT_FIELDS = java.util.List.of("known", "orderable", "comparable");

    private PhysicalTypeBridgeDeclaration() {}

    static void validate(JsonNode root) {
        BindingDeclarationJson.keys(root, Set.of("anonymousComputedFieldPrefix", "projections"), Set.of());
        if (!BindingDeclarationJson.text(root, "anonymousComputedFieldPrefix").matches("[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException("Malformed anonymous computed field prefix");
        }
        var projections = root.get("projections");
        bounded(projections);
        if (projections.isEmpty()) throw new IllegalArgumentException("Physical bridge has no projections");
        Set<String> kinds = new HashSet<>();
        for (JsonNode projection : projections) {
            String kind = BindingDeclarationJson.text(projection, "kind");
            if (!kinds.add(kind)) throw new IllegalArgumentException("Duplicate canonical bridge kind");
            validateProjection(projection, kindDescriptor(kind));
        }
    }

    private static void validateProjection(JsonNode source, Descriptor descriptor) {
        BindingDeclarationJson.keys(source, Set.of("kind", "parameters", "children", "variants", "known",
                "orderable", "comparable"), Set.of("predicates"));
        var variables = parameters(source.get("parameters"), descriptor);
        children(source.get("children"), descriptor);
        predicates(source.get("predicates"), variables);
        bounded(source.get("variants"));
        if (source.get("variants").isEmpty()) throw new IllegalArgumentException("Physical bridge has no variants");
        for (JsonNode variant : source.get("variants")) variant(variant, variables);
        for (String trait : TRAIT_FIELDS) {
            if (!TRAITS.contains(BindingDeclarationJson.text(source, trait))) {
                throw new IllegalArgumentException("Unknown actual type trait rule");
            }
        }
    }

    private static Set<String> parameters(JsonNode parameters, Descriptor descriptor) {
        bounded(parameters);
        Set<String> names = new HashSet<>();
        Set<String> fields = new HashSet<>();
        for (JsonNode parameter : parameters) {
            BindingDeclarationJson.keys(parameter,
                    Set.of("name", "field", "minimum", "maximum", "nativeArgument"), Set.of());
            String name = BindingDeclarationJson.text(parameter, "name");
            String field = BindingDeclarationJson.text(parameter, "field");
            if (!name.matches("[a-z][a-z0-9_]*") || !names.add(name) || !fields.add(field)) {
                throw new IllegalArgumentException("Malformed or duplicate canonical bridge parameter");
            }
            var declared = descriptor.findFieldByName(field);
            if (declared == null || declared.getType() != FieldDescriptor.Type.INT32 || declared.isRepeated()) {
                throw new IllegalArgumentException("Bridge parameter is not an exact canonical int32 field");
            }
            int low = exactInteger(parameter.get("minimum"));
            int high = exactInteger(parameter.get("maximum"));
            if (low > high) throw new IllegalArgumentException("Bridge parameter bounds are inverted");
            exactBoolean(parameter.get("nativeArgument"));
        }
        return Set.copyOf(names);
    }

    private static void children(JsonNode children, Descriptor descriptor) {
        bounded(children);
        Set<String> fields = new HashSet<>();
        for (JsonNode child : children) {
            BindingDeclarationJson.keys(child, Set.of("field", "repeated", "computedNames"), Set.of());
            String name = BindingDeclarationJson.text(child, "field");
            var field = descriptor.findFieldByName(name);
            if (!fields.add(name) || field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                    || !field.getMessageType().equals(Type.getDescriptor())) {
                throw new IllegalArgumentException("Bridge child is not a unique canonical Type field");
            }
            boolean repeated = exactBoolean(child.get("repeated"));
            if (field.isRepeated() != repeated || (exactBoolean(child.get("computedNames")) && !repeated)) {
                throw new IllegalArgumentException("Bridge child has invalid cardinality or name generation");
            }
        }
    }

    private static void variant(JsonNode variant, Set<String> variables) {
        BindingDeclarationJson.keys(variant, Set.of("nativeName", "bindingKind", "physicalTag", "bindingParameters",
                "predicates"), Set.of());
        for (String field : new String[]{"nativeName", "physicalTag"}) {
            if (!BindingDeclarationJson.text(variant, field).matches("[A-Z][A-Z0-9_ ]*")) {
                throw new IllegalArgumentException("Bridge requires exact actual runtime names");
            }
        }
        var descriptor = kindDescriptor(BindingDeclarationJson.text(variant, "bindingKind"));
        var substitutions = variant.get("bindingParameters");
        if (!substitutions.isObject() || substitutions.size() > 256) {
            throw new IllegalArgumentException("Binding parameter substitutions exceed their bound");
        }
        substitutions.fields().forEachRemaining(entry -> {
            var field = descriptor.findFieldByName(entry.getKey());
            if (field == null || field.getType() != FieldDescriptor.Type.INT32 || field.isRepeated()) {
                throw new IllegalArgumentException("Binding substitution is not a canonical int32 parameter");
            }
            FunctionFormulaDeclaration.parse(entry.getValue(), variables);
        });
        predicates(variant.get("predicates"), variables);
    }

    private static Descriptor kindDescriptor(String kind) {
        var field = Type.getDescriptor().findFieldByName(kind);
        if (field == null || field.getJavaType() != FieldDescriptor.JavaType.MESSAGE
                || field.getContainingOneof() == null) {
            throw new IllegalArgumentException("Bridge kind is absent from the canonical Type descriptor");
        }
        return field.getMessageType();
    }

    private static void predicates(JsonNode value, Set<String> variables) {
        if (value == null) return;
        bounded(value);
        for (JsonNode formula : value) FunctionFormulaDeclaration.parse(formula, variables);
    }

    private static void bounded(JsonNode node) {
        if (node == null || !node.isArray() || node.size() > 256) {
            throw new IllegalArgumentException("Physical bridge list is absent or exceeds its bound");
        }
    }

    private static int exactInteger(JsonNode node) {
        if (!node.isIntegralNumber() || !node.canConvertToInt()) {
            throw new IllegalArgumentException("Physical bridge parameter bound is not signed int32");
        }
        return node.intValue();
    }

    private static boolean exactBoolean(JsonNode node) {
        if (!node.isBoolean()) throw new IllegalArgumentException("Physical bridge flag is not boolean");
        return node.booleanValue();
    }

}
