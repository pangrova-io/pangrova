package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.HashSet;
import java.util.Set;

/** Validates declaration shape without inventing Native types or registrations. */
final class BindingDeclarationValidation {
    private static final int MAX_DECLARATION_LIST_SIZE = 4096;
    private static final int NORMALIZED_ARGUMENT_ORDINAL_LIMIT = 65536;

    private BindingDeclarationValidation() {}

    static void binding(JsonNode entry) {
        BindingDeclarationJson.keys(entry, Set.of("namespace", "canonicalUrn", "name", "kind"),
                Set.of("implementation", "sqlNames", "operators", "rewrite", "structuralLowering",
                        "registrationDependency", "governedLayout", "normalizedArguments",
                        "runtimeSignature", "options"));
        String kind = BindingDeclarationJson.text(entry, "kind");
        if (!BindingDeclarationJson.text(entry, "canonicalUrn")
                .matches("extension:[a-z0-9.]+:[a-z][a-z0-9_]*")) {
            throw new IllegalArgumentException("A binding requires its exact canonical extension URN");
        }
        if (!Set.of("SCALAR", "AGGREGATE", "WINDOW").contains(kind)) {
            throw new IllegalArgumentException("Unknown function declaration kind: " + kind);
        }
        int paths = (entry.has("implementation") ? 1 : 0) + (entry.has("rewrite") ? 1 : 0)
                + (entry.has("structuralLowering") ? 1 : 0);
        if (paths != 1) {
            throw new IllegalArgumentException("Function declaration requires one implementation path");
        }
        namesIfPresent(entry, "operators");
        namesIfPresent(entry, "sqlNames");
        if (entry.has("options")) {
            validateOptions(entry);
        }
        if (entry.has("normalizedArguments")) {
            array(entry.get("normalizedArguments"));
            Set<Integer> ordinals = new HashSet<>();
            for (JsonNode argument : entry.get("normalizedArguments")) {
                normalizedArgument(argument);
                if (!ordinals.add(argument.get("ordinal").intValue())) {
                    throw new IllegalArgumentException("Duplicate normalized argument ordinal");
                }
            }
        }
    }

    private static void validateOptions(JsonNode entry) {
        if (!"WINDOW".equals(BindingDeclarationJson.text(entry, "kind")) || !entry.has("implementation")) {
            throw new IllegalArgumentException("Executable options require a registered window implementation");
        }
        array(entry.get("options"));
        Set<String> names = new HashSet<>();
        Set<String> behaviors = new HashSet<>();
        for (JsonNode option : entry.get("options")) {
            DeclaredFunctionOption parsed = DeclaredFunctionOption.parse(option);
            if (!names.add(parsed.name()) || !behaviors.add(parsed.behavior())) {
                throw new IllegalArgumentException("Duplicate executable function option");
            }
        }
    }

    static void array(JsonNode node) {
        if (node == null || !node.isArray() || node.size() > MAX_DECLARATION_LIST_SIZE) {
            throw new IllegalArgumentException("Declaration list is absent or exceeds its bound");
        }
    }

    static void normalizedArgument(JsonNode argument) {
        BindingDeclarationJson.keys(argument, Set.of("ordinal", "name", "type", "constant", "optional",
                "default", "enumValues"), Set.of());
        JsonNode ordinal = argument.get("ordinal");
        if (!ordinal.isIntegralNumber() || !ordinal.canConvertToInt() || ordinal.intValue() < 0
                || ordinal.intValue() >= NORMALIZED_ARGUMENT_ORDINAL_LIMIT) {
            throw new IllegalArgumentException("Normalized argument ordinal is outside its exact domain");
        }
        BindingDeclarationJson.text(argument, "name");
        BindingDeclarationJson.text(argument, "default");
        names(argument.get("enumValues"));
    }

    private static void namesIfPresent(JsonNode entry, String field) {
        if (entry.has(field)) {
            names(entry.get(field));
        }
    }

    private static void names(JsonNode values) {
        array(values);
        Set<String> seen = new HashSet<>();
        for (JsonNode name : values) {
            if (!name.isTextual() || name.textValue().isEmpty() || !seen.add(name.textValue())) {
                throw new IllegalArgumentException("Declaration names must be unique nonempty strings");
            }
        }
    }
}
