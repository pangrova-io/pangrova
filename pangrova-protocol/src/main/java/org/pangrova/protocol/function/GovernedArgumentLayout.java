package org.pangrova.protocol.function;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Exact hidden callback ABI declared with its public binding, not a SQL API. */
public record GovernedArgumentLayout(String id, List<Argument> arguments) {
    public GovernedArgumentLayout {
        if (id == null || !id.matches("[A-Z][A-Z0-9_]*")) {
            throw new IllegalArgumentException("Malformed governed layout identity");
        }
        arguments = List.copyOf(arguments);
        if (arguments.isEmpty() || arguments.size() > 256) {
            throw new IllegalArgumentException("Governed layout requires bounded exact arguments");
        }
        Set<String> names = new HashSet<>();
        for (Argument argument : arguments) {
            if (!names.add(argument.name())) throw new IllegalArgumentException("Duplicate governed argument");
        }
    }

    public List<String> signature() {
        return arguments.stream().map(Argument::type).toList();
    }

    public int ordinal(String name) {
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments.get(i).name().equals(name)) return i;
        }
        throw new IllegalArgumentException("Unknown governed argument: " + name);
    }

    static GovernedArgumentLayout parse(JsonNode node) {
        BindingDeclarationJson.keys(node, Set.of("id", "arguments"), Set.of());
        BindingDeclarationValidation.array(node.get("arguments"));
        List<Argument> arguments = new ArrayList<>();
        for (JsonNode argument : node.get("arguments")) arguments.add(parseArgument(argument));
        return new GovernedArgumentLayout(BindingDeclarationJson.text(node, "id"), arguments);
    }

    private static Argument parseArgument(JsonNode node) {
        BindingDeclarationJson.keys(node, Set.of("name", "type", "source", "encoding"),
                Set.of("path", "allowAbsentMessage"));
        List<String> path = new ArrayList<>();
        if (node.has("path")) {
            BindingDeclarationValidation.array(node.get("path"));
            for (JsonNode component : node.get("path")) {
                if (!component.isTextual()) throw new IllegalArgumentException("Binding path must be textual");
                path.add(component.textValue());
            }
        }
        if (node.has("allowAbsentMessage") && !node.get("allowAbsentMessage").isBoolean()) {
            throw new IllegalArgumentException("Absent binding policy must be boolean");
        }
        if (!"OPERATOR_BINDING".equals(BindingDeclarationJson.text(node, "source"))
                && (node.has("path") || node.has("allowAbsentMessage"))) {
            throw new IllegalArgumentException("Non-binding argument cannot carry protobuf path metadata");
        }
        return new Argument(BindingDeclarationJson.text(node, "name"), BindingDeclarationJson.text(node, "type"),
                BindingDeclarationJson.text(node, "source"), BindingDeclarationJson.text(node, "encoding"),
                path, node.path("allowAbsentMessage").booleanValue());
    }

    public record Argument(String name, String type, String source, String encoding,
                           List<String> path, boolean allowAbsentMessage) {
        public Argument {
            path = List.copyOf(path);
            if (name == null || !name.matches("[a-z][a-z0-9_]*") || !"VARCHAR".equals(type)) {
                throw new IllegalArgumentException("Governed argument requires an exact name/type");
            }
            if (source == null || encoding == null
                    || !Set.of("OPERATOR_BINDING", "PLANNER_INVOCATION_ID", "OUTPUT_VECTOR_DIMENSION", "VALUE_EXPRESSION").contains(source)
                    || !Set.of("STRING", "BASE64", "UNSIGNED_DECIMAL", "BOOLEAN", "EXACT_FLOAT").contains(encoding)) {
                throw new IllegalArgumentException("Unknown governed source or encoding");
            }
            validatePath(source, path, allowAbsentMessage);
        }

        private static void validatePath(String source, List<String> path, boolean allowAbsentMessage) {
            if ("OPERATOR_BINDING".equals(source)) {
                if (path.isEmpty() || path.size() > 16
                        || path.stream().anyMatch(part -> !part.matches("[a-z][a-z0-9_]*"))) {
                    throw new IllegalArgumentException("Malformed exact OperatorBinding path");
                }
            } else if (!path.isEmpty() || allowAbsentMessage) {
                throw new IllegalArgumentException("Non-binding argument cannot carry a binding path");
            }
        }
    }
}
