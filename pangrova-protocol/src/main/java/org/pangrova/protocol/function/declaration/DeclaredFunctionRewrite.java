package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/** Ordered declaration-owned scalar program, not a Native implementation-name mapping. */
public record DeclaredFunctionRewrite(List<Node> nodes, int result) {
    private static final int MAX_INPUT_ORDINAL = 65535;
    private static final int MAX_NODE_ORDINAL = 63;

    public DeclaredFunctionRewrite {
        nodes = List.copyOf(nodes);
        if (nodes.isEmpty() || nodes.size() > 64 || result < 0 || result >= nodes.size()) {
            throw new IllegalArgumentException("Rewrite requires a bounded DAG and an existing result");
        }
        for (int ordinal = 0; ordinal < nodes.size(); ordinal++) {
            for (int dependency : nodes.get(ordinal).nodes()) {
                if (dependency >= ordinal) {
                    throw new IllegalArgumentException("Rewrite dependency is forward or cyclic");
                }
            }
        }
    }

    static DeclaredFunctionRewrite parse(JsonNode rewrite, Collection<JsonNode> declarations) {
        BindingDeclarationJson.keys(rewrite, Set.of("nodes", "result"), Set.of());
        BindingDeclarationValidation.array(rewrite.get("nodes"));
        List<Node> nodes = new ArrayList<>();
        for (JsonNode node : rewrite.get("nodes")) {
            BindingDeclarationJson.keys(node, Set.of("implementation"), Set.of("inputs", "nodes"));
            String implementation = BindingDeclarationJson.text(node, "implementation");
            var candidates = declarations.stream().filter(declaration ->
                    "SCALAR".equals(declaration.get("kind").textValue())
                    && declaration.has("implementation")
                    && implementation.equals(declaration.get("implementation").textValue())).toList();
            if (candidates.size() != 1) {
                throw new IllegalArgumentException("Rewrite dependency requires one exact direct scalar declaration");
            }
            var target = candidates.get(0);
            nodes.add(new Node(new DeclaredFunctionReference(FunctionKind.FUNCTION_KIND_SCALAR,
                    target.get("namespace").textValue(), target.get("name").textValue(),
                    target.get("canonicalUrn").textValue()),
                    ordinals(node.get("inputs")), ordinals(node.get("nodes"))));
        }
        JsonNode result = rewrite.get("result");
        if (!result.isIntegralNumber() || !result.canConvertToInt()) {
            throw new IllegalArgumentException("Rewrite result ordinal is malformed");
        }
        return new DeclaredFunctionRewrite(nodes, result.intValue());
    }

    private static List<Integer> ordinals(JsonNode source) {
        if (source == null) return List.of();
        BindingDeclarationValidation.array(source);
        List<Integer> result = new ArrayList<>();
        for (JsonNode ordinal : source) {
            if (!ordinal.isIntegralNumber() || !ordinal.canConvertToInt() || ordinal.intValue() < 0
                    || ordinal.intValue() > MAX_INPUT_ORDINAL) {
                throw new IllegalArgumentException("Rewrite input is outside its domain");
            }
            result.add(ordinal.intValue());
        }
        return List.copyOf(result);
    }

    public record Node(DeclaredFunctionReference target, List<Integer> inputs, List<Integer> nodes) {
        public Node {
            java.util.Objects.requireNonNull(target, "target");
            inputs = List.copyOf(inputs);
            nodes = List.copyOf(nodes);
            if (target.kind() != FunctionKind.FUNCTION_KIND_SCALAR || inputs.isEmpty() == nodes.isEmpty()
                    || inputs.size() + nodes.size() > 256
                    || inputs.stream().anyMatch(value -> value < 0 || value > MAX_INPUT_ORDINAL)
                    || nodes.stream().anyMatch(value -> value < 0 || value > MAX_NODE_ORDINAL)) {
                throw new IllegalArgumentException("Rewrite node requires one bounded argument source");
            }
        }
    }
}
