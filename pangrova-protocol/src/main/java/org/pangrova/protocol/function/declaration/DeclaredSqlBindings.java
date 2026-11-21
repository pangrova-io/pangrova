package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.protocol.v2.FunctionKind;

import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Immutable indexes of the sole declaration's names, never an execution registry. */
final class DeclaredSqlBindings {
    private final Map<Key, DeclaredFunctionReference> names;
    private final Map<Key, DeclaredFunctionReference> operators;

    DeclaredSqlBindings(Collection<JsonNode> declarations) {
        Map<Key, DeclaredFunctionReference> named = new HashMap<>();
        Map<Key, DeclaredFunctionReference> standard = new HashMap<>();
        for (JsonNode declaration : declarations) {
            FunctionKind kind = FunctionKind.valueOf("FUNCTION_KIND_"
                    + BindingDeclarationJson.text(declaration, "kind"));
            var reference = new DeclaredFunctionReference(kind,
                    BindingDeclarationJson.text(declaration, "namespace"),
                    BindingDeclarationJson.text(declaration, "name"),
                    BindingDeclarationJson.text(declaration, "canonicalUrn"));
            index(named, declaration.get("sqlNames"), reference);
            index(standard, declaration.get("operators"), reference);
        }
        names = Map.copyOf(named);
        operators = Map.copyOf(standard);
    }

    Optional<DeclaredFunctionReference> find(FunctionKind kind, String sqlName, String operator) {
        Objects.requireNonNull(kind, "kind");
        if (kind == FunctionKind.FUNCTION_KIND_UNSPECIFIED || kind == FunctionKind.UNRECOGNIZED) {
            throw new IllegalArgumentException("A SQL binding requires an explicit function kind");
        }
        var named = names.get(new Key(kind, normalized(sqlName)));
        return Optional.ofNullable(named != null ? named : operators.get(new Key(kind, normalized(operator))));
    }

    private static void index(Map<Key, DeclaredFunctionReference> target, JsonNode values,
                              DeclaredFunctionReference reference) {
        if (values == null) return;
        BindingDeclarationValidation.array(values);
        for (JsonNode value : values) {
            if (!value.isTextual() || value.textValue().isBlank()) {
                throw new IllegalArgumentException("A SQL binding name must be a nonempty string");
            }
            var key = new Key(reference.kind(), normalized(value.textValue()));
            if (target.putIfAbsent(key, reference) != null) {
                throw new IllegalArgumentException("Ambiguous SQL function binding: " + key.name());
            }
        }
    }

    private static String normalized(String value) {
        return Objects.requireNonNull(value, "binding name").toUpperCase(Locale.ROOT);
    }

    private record Key(FunctionKind kind, String name) {}
}
