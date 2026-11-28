package org.pangrova.protocol.function.declaration;

import org.pangrova.common.crypto.Sha256Digests;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.protobuf.ByteString;
import io.substrait.proto.Type;
import org.pangrova.protocol.substrait.generated.GeneratedTypeLayoutNaming;
import org.pangrova.protocol.v2.CapabilityRef;
import org.pangrova.protocol.v2.FunctionNormalizedArgument;
import org.pangrova.protocol.v2.FunctionKind;
import org.pangrova.protocol.v2.FunctionTypeTemplate;
import org.pangrova.protocol.v2.LogicalType;
import org.pangrova.protocol.v2.TypedValue;
import org.pangrova.function.api.metadata.FunctionMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** SQL/structural semantics, never a substitute for an actual Native inventory. */
public final class FunctionBindingDeclarations {
    private static final FunctionBindingDeclarations INSTANCE = new FunctionBindingDeclarations();
    private final Map<String, JsonNode> structural = new HashMap<>();
    private final Map<String, JsonNode> bindings = new HashMap<>();
    private final Map<String, GovernedArgumentLayout> governedLayouts = new HashMap<>();
    private final DeclaredSqlBindings sqlBindings;
    private final ByteString declarationSha256;

    private FunctionBindingDeclarations() {
        JsonNode root = BindingDeclarationJson.load();
        declarationSha256 = ByteString.copyFrom(Sha256Digests.sha256().digest(BindingDeclarationJson.canonicalBytes(
                root)));
        BindingDeclarationJson.keys(root, Set.of("schemaVersion", "bindings", "structuralContracts",
                "physicalTypeBridge", "governedLayouts"), Set.of());
        if (!root.get("schemaVersion").isIntegralNumber() || !root.get("schemaVersion").canConvertToInt()
                || root.get("schemaVersion").intValue() != 1) {
            throw new IllegalArgumentException("Unknown binding declaration schema");
        }
        BindingDeclarationValidation.array(root.get("bindings"));
        BindingDeclarationValidation.array(root.get("structuralContracts"));
        PhysicalTypeProjectionDeclaration.validate(root.get("physicalTypeBridge"));
        loadGovernedLayouts(root.get("governedLayouts"));
        loadBindings(root.get("bindings"));
        for (JsonNode binding : bindings.values()) {
            if (binding.has("rewrite")) DeclaredFunctionRewrite.parse(binding.get("rewrite"), bindings.values());
        }
        sqlBindings = new DeclaredSqlBindings(bindings.values());
        loadStructuralContracts(root.get("structuralContracts"));
    }

    /** Exact identity of the sole complete SQL binding declaration. */
    public static ByteString declarationSha256() {
        return INSTANCE.declarationSha256;
    }

    private void loadGovernedLayouts(JsonNode entries) {
        BindingDeclarationValidation.array(entries);
        if (entries.size() > 64) {
            throw new IllegalArgumentException("Governed layouts exceed their bound");
        }
        for (JsonNode entry : entries) {
            GovernedArgumentLayout layout = GovernedArgumentLayout.parse(entry);
            if (governedLayouts.putIfAbsent(layout.id(), layout) != null) {
                throw new IllegalArgumentException("Duplicate governed argument layout: " + layout.id());
            }
        }
    }

    private void loadBindings(JsonNode entries) {
        for (JsonNode entry : entries) {
            BindingDeclarationValidation.binding(entry);
            if (entry.has("governedLayout") && !governedLayouts.containsKey(entry.get("governedLayout").textValue())) {
                throw new IllegalArgumentException("Binding refers to an absent governed layout");
            }
            String key = BindingDeclarationJson.text(entry, "kind") + ":"
                    + BindingDeclarationJson.text(entry, "namespace") + ":"
                    + BindingDeclarationJson.text(entry, "name");
            if (bindings.putIfAbsent(key, entry) != null) {
                throw new IllegalArgumentException("Duplicate function binding: " + key);
            }
        }
    }

    private void loadStructuralContracts(JsonNode entries) {
        for (JsonNode entry : entries) {
            BindingDeclarationJson.keys(entry, Set.of("id", "version", "kind", "placement", "implementation",
                    "argumentLayout", "options", "output", "deterministic", "retry"), Set.of());
            String id = BindingDeclarationJson.text(entry, "id");
            if (structural.putIfAbsent(id, entry) != null) {
                throw new IllegalArgumentException("Duplicate structural contract: " + id);
            }
        }
    }

    /** Semantic compatibility identity only: neither authorization nor behavioral proof. */
    public static CapabilityRef structuralReference(String id) {
        JsonNode entry = INSTANCE.structural.get(id);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown structural contract: " + id);
        }
        return CapabilityRef.newBuilder().setCapabilityId(id)
                .setSemanticVersion(BindingDeclarationJson.text(entry, "version"))
                .setSignatureSha256(ByteString.copyFrom(Sha256Digests.sha256().digest(
                        BindingDeclarationJson.canonicalBytes(entry))))
                .build();
    }

    /** Declared names/operators only. Actual signature admission is a separate required check. */
    public static Optional<DeclaredFunctionReference> sqlBinding(FunctionKind kind, String name, String operator) {
        return INSTANCE.sqlBindings.find(kind, name, operator);
    }

    /** Rewrites refer only to exact direct identities from this same declaration. */
    public static Optional<DeclaredFunctionRewrite> rewrite(String namespace, String name) {
        JsonNode entry = binding("SCALAR", namespace, name);
        return entry.has("rewrite")
                ? Optional.of(DeclaredFunctionRewrite.parse(entry.get("rewrite"), INSTANCE.bindings.values()))
                : Optional.empty();
    }

    /** Exact serialized extension identity; unknown bindings never synthesize an extension namespace. */
    public static String canonicalUrn(FunctionKind kind, String namespace, String name) {
        if (kind == FunctionKind.FUNCTION_KIND_UNSPECIFIED || kind == FunctionKind.UNRECOGNIZED) {
            throw new IllegalArgumentException("Canonical extension identity requires an explicit function kind");
        }
        return BindingDeclarationJson.text(binding(kind.name().substring("FUNCTION_KIND_".length()), namespace, name),
                "canonicalUrn");
    }

    /** Ordered defaults/enum constraints originate in the same declaration as the SQL binding. */
    public static List<FunctionNormalizedArgument> normalizedArguments(String kind, String namespace, String name) {
        JsonNode entry = binding(kind, namespace, name);
        List<FunctionNormalizedArgument> arguments = new ArrayList<>();
        JsonNode declared = entry.get("normalizedArguments");
        if (declared != null) {
            declared.forEach(argument -> arguments.add(normalizedArgument(argument)));
        }
        return List.copyOf(arguments);
    }

    /** Pangrova extension options are declared, never inferred from a Native name. */
    public static List<DeclaredFunctionOption> options(String kind, String namespace, String name) {
        JsonNode declared = binding(kind, namespace, name).get("options");
        if (declared == null) return List.of();
        List<DeclaredFunctionOption> result = new ArrayList<>();
        declared.forEach(option -> result.add(DeclaredFunctionOption.parse(option)));
        return List.copyOf(result);
    }

    public static io.substrait.proto.FunctionOption option(
            String kind, String namespace, String name, String behavior, boolean enabled) {
        return options(kind, namespace, name).stream().filter(option -> option.behavior().equals(behavior))
                .findFirst().orElseThrow(() -> new IllegalArgumentException(
                        "Function does not support option behavior " + behavior + ": " + name)).select(enabled);
    }

    /** Runtime hidden binding membership is declared alongside the public SQL name. */
    public static boolean hasGovernedArguments(String kind, String namespace, String name) {
        return binding(kind, namespace, name).has("governedLayout");
    }

    /** Unknown layouts fail closed; consumers must not guess an argument count or ordinal. */
    public static GovernedArgumentLayout governedLayout(String id) {
        GovernedArgumentLayout layout = INSTANCE.governedLayouts.get(id);
        if (layout == null) throw new IllegalArgumentException("Unknown governed argument layout: " + id);
        return layout;
    }

    /** Anonymous computed ROW fields only; explicit NamedStruct DFS names are never synthesized. */
    public static String anonymousComputedField(int ordinal) {
        return GeneratedTypeLayoutNaming.anonymousComputedField(ordinal);
    }

    /** The actual Worker callback registers precisely this declared public metadata. */
    public static FunctionMetadata callbackMetadata(String name) {
        return callbackMetadata("ai", name);
    }

    /** Declared callback semantics for a SQL name; this does not admit an actual Native implementation. */
    public static Optional<FunctionMetadata> callbackMetadataForSqlName(String name) {
        return sqlBinding(FunctionKind.FUNCTION_KIND_SCALAR, name, "")
                .filter(reference -> binding("SCALAR", reference.namespace(), reference.name())
                        .has("runtimeSignature"))
                .map(reference -> callbackMetadata(reference.namespace(), reference.name()));
    }

    /** Same callback contract entry point for FILE and other declared namespaces. */
    public static FunctionMetadata callbackMetadata(String namespace, String name) {
        JsonNode entry = binding("SCALAR", namespace, name);
        JsonNode runtime = runtimeSignature(entry);
        return new FunctionMetadata(BindingDeclarationJson.text(entry, "implementation"),
                BindingDeclarationJson.text(runtime, "returnType"), callbackArgumentSignatures(namespace, name).get(0),
                runtime.get("deterministic").booleanValue(), runtime.get("defaultNullBehavior").booleanValue(),
                BindingDeclarationJson.text(runtime, "description"));
    }

    /** Public normalized overloads only; governed hidden layout is a separate exact contract. */
    public static List<List<String>> callbackArgumentSignatures(String name) {
        return callbackArgumentSignatures("ai", name);
    }

    /** Explicit namespace prevents guessing FILE/AI routing from a function name. */
    public static List<List<String>> callbackArgumentSignatures(String namespace, String name) {
        JsonNode runtime = runtimeSignature(binding("SCALAR", namespace, name));
        if (runtime.has("argumentLayout")) {
            return List.of(governedLayout(BindingDeclarationJson.text(runtime, "argumentLayout")).signature());
        }
        BindingDeclarationValidation.array(runtime.get("argumentSignatures"));
        List<List<String>> signatures = new ArrayList<>();
        for (JsonNode signature : runtime.get("argumentSignatures")) {
            BindingDeclarationValidation.array(signature);
            List<String> arguments = new ArrayList<>();
            signature.forEach(type -> {
                if (!type.isTextual() || type.textValue().isEmpty()) {
                    throw new IllegalArgumentException("Callback signature requires explicit canonical SQL types");
                }
                arguments.add(type.textValue());
            });
            if (arguments.isEmpty()) throw new IllegalArgumentException("Callback argument signature cannot be absent");
            signatures.add(List.copyOf(arguments));
        }
        if (signatures.isEmpty()) throw new IllegalArgumentException("Callback signatures cannot be absent");
        return List.copyOf(signatures);
    }

    private static JsonNode runtimeSignature(JsonNode entry) {
        JsonNode runtime = entry.get("runtimeSignature");
        if (!entry.has("registrationDependency") || runtime == null) {
            throw new IllegalArgumentException("No declared actual callback registration dependency");
        }
        BindingDeclarationJson.keys(runtime,
                Set.of("returnType", "deterministic", "defaultNullBehavior", "description"),
                Set.of("argumentSignatures", "argumentLayout"));
        if (runtime.has("argumentSignatures") == runtime.has("argumentLayout")) {
            throw new IllegalArgumentException("Callback requires exactly one declared argument source");
        }
        if (runtime.has("argumentLayout") && !BindingDeclarationJson.text(runtime, "argumentLayout")
                .equals(BindingDeclarationJson.text(entry, "governedLayout"))) {
            throw new IllegalArgumentException("Callback argument layout differs from the binding declaration");
        }
        if (!runtime.get("deterministic").isBoolean() || !runtime.get("defaultNullBehavior").isBoolean()) {
            throw new IllegalArgumentException("Callback determinism and NULL-input policy must be explicit");
        }
        return runtime;
    }

    private static JsonNode binding(String kind, String namespace, String name) {
        JsonNode entry = INSTANCE.bindings.get(kind + ":" + namespace + ":" + name);
        if (entry == null) {
            throw new IllegalArgumentException("Unknown function binding: " + kind + ":" + namespace + ":" + name);
        }
        return entry;
    }

    private static FunctionNormalizedArgument normalizedArgument(JsonNode argument) {
        BindingDeclarationJson.keys(argument, Set.of("ordinal", "name", "type", "constant", "optional",
                "default", "enumValues"), Set.of());
        if (!"VARCHAR".equals(BindingDeclarationJson.text(argument, "type"))
                || !argument.get("constant").isBoolean() || !argument.get("constant").booleanValue()
                || !argument.get("optional").isBoolean() || !argument.get("optional").booleanValue()) {
            throw new IllegalArgumentException("Normalized enum argument must be an optional constant VARCHAR");
        }
        Type type = Type.newBuilder().setString(Type.String.newBuilder()
                .setNullability(Type.Nullability.NULLABILITY_REQUIRED)).build();
        String defaultValue = BindingDeclarationJson.text(argument, "default");
        FunctionNormalizedArgument.Builder builder = FunctionNormalizedArgument.newBuilder()
                .setOrdinal(argument.get("ordinal").intValue())
                .setName(BindingDeclarationJson.text(argument, "name"))
                .setType(FunctionTypeTemplate.newBuilder().setSkeleton(type)).setConstant(true).setOptional(true)
                .setDefaultValue(TypedValue.newBuilder().setType(LogicalType.newBuilder()
                        .setCanonicalSql("VARCHAR").setSubstraitType(type)).setStringValue(defaultValue));
        argument.get("enumValues").forEach(value -> builder.addEnumValues(value.textValue()));
        if (!builder.getEnumValuesList().contains(defaultValue)) {
            throw new IllegalArgumentException("Normalized default is outside its declared enum");
        }
        return builder.build();
    }
}
