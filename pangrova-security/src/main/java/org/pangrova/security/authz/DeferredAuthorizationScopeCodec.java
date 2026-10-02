package org.pangrova.security.authz;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.pangrova.spi.connector.metadata.table.QualifiedTableName;
import org.pangrova.spi.coordination.WriteDeclarationSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Canonical metadata encoding of exact deferred permissions, without credentials or identity groups. */
public final class DeferredAuthorizationScopeCodec {
    private static final ObjectMapper MAPPER = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final int MAX_BYTES = 1024 * 1024;
    private static final Set<String> PERMISSION_FIELDS = Set.of(
            "catalog", "schema", "table", "columns", "privilege", "readMode", "rowFilter", "columnMasks");
    private static final Set<String> DEFINITION_FIELDS = Set.of(
            "catalog", "schema", "table", "schemaSha256", "generatedColumnsSha256", "hasGeneratedColumns", "models");
    private static final Set<String> MODEL_FIELDS = Set.of(
            "logicalName", "incarnation", "configurationVersion", "deploymentVersion", "signature");

    private DeferredAuthorizationScopeCodec() {}

    /** Builds one receipt from the same scope observation, without a second connector lookup. */
    public static org.pangrova.spi.coordination.WriteAuthorizationReceipt encodeWriteReceipt(
            DeferredAuthorizationScope scope) {
        var declaration = scope.definitionSnapshot().orElseThrow(() -> new IllegalArgumentException(
                "A write receipt requires its actual connector definition snapshot")).declaration();
        return new org.pangrova.spi.coordination.WriteAuthorizationReceipt(encode(scope), declaration);
    }

    /** Restores the declaration from the sole persisted scope, rejecting historical unscoped write records. */
    public static org.pangrova.spi.coordination.WriteAuthorizationReceipt decodeWriteReceipt(byte[] bytes) {
        var scope = decode(bytes);
        var receipt = encodeWriteReceipt(scope);
        if (!java.util.Arrays.equals(receipt.authorizationScope(), bytes)) {
            throw new IllegalArgumentException("Write receipt must use canonical exact authority encoding");
        }
        return receipt;
    }

    /** Serializes observed content deterministically for metadata persistence and hashing. */
    public static byte[] encode(DeferredAuthorizationScope scope) {
        var root = MAPPER.createObjectNode();
        var permissionNodes = root.putArray("permissions");
        for (var permission : scope.permissions()) {
            var node = permissionNodes.addObject();
            node.put("catalog", permission.table().catalog());
            node.put("schema", permission.table().schema());
            node.put("table", permission.table().table());
            var columns = node.putArray("columns");
            new TreeSet<>(permission.columns()).forEach(columns::add);
            node.put("privilege", permission.privilege().name());
            node.put("readMode", permission.readMode().name());
            permission.observedPolicy().rowFilter().ifPresentOrElse(
                    value -> node.put("rowFilter", value), () -> node.putNull("rowFilter"));
            var masks = node.putObject("columnMasks");
            new TreeMap<>(permission.observedPolicy().columnMasks()).forEach((column, expression) ->
                    expression.ifPresentOrElse(value -> masks.put(column, value), () -> masks.putNull(column)));
        }
        scope.definitionSnapshot().ifPresentOrElse(snapshot -> writeDefinition(root.putObject("definition"), snapshot),
                () -> root.putNull("definition"));
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(root);
            if (bytes.length > MAX_BYTES) {
                throw new IllegalArgumentException("Deferred scope exceeds the metadata size limit");
            }
            return bytes;
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot encode deferred authorization scope", failure);
        }
    }

    /** Rejects missing, corrupt or unbounded scopes; unscoped historical jobs cannot replay. */
    public static DeferredAuthorizationScope decode(byte[] bytes) {
        requireBoundedBytes(bytes);
        try {
            JsonNode root = MAPPER.readTree(bytes);
            requireFields(root, Set.of("permissions", "definition"));
            var permissionNodes = root.required("permissions");
            if (!permissionNodes.isArray() || permissionNodes.isEmpty() || permissionNodes.size() > 256) {
                throw new IllegalArgumentException("Deferred authorization requires bounded table permissions");
            }
            var permissions = new ArrayList<DeferredAuthorizationScope.TablePermission>();
            for (var node : permissionNodes) {
                permissions.add(readPermission(node));
            }
            var definition = root.required("definition");
            return new DeferredAuthorizationScope(permissions,
                    definition.isNull() ? Optional.empty() : Optional.of(readDefinition(definition)));
        } catch (IOException failure) {
            throw new IllegalArgumentException("Cannot decode deferred authorization scope", failure);
        }
    }

    private static void writeDefinition(com.fasterxml.jackson.databind.node.ObjectNode node,
                                        DeferredAuthorizationScope.DefinitionSnapshot snapshot) {
        var declaration = snapshot.declaration();
        node.put("catalog", declaration.table().catalog());
        node.put("schema", declaration.table().schema());
        node.put("table", declaration.table().table());
        node.put("schemaSha256", declaration.schemaSha256());
        node.put("generatedColumnsSha256", declaration.generatedColumnsSha256());
        node.put("hasGeneratedColumns", declaration.hasGeneratedColumns());
        var models = node.putArray("models");
        snapshot.models().stream().sorted(java.util.Comparator.comparing(
                DeferredAuthorizationScope.ModelSnapshot::logicalName)).forEach(model -> {
                    var value = models.addObject();
                    value.put("logicalName", model.logicalName());
                    value.put("incarnation", model.incarnation());
                    value.put("configurationVersion", model.configurationVersion());
                    value.put("deploymentVersion", model.deploymentVersion());
                    value.put("signature", model.signature());
                });
    }

    private static DeferredAuthorizationScope.DefinitionSnapshot readDefinition(JsonNode node) {
        requireFields(node, DEFINITION_FIELDS);
        var modelNodes = node.required("models");
        var hasGeneratedColumns = node.required("hasGeneratedColumns");
        if (!modelNodes.isArray() || modelNodes.size() > 256 || !hasGeneratedColumns.isBoolean()) {
            throw new IllegalArgumentException("Definition requires bounded models and an explicit declaration flag");
        }
        var models = new ArrayList<DeferredAuthorizationScope.ModelSnapshot>();
        for (var model : modelNodes) {
            requireFields(model, MODEL_FIELDS);
            var version = model.required("configurationVersion");
            if (!version.isIntegralNumber() || !version.canConvertToInt()) {
                throw new IllegalArgumentException("Model configuration version must be an exact int");
            }
            models.add(new DeferredAuthorizationScope.ModelSnapshot(text(model.required("logicalName")),
                    text(model.required("incarnation")), version.intValue(),
                    text(model.required("deploymentVersion")), text(model.required("signature"))));
        }
        return new DeferredAuthorizationScope.DefinitionSnapshot(new WriteDeclarationSnapshot(
                new QualifiedTableName(
                text(node.required("catalog")), text(node.required("schema")), text(node.required("table"))),
                text(node.required("schemaSha256")), text(node.required("generatedColumnsSha256")),
                hasGeneratedColumns.booleanValue()), models);
    }

    private static void requireBoundedBytes(byte[] bytes) {
        if (bytes == null || bytes.length == 0 || bytes.length > MAX_BYTES) {
            throw new IllegalArgumentException("Deferred authorization scope is missing or oversized");
        }
    }

    private static DeferredAuthorizationScope.TablePermission readPermission(JsonNode node) {
        requirePermissionFields(node);
        Set<String> columns = new TreeSet<>();
        var columnNode = node.required("columns");
        if (!columnNode.isArray() || columnNode.size() > DeferredAuthorizationScope.MAX_COLUMNS) {
            throw new IllegalArgumentException("Deferred read columns must be bounded");
        }
        columnNode.forEach(column -> columns.add(text(column)));
        Map<String, Optional<String>> masks = new TreeMap<>();
        var maskNode = node.required("columnMasks");
        if (!maskNode.isObject()) {
            throw new IllegalArgumentException("Deferred policy masks must be an object");
        }
        maskNode.fields().forEachRemaining(entry -> masks.put(entry.getKey(), optionalText(entry.getValue())));
        return new DeferredAuthorizationScope.TablePermission(
                new QualifiedTableName(text(node.required("catalog")), text(node.required("schema")),
                        text(node.required("table"))), columns,
                Privilege.valueOf(text(node.required("privilege"))),
                DeferredAuthorizationScope.ReadMode.valueOf(text(node.required("readMode"))),
                new DeferredAuthorizationScope.PolicySnapshot(optionalText(node.required("rowFilter")), masks));
    }

    private static void requirePermissionFields(JsonNode node) {
        requireFields(node, PERMISSION_FIELDS);
    }

    private static void requireFields(JsonNode node, Set<String> expected) {
        if (node == null || !node.isObject()) {
            throw new IllegalArgumentException("Deferred scope fields require an object");
        }
        Set<String> fields = new TreeSet<>();
        node.fieldNames().forEachRemaining(fields::add);
        if (!fields.equals(expected)) {
            throw new IllegalArgumentException("Deferred scope permission fields differ from the canonical contract");
        }
    }

    private static Optional<String> optionalText(JsonNode node) {
        return node.isNull() ? Optional.empty() : Optional.of(text(node));
    }

    private static String text(JsonNode node) {
        if (!node.isTextual()) {
            throw new IllegalArgumentException("Deferred scope field must be text");
        }
        return node.textValue();
    }
}
