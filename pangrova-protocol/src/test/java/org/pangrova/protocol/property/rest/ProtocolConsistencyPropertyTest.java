package org.pangrova.protocol.property.rest;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tag;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.pangrova.protocol.rest.config.ResourceLimitPayload;
import org.pangrova.protocol.rest.config.WriteConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based test verifying semantic consistency between REST DTO records
 * and their Protobuf counterparts in {@code pangrova-protocol}.
 *
 * <p>For each paired concept (TaskRequest, ResourceLimit, WriteConfig, TaskStats),
 * the test verifies that field names are semantically equivalent and types are
 * compatible across the two protocol representations.</p>
 *
 * <p><b>Validates: Requirements 2.3, 2.5</b></p>
 */
class ProtocolConsistencyPropertyTest {

    /**
     * Mapping of REST DTO class to its Protobuf counterpart descriptor.
     * Each entry represents a concept that exists in both protocol layers.
     */
    private static final List<ProtocolPair> PROTOCOL_PAIRS = List.of(
            new ProtocolPair(
                    "ResourceLimit",
                    ResourceLimitPayload.class,
                    org.pangrova.protocol.v1.ResourceLimit.getDescriptor()
            ),
            new ProtocolPair(
                    "WriteConfig",
                    WriteConfig.class,
                    org.pangrova.protocol.v1.WriteConfig.getDescriptor()
            ),
            new ProtocolPair(
                    "TaskStats",
                    org.pangrova.protocol.rest.task.TaskStats.class,
                    org.pangrova.protocol.v1.TaskStats.getDescriptor()
            ),
            new ProtocolPair(
                    "TaskRequest",
                    org.pangrova.protocol.rest.task.SubmitTaskRequest.class,
                    org.pangrova.protocol.v1.TaskRequest.getDescriptor()
            )
    );

    /**
     * Fields in the REST DTO that are intentionally absent from the Protobuf
     * definition (e.g., exchange_sources is a REST-only routing concern,
     * output_config is a REST-only buffering concern).
     */
    private static final Map<String, Set<String>> REST_ONLY_FIELDS = Map.of(
            "TaskRequest", Set.of("exchange_sources", "output_config", "dynamic_filters", "split_mode", "vector_search", "coordinator_epoch"),
            "TaskStats", Set.of("compressed_bytes_transferred", "uncompressed_bytes_transferred")
    );

    /**
     * Fields in the Protobuf definition that are intentionally absent from
     * the REST DTO (e.g., deprecated legacy fields, binary plan fragments).
     */
    private static final Map<String, Set<String>> PROTO_ONLY_FIELDS = Map.of(
            "TaskRequest", Set.of("substrait_plan")
    );

    /**
     * Weak types that must not appear in any protocol field.
     * These indicate design issues where structured data is passed as opaque types.
     */
    private static final Set<Class<?>> WEAK_TYPES = Set.of(
            Object.class
    );

    /**
     * Property 9: Every REST DTO field has a semantically equivalent Protobuf field.
     *
     * <p>For any protocol pair, each non-excluded REST record component should
     * have a corresponding Protobuf field with the same snake_case name.</p>
     *
     * <p><b>Validates: Requirements 2.3, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: REST DTO and Protobuf semantic consistency")
    void restFieldsHaveProtobufCounterparts(@ForAll("protocolPairs") ProtocolPair pair) {
        Set<String> protoFieldNames = pair.protoDescriptor().getFields().stream()
                .filter(f -> !f.getOptions().getDeprecated())
                .map(FieldDescriptor::getName)
                .collect(Collectors.toSet());

        Set<String> restOnlyExclusions = REST_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());

        for (RecordComponent component : pair.restClass().getRecordComponents()) {
            String snakeName = camelToSnake(component.getName());
            if (restOnlyExclusions.contains(snakeName)) {
                continue;
            }
            assertThat(protoFieldNames)
                    .as("REST field '%s' (snake: '%s') in %s must have a Protobuf counterpart",
                            component.getName(), snakeName, pair.name())
                    .contains(snakeName);
        }
    }

    /**
     * Property 9: Every non-deprecated Protobuf field has a semantically equivalent REST field.
     *
     * <p>For any protocol pair, each non-excluded, non-deprecated Protobuf field
     * should have a corresponding REST record component with the same name
     * (after camelCase conversion).</p>
     *
     * <p><b>Validates: Requirements 2.3, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: REST DTO and Protobuf semantic consistency")
    void protobufFieldsHaveRestCounterparts(@ForAll("protocolPairs") ProtocolPair pair) {
        Set<String> restFieldNames = Arrays.stream(pair.restClass().getRecordComponents())
                .map(c -> camelToSnake(c.getName()))
                .collect(Collectors.toSet());

        Set<String> protoOnlyExclusions = PROTO_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());

        for (FieldDescriptor field : pair.protoDescriptor().getFields()) {
            if (field.getOptions().getDeprecated()) {
                continue;
            }
            String protoName = field.getName();
            if (protoOnlyExclusions.contains(protoName)) {
                continue;
            }
            assertThat(restFieldNames)
                    .as("Protobuf field '%s' in %s must have a REST DTO counterpart",
                            protoName, pair.name())
                    .contains(protoName);
        }
    }

    /**
     * Property 9: Field types are compatible between REST DTO and Protobuf.
     *
     * <p>For any matched field pair, the REST Java type and Protobuf field type
     * must be compatible (e.g., long ↔ int64, String ↔ string, int ↔ int32,
     * List ↔ repeated, Map ↔ map).</p>
     *
     * <p><b>Validates: Requirements 2.3, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: REST DTO and Protobuf semantic consistency")
    void matchedFieldTypesAreCompatible(@ForAll("protocolPairs") ProtocolPair pair) {
        Map<String, RecordComponent> restFields = buildRestFieldMap(pair);
        Set<String> protoOnlyExclusions = PROTO_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());

        for (FieldDescriptor protoField : pair.protoDescriptor().getFields()) {
            if (protoField.getOptions().getDeprecated() || protoOnlyExclusions.contains(protoField.getName())) {
                continue;
            }
            RecordComponent restComponent = restFields.get(protoField.getName());
            if (restComponent == null) {
                continue; // covered by bidirectional field presence tests
            }
            assertThat(isTypeCompatible(restComponent.getType(), protoField))
                    .as("Field '%s' in %s: REST type %s must be compatible with Protobuf type %s",
                            protoField.getName(), pair.name(),
                            restComponent.getType().getSimpleName(),
                            protoField.getType())
                    .isTrue();
        }
    }

    /**
     * Property 9: No REST DTO field uses a weak type.
     *
     * <p>For any protocol pair, no REST record component should use
     * {@code Object}, which indicates opaque untyped data passing.</p>
     *
     * <p><b>Validates: Requirements 2.3, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: REST DTO and Protobuf semantic consistency")
    void noRestFieldUsesWeakType(@ForAll("protocolPairs") ProtocolPair pair) {
        for (RecordComponent component : pair.restClass().getRecordComponents()) {
            assertThat(WEAK_TYPES)
                    .as("REST field '%s' in %s must not use weak type %s",
                            component.getName(), pair.name(),
                            component.getType().getSimpleName())
                    .doesNotContain(component.getType());
        }
    }

    /**
     * Property 9: Field count consistency between REST and Protobuf.
     *
     * <p>For any protocol pair, the number of semantically matched fields
     * (excluding known exclusions and deprecated fields) should be equal,
     * ensuring no silent field drift between the two representations.</p>
     *
     * <p><b>Validates: Requirements 2.3, 2.5</b></p>
     */
    @Property(tries = 10)
    @Tag("Feature: full-pipeline-audit-fix, Property 9: REST DTO and Protobuf semantic consistency")
    void fieldCountIsConsistent(@ForAll("protocolPairs") ProtocolPair pair) {
        Set<String> restOnlyExclusions = REST_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());
        Set<String> protoOnlyExclusions = PROTO_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());

        long restFieldCount = Arrays.stream(pair.restClass().getRecordComponents())
                .map(c -> camelToSnake(c.getName()))
                .filter(name -> !restOnlyExclusions.contains(name))
                .count();

        long protoFieldCount = pair.protoDescriptor().getFields().stream()
                .filter(f -> !f.getOptions().getDeprecated())
                .filter(f -> !protoOnlyExclusions.contains(f.getName()))
                .count();

        assertThat(restFieldCount)
                .as("Matched field count for %s: REST (%d) vs Protobuf (%d)",
                        pair.name(), restFieldCount, protoFieldCount)
                .isEqualTo(protoFieldCount);
    }

    @Provide
    Arbitrary<ProtocolPair> protocolPairs() {
        return Arbitraries.of(PROTOCOL_PAIRS);
    }

    // --- Helper methods ---

    /**
     * Builds a map from snake_case field name to REST RecordComponent
     * for efficient lookup during type compatibility checks.
     */
    private static Map<String, RecordComponent> buildRestFieldMap(ProtocolPair pair) {
        Map<String, RecordComponent> map = new LinkedHashMap<>();
        Set<String> restOnlyExclusions = REST_ONLY_FIELDS.getOrDefault(pair.name(), Set.of());
        for (RecordComponent component : pair.restClass().getRecordComponents()) {
            String snakeName = camelToSnake(component.getName());
            if (!restOnlyExclusions.contains(snakeName)) {
                map.put(snakeName, component);
            }
        }
        return map;
    }

    /**
     * Checks whether a Java type from a REST record component is compatible
     * with the corresponding Protobuf field descriptor type.
     */
    private static boolean isTypeCompatible(Class<?> javaType, FieldDescriptor protoField) {
        // Handle repeated fields (List in Java)
        if (protoField.isRepeated() && !protoField.isMapField()) {
            return javaType == List.class;
        }
        // Handle map fields
        if (protoField.isMapField()) {
            return javaType == Map.class;
        }
        // Handle message fields (nested Protobuf messages map to nested records)
        if (protoField.getType() == FieldDescriptor.Type.MESSAGE) {
            return javaType.isRecord() || javaType == String.class;
        }
        // Scalar type compatibility
        return switch (protoField.getType()) {
            case INT64 -> javaType == long.class || javaType == Long.class;
            case INT32 -> javaType == int.class || javaType == Integer.class;
            case STRING -> javaType == String.class;
            case BOOL -> javaType == boolean.class || javaType == Boolean.class;
            case DOUBLE -> javaType == double.class || javaType == Double.class;
            case FLOAT -> javaType == float.class || javaType == Float.class;
            case BYTES -> javaType == byte[].class || javaType == String.class;
            case ENUM -> javaType == String.class || javaType.isEnum();
            default -> false;
        };
    }

    /**
     * Converts a camelCase Java identifier to snake_case for comparison
     * with Protobuf field names.
     */
    static String camelToSnake(String camel) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camel.length(); i++) {
            char c = camel.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /**
     * Immutable record representing a paired REST DTO and Protobuf message
     * that should have semantically equivalent fields.
     */
    record ProtocolPair(String name, Class<? extends Record> restClass, Descriptor protoDescriptor) {}
}
