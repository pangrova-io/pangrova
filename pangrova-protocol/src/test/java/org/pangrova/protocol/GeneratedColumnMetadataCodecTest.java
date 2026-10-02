package org.pangrova.protocol;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.v2.LogicalType;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GeneratedColumnMetadataCodecTest {

    @Test
    void roundTripsGenericCapabilityDefinition() {
        GeneratedColumnSet definitions = oneDefinition();

        Map<String, String> properties = GeneratedColumnMetadataCodec.encode(definitions);

        assertThat(GeneratedColumnMetadataCodec.decode(properties)).isEqualTo(definitions);
    }

    @Test
    void rejectsPartialOrModifiedCatalogPayload() {
        Map<String, String> properties = new LinkedHashMap<>(
                GeneratedColumnMetadataCodec.encode(oneDefinition()));
        String digest = properties.get(GeneratedColumnMetadataCodec.SHA256_PROPERTY);
        properties.put(GeneratedColumnMetadataCodec.SHA256_PROPERTY,
                (digest.charAt(0) == 'A' ? "B" : "A") + digest.substring(1));

        assertThatThrownBy(() -> GeneratedColumnMetadataCodec.decode(properties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-256 mismatch");

        assertThatThrownBy(() -> GeneratedColumnMetadataCodec.decode(Map.of(
                GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY,
                properties.get(GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be present together");
    }

    @Test
    void rejectsDuplicateTargetsAndUnspecifiedMode() {
        GeneratedColumnDefinition definition = definition("embedding")
                .toBuilder()
                .setExecutionMode(ExecutionMode.EXECUTION_MODE_UNSPECIFIED)
                .build();
        GeneratedColumnSet invalidMode = GeneratedColumnSet.newBuilder()
                .setSchemaVersion(1)
                .addDefinitions(definition)
                .build();
        assertThatThrownBy(() -> GeneratedColumnMetadataCodec.encode(invalidMode))
                .hasMessageContaining("execution mode");

        GeneratedColumnSet duplicate = GeneratedColumnSet.newBuilder()
                .setSchemaVersion(1)
                .addDefinitions(definition("embedding"))
                .addDefinitions(definition("EMBEDDING"))
                .build();
        assertThatThrownBy(() -> GeneratedColumnMetadataCodec.encode(duplicate))
                .hasMessageContaining("duplicate generated target column");
    }

    private static GeneratedColumnSet oneDefinition() {
        return GeneratedColumnSet.newBuilder()
                .setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION)
                .addDefinitions(definition("embedding"))
                .build();
    }

    private static GeneratedColumnDefinition definition(String target) {
        return GeneratedColumnDefinition.newBuilder()
                .setTargetColumn(target)
                .setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setLogicalModelName("text-embedding")
                .addSourceColumns("title")
                .addSourceColumns("body")
                .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                .setOutputType(LogicalType.newBuilder()
                        .setCanonicalSql("VECTOR(768, COSINE)"))
                .build();
    }
}
