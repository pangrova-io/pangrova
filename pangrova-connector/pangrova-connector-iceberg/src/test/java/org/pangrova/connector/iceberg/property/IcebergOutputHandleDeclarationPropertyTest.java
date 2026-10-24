package org.pangrova.connector.iceberg.property;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Provide;
import net.jqwik.api.Property;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Types;
import org.pangrova.connector.iceberg.handle.IcebergDeclarationProperties;
import org.pangrova.connector.iceberg.handle.IcebergOutputHandleCodec;
import org.pangrova.connector.iceberg.handle.IcebergOutputTableHandle;
import org.pangrova.connector.iceberg.handle.IcebergOverwriteScope;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Canonical handle bytes preserve declarations and reject loss, reordering and old codecs. */
class IcebergOutputHandleDeclarationPropertyTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.optional(1, "embedding",
            Types.ListType.ofRequired(2, Types.FloatType.get())));
    private final IcebergOutputHandleCodec codec = new IcebergOutputHandleCodec();

    @Property(tries = 100)
    void declarationRoundTripAndDigestAreExact(@ForAll("dimensions") int dimension) {
        var properties = IcebergDeclarationProperties.capture(
                Map.of("pangrova.vector.dimension.embedding", String.valueOf(dimension), "secret.token", "hidden"));
        var handle = handle(properties);
        byte[] encoded = codec.serialize(handle);
        assertThat(codec.deserialize(codec.codecVersion(), encoded)).isEqualTo(handle);
        assertThat(handle.declarationProperties()).doesNotContainKey("secret.token");
        var reordered = new LinkedHashMap<String, String>();
        properties.entrySet().stream().sorted(Map.Entry.<String, String>comparingByKey().reversed())
                .forEach(entry -> reordered.put(entry.getKey(), entry.getValue()));
        assertThat(codec.serialize(handle(reordered))).isEqualTo(encoded);
        var changed = new LinkedHashMap<>(properties);
        changed.put("pangrova.vector.dimension.embedding", String.valueOf(dimension + 1));
        assertThat(codec.serialize(handle(changed))).isNotEqualTo(encoded);
    }

    @Property(tries = 100)
    void canonicalPayloadRejectsTruncationTrailingBytesAndPreviousVersion(@ForAll("dimensions") int dimension) {
        byte[] payload = codec.serialize(handle(IcebergDeclarationProperties.capture(
                Map.of("pangrova.vector.dimension.embedding", String.valueOf(dimension)))));
        for (int cut = 0; cut < payload.length; cut++) {
            byte[] truncated = Arrays.copyOf(payload, cut);
            assertThatThrownBy(() -> codec.deserialize(codec.codecVersion(), truncated))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> codec.deserialize(3, payload)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> codec.deserialize(codec.codecVersion(), Arrays.copyOf(payload, payload.length + 1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handle(Map.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Provide
    Arbitrary<Integer> dimensions() { return Arbitraries.integers().between(1, 65534); }

    private IcebergOutputTableHandle handle(Map<String, String> properties) {
        return new IcebergOutputTableHandle("db", "events", "00000000-0000-0000-0000-000000000001",
                "file:///warehouse/events", SchemaParser.toJson(SCHEMA),
                PartitionSpecParser.toJson(PartitionSpec.unpartitioned()), 2, -1, "INSERT",
                IcebergOverwriteScope.NONE, Map.of(), properties);
    }
}
