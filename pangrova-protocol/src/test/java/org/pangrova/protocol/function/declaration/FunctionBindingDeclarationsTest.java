package org.pangrova.protocol.function.declaration;

import com.google.protobuf.ByteString;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;

import java.nio.ReadOnlyBufferException;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionBindingDeclarationsTest {
    @Test
    void declarationIdentityIsTheExactCanonicalDigestAndCannotBeMutatedByConsumers() throws Exception {
        byte[] expected = MessageDigest.getInstance("SHA-256")
                .digest(BindingDeclarationJson.canonicalBytes(BindingDeclarationJson.load()));
        ByteString identity = FunctionBindingDeclarations.declarationSha256();
        assertThat(identity).hasSize(32).isEqualTo(ByteString.copyFrom(expected));
        byte[] exported = identity.toByteArray();
        exported[0] ^= 1;
        assertThatThrownBy(() -> identity.asReadOnlyByteBuffer().put(0, (byte) 0))
                .isInstanceOf(ReadOnlyBufferException.class);
        assertThat(FunctionBindingDeclarations.declarationSha256())
                .isSameAs(identity).isEqualTo(ByteString.copyFrom(expected));
    }

    @Test
    void unnestIdentityComesFromTheSameCanonicalStructuralDeclaration() {
        var reference = BuiltInCapabilityCatalog.unnestPlanRef();
        assertThat(reference.getCapabilityId()).isEqualTo("pangrova.table.unnest");
        assertThat(reference.getSemanticVersion()).isEqualTo("1.0.0");
        assertThat(reference.getImplementationBuildId()).isEmpty();
        assertThat(HexFormat.of().formatHex(reference.getSignatureSha256().toByteArray()))
                .isEqualTo("d3b04ee20b0ac364c040e3e9a775221cb5e6126e6391d39e749d51ba372cd1c6");
    }

    @Test
    void generateSeriesIdentityIsDeclaredForNativeTableExecution() {
        var reference = BuiltInCapabilityCatalog.generateSeriesPlanRef();
        assertThat(reference.getCapabilityId()).isEqualTo("pangrova.table.generate_series");
        assertThat(reference.getSemanticVersion()).isEqualTo("1.0.0");
        assertThat(reference.getSignatureSha256()).hasSize(32);
        assertThat(reference.getImplementationBuildId()).isEmpty();
    }

    @Test
    void embeddingDefaultAndAllowedPurposesAreDeclaredNotCardinalityDerived() {
        var argument = FunctionBindingDeclarations.normalizedArguments("SCALAR", "ai", "ai_embed").get(0);
        assertThat(argument.getOrdinal()).isEqualTo(2);
        assertThat(argument.getName()).isEqualTo("purpose");
        assertThat(argument.getConstant()).isTrue();
        assertThat(argument.getOptional()).isTrue();
        assertThat(argument.getType().getSkeleton().getString().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_REQUIRED);
        assertThat(argument.getDefaultValue().getStringValue()).isEqualTo("RAW");
        assertThat(argument.getDefaultValue().getType().getSubstraitType())
                .isEqualTo(argument.getType().getSkeleton());
        assertThat(argument.getEnumValuesList()).containsExactly("RAW", "QUERY", "DOCUMENT");
    }

    @Test
    void unknownDeclarationsCannotPretendRuntimeAvailability() {
        assertThatThrownBy(() -> FunctionBindingDeclarations.structuralReference("unknown"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FunctionBindingDeclarations.normalizedArguments("SCALAR", "ai", "unknown"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(FunctionBindingDeclarations.normalizedArguments("SCALAR", "arithmetic", "round")).isEmpty();
    }

    @Test
    void computedRowNamesUseTheDeclaredBridgeRuleNotExplicitSchemaFallback() {
        assertThat(FunctionBindingDeclarations.anonymousComputedField(0)).isEqualTo("field_0");
        assertThat(FunctionBindingDeclarations.anonymousComputedField(42)).isEqualTo("field_42");
        assertThat(FunctionBindingDeclarations.anonymousComputedField(Integer.MAX_VALUE)).isEqualTo("field_2147483647");
        assertThatThrownBy(() -> FunctionBindingDeclarations.anonymousComputedField(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void actualEmbeddingCallbackMetadataAndNormalizedOverloadsHaveTheSameSource() {
        var metadata = FunctionBindingDeclarations.callbackMetadata("ai_embed");
        assertThat(metadata.name()).isEqualTo("ai_embed");
        assertThat(metadata.returnType()).isEqualTo("VECTOR");
        assertThat(metadata.deterministic()).isFalse();
        assertThat(metadata.defaultNullBehavior()).isFalse();
        assertThat(FunctionBindingDeclarations.callbackArgumentSignatures("ai_embed"))
                .containsExactly(java.util.List.of("VARCHAR", "VARCHAR", "VARCHAR"),
                        java.util.List.of("VARCHAR", "ROW", "VARCHAR"));
        assertThat(metadata.argumentTypes()).isEqualTo(
                FunctionBindingDeclarations.callbackArgumentSignatures("ai_embed").get(0));
        assertThatThrownBy(() -> FunctionBindingDeclarations.callbackMetadata("unknown"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void textMediaAndFileOverloadsUseTheSameCallbackDeclarationPath() {
        assertThat(FunctionBindingDeclarations.callbackArgumentSignatures("ai_complete"))
                .containsExactly(java.util.List.of("VARCHAR", "VARCHAR"),
                        java.util.List.of("VARCHAR", "VARCHAR", "VARCHAR"),
                        java.util.List.of("VARCHAR", "VARCHAR", "ROW"));
        assertThat(FunctionBindingDeclarations.callbackMetadata("file", "file_from_bytes").argumentTypes())
                .containsExactly("VARBINARY", "VARCHAR");
        assertThat(FunctionBindingDeclarations.callbackMetadata("file", "file_from_url").returnType())
                .isEqualTo("ROW");
    }

    @Test
    void sqlCallbackMetadataIsTheSameCanonicalRuntimeEntry() {
        int checked = 0;
        for (var entry : BindingDeclarationJson.load().get("bindings")) {
            if (!entry.has("runtimeSignature") || !entry.has("sqlNames")) {
                continue;
            }
            var metadata = FunctionBindingDeclarations.callbackMetadata(
                    entry.get("namespace").textValue(), entry.get("name").textValue());
            assertThat(metadata.defaultNullBehavior())
                    .isEqualTo(entry.get("runtimeSignature").get("defaultNullBehavior").booleanValue());
            for (var name : entry.get("sqlNames")) {
                assertThat(FunctionBindingDeclarations.callbackMetadataForSqlName(name.textValue()))
                        .contains(metadata);
                assertThat(FunctionBindingDeclarations.callbackMetadataForSqlName(
                        name.textValue().toLowerCase(java.util.Locale.ROOT))).contains(metadata);
                checked++;
            }
        }
        assertThat(checked).isPositive();
        assertThat(FunctionBindingDeclarations.callbackMetadataForSqlName("MATCH")).isEmpty();
        assertThat(FunctionBindingDeclarations.callbackMetadataForSqlName("NO_SUCH_CALLBACK")).isEmpty();
    }

    @Test
    void governedCallbackLayoutIsOrderedAndRepeatedPathNamesRemainExact() {
        var layout = FunctionBindingDeclarations.governedLayout("AI_MODEL");
        assertThat(layout.arguments()).hasSize(26);
        assertThat(layout.signature()).containsOnly("VARCHAR");
        assertThat(layout.ordinal("model_id")).isEqualTo(1);
        assertThat(layout.arguments().get(layout.ordinal("model_id")).path())
                .containsExactly("model", "model", "id");
        assertThat(layout.arguments().get(layout.ordinal("reuse_similarity_threshold")).encoding())
                .isEqualTo("EXACT_FLOAT");
        assertThatThrownBy(() -> layout.ordinal("unknown")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FunctionBindingDeclarations.governedLayout("unknown"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void internalRerankCallbackConsumesItsSameOrderedFullDeclaration() {
        var layout = FunctionBindingDeclarations.governedLayout("AI_RERANK");
        assertThat(layout.arguments()).hasSize(17);
        assertThat(FunctionBindingDeclarations.callbackArgumentSignatures("internal", "rerank_score"))
                .containsExactly(layout.signature());
        assertThat(FunctionBindingDeclarations.callbackMetadata("internal", "rerank_score").argumentTypes())
                .isEqualTo(layout.signature());
        assertThat(layout.arguments().get(layout.ordinal("query")).source()).isEqualTo("VALUE_EXPRESSION");
        assertThat(layout.arguments().get(layout.ordinal("provider_attempts")).path())
                .containsExactly("resources", "provider_attempts");
        assertThat(layout.arguments().get(layout.ordinal("max_request_bytes")).path())
                .containsExactly("model", "request_limits", "max_request_bytes");
    }
}
