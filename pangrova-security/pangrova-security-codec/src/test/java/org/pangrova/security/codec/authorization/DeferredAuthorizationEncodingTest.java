package org.pangrova.security.codec.authorization;

import org.pangrova.security.api.authorization.DeferredAuthorizationScope;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pins persisted authority bytes independently of Java package and implementation ownership. */
class DeferredAuthorizationEncodingTest {

    @Test
    void rawAuthorityPreservesPersistedBytesAndDigest() throws IOException {
        var table = new QualifiedTableName("lake", "sales", "facts");
        var scope = DeferredAuthorizationScope.raw(table, Set.of("b", "a"), table);
        byte[] persisted = fixture("raw");
        assertThat(DeferredAuthorizationScopeCodec.encode(scope)).isEqualTo(persisted);
        assertThat(DeferredAuthorizationScopeCodec.decode(persisted)).isEqualTo(scope);
        assertThat(DeferredAuthorizationScopeCodec.sha256(scope)).isEqualTo("187a40a6ce1cf430e8d3387bb3490f396bfe4632b85568e5f491e0e761dafd60");
    }

    @Test
    void generatedWriteReceiptPreservesPersistedDeclarationAndModelIdentity() throws IOException {
        byte[] persisted = fixture("generated-write");
        var receipt = DeferredAuthorizationScopeCodec.decodeWriteReceipt(persisted);
        assertThat(receipt.authorizationScope()).isEqualTo(persisted);
        var scope = DeferredAuthorizationScopeCodec.decode(persisted);
        assertThat(DeferredAuthorizationScopeCodec.encodeWriteReceipt(scope)).isEqualTo(receipt);
        assertThat(scope.definitionSnapshot().orElseThrow().models()).containsExactly(
                new DeferredAuthorizationScope.ModelSnapshot("embed", "incarnation-1", 1,
                        "deployment-1", "a".repeat(64)));
        assertThat(DeferredAuthorizationScopeCodec.sha256(scope)).isEqualTo("00d4ef38da346154aa9c2e919389e276b681e9648bfd1ad8c250d105ba4eaa63");
    }

    @ParameterizedTest
    @ValueSource(strings = {" {}", " null", " true", " 1", " []", " garbage"})
    void rejectsTrailingContentAfterAnOtherwiseValidScope(String trailing) throws IOException {
        byte[] corrupt = (new String(fixture("raw"), StandardCharsets.UTF_8) + trailing)
                .getBytes(StandardCharsets.UTF_8);
        assertThatThrownBy(() -> DeferredAuthorizationScopeCodec.decode(corrupt))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void permitsWhitespaceAfterOneCompleteScope() throws IOException {
        byte[] persisted = fixture("raw");
        byte[] spaced = (new String(persisted, StandardCharsets.UTF_8) + "\n\t ")
                .getBytes(StandardCharsets.UTF_8);
        assertThat(DeferredAuthorizationScopeCodec.decode(spaced))
                .isEqualTo(DeferredAuthorizationScopeCodec.decode(persisted));
    }

    private static byte[] fixture(String name) throws IOException {
        try (var input = DeferredAuthorizationEncodingTest.class.getResourceAsStream(
                "/authorization/" + name + ".json")) {
            if (input == null) {
                throw new IOException("Missing persisted authorization fixture: " + name);
            }
            return input.readAllBytes();
        }
    }
}
