package org.pangrova.execution.write;

import org.pangrova.query.api.write.WriteAuthorityRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnDefinition;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.security.api.authorization.DeferredAuthorizationScope;
import org.pangrova.security.api.enums.Privilege;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.metadata.table.QualifiedTableName;
import org.pangrova.connector.api.commit.WriteAuthorizationReceipt;
import org.pangrova.connector.api.commit.WriteDeclarationSnapshot;
import org.pangrova.security.api.exception.AccessDeniedException;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Receipt content/pre-allocation contract tests; current authorization is tested with real PostgreSQL separately. */
class WriteAuthorityAdmissionTest {
    private static final QualifiedTableName TABLE = new QualifiedTableName("catalog", "schema", "documents");
    private static final GeneratedColumnSet ORDINARY = GeneratedColumnSet.newBuilder()
            .setSchemaVersion(GeneratedColumnMetadataCodec.SCHEMA_VERSION).build();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void everyWriteRequiresInstalledAuthorityProducer(boolean generated) {
        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(definitions(generated)), definitions(generated), null))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("before connector allocation");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void acceptsExactlyOneMatchingProducerObservation(boolean generated) {
        GeneratedColumnSet definitions = definitions(generated);
        WriteAuthorizationReceipt receipt = receipt(TABLE, definitions, generated);
        AtomicInteger calls = new AtomicInteger();

        var admitted = WriteAuthorityAdmission.resolve(request(definitions), definitions, request -> {
            calls.incrementAndGet();
            return receipt;
        });

        assertThat(admitted).isSameAs(receipt);
        assertThat(calls).hasValue(1);
    }

    @Test
    void rejectsReceiptForAnotherTable() {
        var receipt = receipt(new QualifiedTableName("catalog", "schema", "another"), ORDINARY, false);

        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(), ORDINARY, ignored -> receipt))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("current table declaration");
    }

    @Test
    void rejectsDeclarationAbsenceWhenTableHasGeneratedColumns() {
        var receipt = receipt(TABLE, ORDINARY, false);

        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(
                request(definitions(true)), definitions(true), ignored -> receipt))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsChangedDefinitionWithUnchangedCount() {
        GeneratedColumnSet original = definitions(true);
        GeneratedColumnSet changed = original.toBuilder().setDefinitions(0,
                original.getDefinitions(0).toBuilder().setLogicalModelName("another-model")).build();
        var receipt = receipt(TABLE, original, true);

        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(changed), changed, ignored -> receipt))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void rejectsNewSchemaReceiptEvenWhenDefinitionBytesAreUnchanged() {
        var original = receipt(TABLE, ORDINARY, false);
        var changed = new WriteDeclarationSnapshot(TABLE, "c".repeat(64),
                original.declaration().generatedColumnsSha256(), false);
        var receipt = new WriteAuthorizationReceipt(original.authorizationScope(), changed);
        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(), ORDINARY, ignored -> receipt))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("current table declaration");
    }

    @Test
    void rejectsInvalidDefinitionsBeforeCallingProducer() {
        AtomicInteger calls = new AtomicInteger();

        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(
                request(), GeneratedColumnSet.getDefaultInstance(), ignored -> {
                    calls.incrementAndGet();
                    return receipt(TABLE, ORDINARY, false);
                })).isInstanceOf(IllegalArgumentException.class);
        assertThat(calls).hasValue(0);
    }

    @Test
    void nullReceiptAndProducerFailureCannotAdmitWrite() {
        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(), ORDINARY, ignored -> null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("receipt");
        var denied = new AccessDeniedException("Current role revoked");
        assertThatThrownBy(() -> WriteAuthorityAdmission.resolve(request(), ORDINARY, ignored -> {
            throw denied;
        })).isSameAs(denied);
    }

    @Test
    void requestRequiresAuthenticatedPrincipalAndExplicitWriteMode() {
        assertThatThrownBy(() -> new WriteAuthorityRequest(
                TABLE, new TestSession(""), "INSERT", List.of(), Set.of(Privilege.INSERT), request().compiledDeclaration(), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WriteAuthorityRequest(
                TABLE, new TestSession("writer"), " ", List.of(), Set.of(Privilege.INSERT), request().compiledDeclaration(), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requestRejectsMissingOrDifferentCompiledTarget() {
        assertThatThrownBy(() -> new WriteAuthorityRequest(
                TABLE, new TestSession("writer"), "INSERT", List.of(), Set.of(Privilege.INSERT), null, false))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("compiledDeclaration");
        var other = receipt(new QualifiedTableName("catalog", "schema", "other"), ORDINARY, false).declaration();
        assertThatThrownBy(() -> new WriteAuthorityRequest(
                TABLE, new TestSession("writer"), "INSERT", List.of(), Set.of(Privilege.INSERT), other, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("requested table");
    }

    private static WriteAuthorityRequest request() {
        return request(ORDINARY);
    }

    private static WriteAuthorityRequest request(GeneratedColumnSet definitions) {
        return new WriteAuthorityRequest(
                TABLE, new TestSession("writer"), "INSERT", List.of(), Set.of(Privilege.INSERT),
                receipt(TABLE, definitions, definitions.getDefinitionsCount() != 0).declaration(), false);
    }

    private static GeneratedColumnSet definitions(boolean generated) {
        return generated ? ORDINARY.toBuilder().addDefinitions(GeneratedColumnDefinition.newBuilder()
                .setTargetColumn("embedding").setCapability(BuiltInCapabilityCatalog.aiEmbeddingPlanRef())
                .setLogicalModelName("model").addSourceColumns("title")
                .setExecutionMode(ExecutionMode.EXECUTION_MODE_ASYNC)
                .setOutputType(VectorLogicalType.create(768))).build() : ORDINARY;
    }

    private static WriteAuthorizationReceipt receipt(QualifiedTableName table,
            GeneratedColumnSet definitions, boolean generated) {
        var declaration = new WriteDeclarationSnapshot(table, "a".repeat(64),
                WriteDeclarationSnapshot.generatedColumnsDigest(definitions.toByteArray()), generated);
        // Decoding/current authorization is tested at the real PostgreSQL boundary, not by this pure gate.
        return new WriteAuthorizationReceipt(new byte[] {1}, declaration);
    }

    private record TestSession(String getUser) implements ConnectorSession {
        @Override
        public java.util.Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority> getQueryAuthority() {
            return java.util.Optional.empty();
        }

        @Override public Optional<String> getCatalog() { return Optional.of(TABLE.catalog()); }
        @Override public Optional<String> getSchema() { return Optional.of(TABLE.schema()); }
        @Override public Map<String, String> getProperties() { return Map.of("tenant_id", "tenant"); }
    }
}
