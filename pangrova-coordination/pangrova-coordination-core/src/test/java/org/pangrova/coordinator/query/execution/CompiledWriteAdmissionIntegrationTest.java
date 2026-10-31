package org.pangrova.coordinator.query.execution;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.coordinator.query.session.CoordinatorSessionContext;
import org.pangrova.security.api.AccessDeniedException;
import org.pangrova.security.api.UserIdentity;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual protected SQL compilation and SDK evolution; stops at the real allocation boundary. */
class CompiledWriteAdmissionIntegrationTest extends ContainerTestBase {
    @TempDir Path warehouse;

    @Test
    void valuesRetainsItsRealTargetDeclarationWithoutInventingSourceReadPermission() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            var compiled = fixture.compileProtectedWrite(actor(),
                    "INSERT INTO catalog.schema.documents (id, title) VALUES (1, 'text')");
            assertThat(compiled.reads()).isEmpty();
            assertThat(compiled.declaration()).isEqualTo(fixture.declaration());
            fixture.installOrdinaryAuthority();
            fixture.engine.ensureWriteContextRegistered(actor(), context(compiled));
            assertThat(fixture.observations).singleElement().satisfies(request -> {
                assertThat(request.compiledDeclaration()).isEqualTo(compiled.declaration());
                assertThat(request.observedReadPermissions()).isEmpty();
            });
            assertThat(fixture.metadata.inserts).isEqualTo(1);
        }
    }

    @Test
    void schemaEvolutionAfterProtectedCompilationRejectsBeforeAuthorityOrSdkAllocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            var compiled = fixture.compileProtectedInsert(actor());
            fixture.installOrdinaryAuthority();
            fixture.evolveActualSchema();
            rejectsBeforeAllocation(fixture, compiled);
        }
    }

    @Test
    void declarationAddedAfterProtectedCompilationRejectsBeforeAuthorityOrSdkAllocation() throws Exception {
        try (var fixture = new SchedulingWriteFixture(warehouse, false)) {
            var compiled = fixture.compileProtectedInsert(actor());
            fixture.installOrdinaryAuthority();
            fixture.evolveActualDeclaration();
            rejectsBeforeAllocation(fixture, compiled);
        }
    }

    private static void rejectsBeforeAllocation(SchedulingWriteFixture fixture, WritePolicyObservation compiled) {
        assertThatThrownBy(() -> fixture.engine.ensureWriteContextRegistered(actor(), context(compiled)))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed before admission");
        assertThat(fixture.observations).isEmpty();
        assertThat(fixture.metadata.inserts).isZero();
        assertThat(fixture.metadata.overwrites).isZero();
        assertThat(fixture.tracker.hasWriteContext(actor().queryId())).isFalse();
    }

    private static DmlWriteContext context(WritePolicyObservation compiled) {
        return new DmlWriteContext(SchedulingWriteFixture.TABLE, "INSERT", List.of(), Optional.of(compiled));
    }

    private static CoordinatorSessionContext actor() {
        return new CoordinatorSessionContext("compiled-write", Map.of(),
                new UserIdentity("alice", Set.of(), Map.of(), Instant.EPOCH), "tenant-1");
    }
}
