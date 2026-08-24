package org.pangrova.coordinator.property.training;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import org.pangrova.coordinator.ai.training.TrainingDataRequest;
import org.pangrova.coordinator.ai.training.TrainingDataRequest.VersionReference;
import org.pangrova.coordinator.ai.training.TrainingDataServer;
import org.pangrova.coordinator.ai.training.TrainingDataServerConfig;
import org.pangrova.observability.noop.NoOpMetricsProvider;
import org.pangrova.spi.security.AccessControl;
import org.pangrova.spi.security.UserIdentity;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for version pinning (P7).
 *
 * <p>Validates that the same version reference always resolves to the same data.
 * When a training data request specifies a tag, branch, or snapshot version,
 * repeated requests with the same version must produce identical endpoint
 * configurations and ticket content.</p>
 *
 * <p><b>Validates: Requirements P7</b></p>
 */
class VersionPinningPropertyTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Provide
    Arbitrary<String> versionTypes() {
        return Arbitraries.of("tag", "branch", "snapshot");
    }

    @Provide
    Arbitrary<String> versionValues() {
        return Arbitraries.of(
                "training_v1", "training_v3", "main", "experiment_42",
                "100", "200", "snapshot_abc", "release_2024");
    }

    @Provide
    Arbitrary<String> tableNames() {
        return Arbitraries.of(
                "catalog.schema.knowledge_base",
                "ml_catalog.public.documents",
                "prod.features.product_embeddings");
    }

    @Provide
    Arbitrary<List<String>> columnLists() {
        return Arbitraries.of(
                List.of("id", "content", "embedding"),
                List.of("id", "embedding"),
                List.of("id", "title", "content", "category", "embedding"));
    }

    /**
     * P7: The same version reference always parses to the same type and value.
     * Version parsing is deterministic and idempotent.
     */
    @Property(tries = 100)
    void versionReferenceParsesIdenticallyOnRepeatedCalls(
            @ForAll("versionTypes") String type,
            @ForAll("versionValues") String value) {

        String versionString = type + ":" + value;
        TrainingDataRequest request = new TrainingDataRequest(
                "catalog.schema.table", versionString, null, null, null);

        VersionReference ref1 = request.parsedVersion();
        VersionReference ref2 = request.parsedVersion();

        assertThat(ref1.type()).isEqualTo(ref2.type());
        assertThat(ref1.value()).isEqualTo(ref2.value());
        assertThat(ref1.type()).isEqualTo(type);
        assertThat(ref1.value()).isEqualTo(value);
    }

    /**
     * P7: Repeated getFlightInfo calls with the same version-pinned request
     * produce identical endpoint tickets. The server does not introduce
     * non-determinism in endpoint planning for the same request.
     */
    @Property(tries = 50)
    void sameVersionedRequestProducesIdenticalEndpoints(
            @ForAll("tableNames") String table,
            @ForAll("versionTypes") String versionType,
            @ForAll("versionValues") String versionValue,
            @ForAll("columnLists") List<String> columns) {

        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9090, 10, "jwt", 65536);
        TrainingDataServer server = new TrainingDataServer(
                config, new PermitAllAccessControl(), new NoOpMetricsProvider());
        server.start();

        try {
            String version = versionType + ":" + versionValue;
            byte[] command = buildCommand(table, version, null, columns);

            UserIdentity identity = testIdentity();

            List<TrainingDataServer.EndpointTicket> endpoints1 = server.getFlightInfo(command, identity);
            List<TrainingDataServer.EndpointTicket> endpoints2 = server.getFlightInfo(command, identity);

            assertThat(endpoints1).hasSameSizeAs(endpoints2);

            for (int i = 0; i < endpoints1.size(); i++) {
                assertThat(endpoints1.get(i).ticket()).isEqualTo(endpoints2.get(i).ticket());
                assertThat(endpoints1.get(i).port()).isEqualTo(endpoints2.get(i).port());
            }
        } finally {
            server.close();
        }
    }

    /**
     * P7: The ticket serialized in the endpoint contains the exact version
     * reference from the original request. No version drift occurs during
     * request processing.
     */
    @Property(tries = 50)
    void ticketPreservesVersionReference(
            @ForAll("tableNames") String table,
            @ForAll("versionTypes") String versionType,
            @ForAll("versionValues") String versionValue) {

        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9090, 10, "jwt", 65536);
        TrainingDataServer server = new TrainingDataServer(
                config, new PermitAllAccessControl(), new NoOpMetricsProvider());
        server.start();

        try {
            String version = versionType + ":" + versionValue;
            byte[] command = buildCommand(table, version, null, null);

            List<TrainingDataServer.EndpointTicket> endpoints =
                    server.getFlightInfo(command, testIdentity());

            assertThat(endpoints).isNotEmpty();

            String ticket = endpoints.get(0).ticket();
            assertThat(ticket).contains(version);
        } finally {
            server.close();
        }
    }

    /**
     * P7: Different version references produce different tickets.
     * Version pinning distinguishes between distinct versions.
     */
    @Property(tries = 50)
    void differentVersionsProduceDifferentTickets(
            @ForAll("tableNames") String table) {

        TrainingDataServerConfig config = new TrainingDataServerConfig(
                true, 9090, 10, "jwt", 65536);
        TrainingDataServer server = new TrainingDataServer(
                config, new PermitAllAccessControl(), new NoOpMetricsProvider());
        server.start();

        try {
            byte[] commandV1 = buildCommand(table, "tag:training_v1", null, null);
            byte[] commandV2 = buildCommand(table, "tag:training_v2", null, null);

            List<TrainingDataServer.EndpointTicket> endpointsV1 =
                    server.getFlightInfo(commandV1, testIdentity());
            List<TrainingDataServer.EndpointTicket> endpointsV2 =
                    server.getFlightInfo(commandV2, testIdentity());

            assertThat(endpointsV1.get(0).ticket())
                    .isNotEqualTo(endpointsV2.get(0).ticket());
        } finally {
            server.close();
        }
    }

    /**
     * P7: Version reference construction is symmetric — parsing a constructed
     * version string yields the original type and value components.
     */
    @Property(tries = 100)
    void versionReferenceRoundTrips(
            @ForAll("versionTypes") String type,
            @ForAll("versionValues") String value) {

        String versionString = type + ":" + value;
        TrainingDataRequest request = new TrainingDataRequest(
                "catalog.schema.table", versionString, null, null, null);

        VersionReference ref = request.parsedVersion();

        assertThat(ref.type() + ":" + ref.value()).isEqualTo(versionString);
    }

    // ── Helpers ──

    private byte[] buildCommand(String table, String version, String split, List<String> columns) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        map.put("table", table);
        if (version != null) {
            map.put("version", version);
        }
        if (split != null) {
            map.put("split", split);
        }
        if (columns != null) {
            map.put("columns", columns);
        }
        try {
            return OBJECT_MAPPER.writeValueAsBytes(map);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to build command JSON", e);
        }
    }

    private static UserIdentity testIdentity() {
        return new UserIdentity("ml-engineer", Set.of("ml-team"), Map.of(), Instant.now());
    }

    // ── Test doubles ──

    /** Permits all access checks for functional testing of version pinning logic. */
    private static final class PermitAllAccessControl implements AccessControl {
        @Override
        public void checkCanSelectFromTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanInsertIntoTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanDeleteFromTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanCreateTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanDropTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanAlterTable(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanRenameTable(UserIdentity user, String catalog, String schema, String oldTable,
                                        String newCatalog, String newSchema, String newTable) {}
        @Override
        public void checkCanShowColumns(UserIdentity user, String catalog, String schema, String table) {}
        @Override
        public void checkCanSelectFromColumns(UserIdentity user, String catalog, String schema,
                                              String table, Set<String> columns) {}
        @Override
        public void checkCanAccessCatalog(UserIdentity user, String catalog) {}
        @Override
        public void checkCanAccessSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanCreateSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanDropSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanAlterSchema(UserIdentity user, String catalog, String schema) {}
        @Override
        public void checkCanShowSchemas(UserIdentity user, String catalog) {}
        @Override
        public void checkCanShowTables(UserIdentity user, String catalog, String schema) {}
        @Override
        public Optional<String> getRowFilter(UserIdentity user, String catalog, String schema, String table) {
            return Optional.empty();
        }
        @Override
        public Optional<String> getColumnMask(UserIdentity user, String catalog, String schema,
                                              String table, String column) {
            return Optional.empty();
        }
    }
}
