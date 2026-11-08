package org.pangrova.persistence.workflow.postgresql.projection;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.catalog.api.error.MetastoreException;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionMutation;
import org.pangrova.persistence.workflow.api.projection.MetadataProjectionReceipt;
import java.time.Instant;
import java.util.*;
import static org.assertj.core.api.Assertions.*;

class ProjectionReceiptCodecTest {
    private final CatalogDeploymentIdentity scope = new CatalogDeploymentIdentity("cat", "incarnation", 1);

    @Test void allVariantsRoundTripIncludingNanosAndEscapedIdentifiers() {
        var instant = Instant.parse("2026-01-01T00:00:00.123456789Z");
        var table = new TableMetadata("cat", "s\"x", "t\n", List.of(), Map.of(), List.of(), Map.of("json", "{x}"), instant, instant);
        var ref = new QualifiedTableRef("cat", table.schema(), table.table());
        var mutations = List.of(new MetadataProjectionMutation.UpsertTable(UUID.randomUUID(), scope, table),
                new MetadataProjectionMutation.DeleteTable(UUID.randomUUID(), scope, ref),
                new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), scope, List.of("a", "z")));
        for (var mutation : mutations) {
            var receipt = new MetadataProjectionReceipt(mutation, true,
                    mutation instanceof MetadataProjectionMutation.ObserveSchemas ? Optional.empty() : Optional.of(table), Set.of("b", "a"));
            assertThat(ProjectionReceiptCodec.decode(mutation.operationId(), scope,
                    ProjectionReceiptCodec.request(mutation), ProjectionReceiptCodec.result(receipt))).isEqualTo(receipt);
        }
    }

    @ParameterizedTest @ValueSource(strings={"0", "2", "4294967297", "1.0", "\"1\"", "null"})
    void unknownOrCoercedVersionsAreNeverAccepted(String version) {
        var mutation = new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), scope, List.of());
        var receipt = new MetadataProjectionReceipt(mutation, false, Optional.empty(), Set.of());
        String request = ProjectionReceiptCodec.request(mutation);
        String result = ProjectionReceiptCodec.result(receipt);
        assertThatThrownBy(() -> ProjectionReceiptCodec.decode(mutation.operationId(), scope,
                request.replace("\"version\":1", "\"version\":"+version), result)).isInstanceOf(MetastoreException.class);
        assertThatThrownBy(() -> ProjectionReceiptCodec.decode(mutation.operationId(), scope,
                request, result.replace("\"version\":1", "\"version\":"+version))).isInstanceOf(MetastoreException.class);
    }

    @ParameterizedTest @ValueSource(strings={"{}", "[]", "null", "{", "{\"version\":1,\"changed\":\"false\",\"tenants\":[]}",
            "{\"version\":1,\"changed\":false,\"tenants\":[null]}", "{\"version\":1,\"changed\":false,\"tenants\":{}}"})
    void malformedResultsAreRejectedInsteadOfBecomingSuccessfulDefaults(String result) {
        var mutation = new MetadataProjectionMutation.ObserveSchemas(UUID.randomUUID(), scope, List.of());
        assertThatThrownBy(() -> ProjectionReceiptCodec.decode(mutation.operationId(), scope,
                ProjectionReceiptCodec.request(mutation), result)).isInstanceOf(MetastoreException.class);
    }
}
