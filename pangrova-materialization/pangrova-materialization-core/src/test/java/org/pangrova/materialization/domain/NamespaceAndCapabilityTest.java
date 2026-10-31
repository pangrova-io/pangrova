package org.pangrova.materialization.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.pangrova.materialization.MaterializationTestFixtures.ATTEMPT_ID;
import static org.pangrova.materialization.MaterializationTestFixtures.DEFINER;
import static org.pangrova.materialization.MaterializationTestFixtures.GENERATION_ID;
import static org.pangrova.materialization.MaterializationTestFixtures.INCARNATION;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.candidate;
import static org.pangrova.materialization.MaterializationTestFixtures.objectId;
import static org.pangrova.materialization.MaterializationTestFixtures.source;
import static org.pangrova.materialization.MaterializationTestFixtures.vector;

import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;

class NamespaceAndCapabilityTest {

    @Test
    void internalSchemaIsReservedExactly() {
        assertThat(InternalNamespace.isReservedSchema("__pangrova_materializations")).isTrue();
        assertThat(InternalNamespace.isReservedSchema("__PANGROVA_MATERIALIZATIONS")).isFalse();
        assertThat(InternalNamespace.isReservedSchema("public")).isFalse();
        assertThat(InternalNamespace.isReservedSchema(null)).isFalse();
    }

    @Test
    void everyInternalTablePrefixIsReservedCaseInsensitively() {
        assertThat(InternalNamespace.isReservedTable("__pg_mat_0123456789abcdef0123456789abcdef")).isTrue();
        assertThat(InternalNamespace.isReservedTable("__PG_MAT_0123456789abcdef0123456789abcdef")).isTrue();
        assertThat(InternalNamespace.isReservedTable("__pg_dt_legacy_table")).isTrue();
        assertThat(InternalNamespace.isReservedTable("orders")).isFalse();
        assertThat(InternalNamespace.isReservedTable(null)).isFalse();
    }

    @Test
    void candidateNamesMatchTheFixedShapeOnly() {
        assertThat(InternalNamespace.isCandidateTable("__pg_mat_0123456789abcdef0123456789abcdef")).isTrue();
        assertThat(InternalNamespace.isCandidateTable("__pg_mat_0123456789ABCDEF0123456789ABCDEF")).isFalse();
        assertThat(InternalNamespace.isCandidateTable("__pg_mat_short")).isFalse();
        assertThat(InternalNamespace.isCandidateTable("__pg_dt_0123456789abcdef0123456789abcdef")).isFalse();
        assertThat(InternalNamespace.candidateTableName("0123456789abcdef0123456789abcdef"))
                .isEqualTo("__pg_mat_0123456789abcdef0123456789abcdef");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> InternalNamespace.candidateTableName("not-hex"))
                .withMessageContaining("32 lowercase hex");
    }

    @Test
    void physicalTableMustLiveInTheInternalNamespace() {
        assertThat(candidate(0).qualifiedName())
                .startsWith("lake.__pangrova_materializations.__pg_mat_");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PhysicalTableRef(
                        "lake", "public", "__pg_mat_0123456789abcdef0123456789abcdef"))
                .withMessageContaining("internal schema");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PhysicalTableRef("lake", InternalNamespace.SCHEMA_NAME, "user_table"))
                .withMessageContaining("__pg_mat_");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new PhysicalTableRef(" ", InternalNamespace.SCHEMA_NAME,
                        "__pg_mat_0123456789abcdef0123456789abcdef"))
                .withMessageContaining("catalog");
    }

    @Test
    void physicalTableIdentityComparesAllParts() {
        PhysicalTableRef first = candidate(1);
        PhysicalTableRef same = new PhysicalTableRef(first.catalog(), first.schema(), first.table());
        PhysicalTableRef otherCatalog = new PhysicalTableRef(
                "s3", first.schema(), first.table());

        assertThat(first.sameTable(same)).isTrue();
        assertThat(first.sameTable(otherCatalog)).isFalse();
        assertThat(first.sameTable(null)).isFalse();
    }

    @Test
    void writeCapabilityIsBoundToTheExactCandidateAndAttempt() {
        MaterializationWriteCapability capability = capability(candidate(0));

        capability.requireCandidate(candidate(0));
        assertThat(capability.isExpiredAt(NOW)).isFalse();
        capability.requireValidAt(NOW);

        MaterializationException foreignCandidate = catchThrowableOfType(
                () -> capability.requireCandidate(candidate(1)), MaterializationException.class);
        assertThat(foreignCandidate.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_WRITE_CAPABILITY_INVALID);
    }

    @Test
    void writeCapabilityExpiresAtTheExclusiveBoundary() {
        MaterializationWriteCapability capability = capability(candidate(0));

        assertThat(capability.isExpiredAt(NOW.plusSeconds(60))).isTrue();
        MaterializationException exception = catchThrowableOfType(
                () -> capability.requireValidAt(NOW.plusSeconds(60)), MaterializationException.class);
        assertThat(exception.getErrorCode())
                .isEqualTo(PangrovaErrorCode.MATERIALIZATION_WRITE_CAPABILITY_INVALID);
    }

    @Test
    void writeCapabilityRejectsMalformedHashesAndWindows() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MaterializationWriteCapability(
                        objectId("orders_dt"), INCARNATION, 1, ATTEMPT_ID, candidate(0), GENERATION_ID,
                        "short", DEFINER, 7, NOW, NOW.plusSeconds(60)))
                .withMessageContaining("SHA-256");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MaterializationWriteCapability(
                        objectId("orders_dt"), INCARNATION, 1, ATTEMPT_ID, candidate(0), GENERATION_ID,
                        vector(source("orders")).canonicalHash(), DEFINER, 7, NOW, NOW))
                .withMessageContaining("expiresAt must be after issuedAt");
    }

    private static MaterializationWriteCapability capability(PhysicalTableRef candidate) {
        return new MaterializationWriteCapability(
                objectId("orders_dt"),
                INCARNATION,
                1,
                ATTEMPT_ID,
                candidate,
                GENERATION_ID,
                vector(source("orders")).canonicalHash(),
                DEFINER,
                7,
                NOW,
                NOW.plusSeconds(60));
    }
}
