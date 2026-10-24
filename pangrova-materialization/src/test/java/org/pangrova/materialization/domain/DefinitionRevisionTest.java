package org.pangrova.materialization.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.materialization.MaterializationTestFixtures.DEFINER;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.binding;
import static org.pangrova.materialization.MaterializationTestFixtures.definition;
import static org.pangrova.materialization.MaterializationTestFixtures.dependency;
import static org.pangrova.materialization.MaterializationTestFixtures.objectId;
import static org.pangrova.materialization.MaterializationTestFixtures.outputSchema;
import static org.pangrova.materialization.MaterializationTestFixtures.policies;
import static org.pangrova.materialization.MaterializationTestFixtures.source;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.spi.materialization.SourceRef;

class DefinitionRevisionTest {

    private static final String SQL = "SELECT id, name FROM db.orders";

    @Test
    void fingerprintIsIndependentOfSourceAndDependencyIterationOrder() {
        MaterializationId object = objectId("orders_dt");
        MaterializationId upstreamOne = objectId("upstream_one");
        MaterializationId upstreamTwo = objectId("upstream_two");
        SourceRef orders = source("orders");
        SourceRef customers = source("customers");

        Set<SourceRef> sourcesOne = new LinkedHashSet<>();
        sourcesOne.add(orders);
        sourcesOne.add(customers);
        Set<SourceRef> sourcesTwo = new LinkedHashSet<>();
        sourcesTwo.add(customers);
        sourcesTwo.add(orders);

        Set<Dependency> dependenciesOne = new LinkedHashSet<>();
        dependenciesOne.add(dependency(upstreamOne, object, 1));
        dependenciesOne.add(dependency(upstreamTwo, object, 1));
        Set<Dependency> dependenciesTwo = new LinkedHashSet<>();
        dependenciesTwo.add(dependency(upstreamTwo, object, 1));
        dependenciesTwo.add(dependency(upstreamOne, object, 1));

        DefinitionRevision first = definition(object, 1, SQL, sourcesOne, dependenciesOne);
        DefinitionRevision second = definition(object, 1, SQL, sourcesTwo, dependenciesTwo);

        assertThat(first.fingerprint()).isEqualTo(second.fingerprint());
        assertThat(first.sameSemantics(second)).isTrue();
    }

    @Test
    void fingerprintChangesWhenAnySemanticInputChanges() {
        MaterializationId object = objectId("orders_dt");
        Set<SourceRef> sources = Set.of(source("orders"));
        DefinitionRevision base = definition(object, 1, SQL, sources);

        DefinitionRevision otherSql = definition(object, 1, SQL + " WHERE id > 0", sources);
        DefinitionRevision otherRevision = new DefinitionRevision(
                object, 2, SQL, binding(SQL), outputSchema(), sources, Set.of(),
                policies(sources), SecurityMode.DEFINER, DEFINER, Map.of(), NOW);
        DefinitionRevision otherBinding = new DefinitionRevision(
                object, 1, SQL, new Binding(SQL, "other-plan"), outputSchema(), sources, Set.of(),
                policies(sources), SecurityMode.DEFINER, DEFINER, Map.of(), NOW);
        DefinitionRevision otherSchema = new DefinitionRevision(
                object, 1, SQL, binding(SQL),
                new OutputSchema(java.util.List.of(
                        new ColumnDefinition("id", "BIGINT", false, 0),
                        new ColumnDefinition("name", "VARCHAR", false, 1))),
                sources, Set.of(), policies(sources), SecurityMode.DEFINER, DEFINER, Map.of(), NOW);
        DefinitionRevision otherDefiner = new DefinitionRevision(
                object, 1, SQL, binding(SQL), outputSchema(), sources, Set.of(),
                policies(sources), SecurityMode.DEFINER, new Principal("other"), Map.of(), NOW);
        DefinitionRevision otherProperties = new DefinitionRevision(
                object, 1, SQL, binding(SQL), outputSchema(), sources, Set.of(),
                policies(sources), SecurityMode.DEFINER, DEFINER,
                Map.of("timezone", "UTC"), NOW);
        DefinitionRevision otherSources = definition(object, 1, SQL, Set.of(source("customers")));

        assertThat(base.fingerprint())
                .isNotEqualTo(otherSql.fingerprint())
                .isNotEqualTo(otherRevision.fingerprint())
                .isNotEqualTo(otherBinding.fingerprint())
                .isNotEqualTo(otherSchema.fingerprint())
                .isNotEqualTo(otherDefiner.fingerprint())
                .isNotEqualTo(otherProperties.fingerprint())
                .isNotEqualTo(otherSources.fingerprint());
        assertThat(base.sameSemantics(otherSql)).isFalse();
        assertThat(base.sameSemantics(null)).isFalse();
    }

    @Test
    void dependencyMustTargetTheOwningObjectAtTheSameRevision() {
        MaterializationId object = objectId("orders_dt");
        MaterializationId other = objectId("other_dt");
        SourceRef orders = source("orders");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> definition(object, 1, SQL, Set.of(orders),
                        Set.of(dependency(objectId("upstream"), other, 1))))
                .withMessageContaining("downstream must be the owning object");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> definition(object, 1, SQL, Set.of(orders),
                        Set.of(dependency(objectId("upstream"), object, 2))))
                .withMessageContaining("must match the definition revision");
    }

    @Test
    void dependencyRejectsSelfReferenceAndCrossTenantEdges() {
        MaterializationId object = objectId("orders_dt");
        MaterializationId foreign = new MaterializationId(
                new TenantId("tenant-b"), object.name());

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Dependency(object, object, 1, DependencyKind.DIRECT))
                .withMessageContaining("cannot depend on itself");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Dependency(object, foreign, 1, DependencyKind.DIRECT))
                .withMessageContaining("cross-tenant");
    }

    @Test
    void policySnapshotMustCoverExactlyTheDefinitionSources() {
        MaterializationId object = objectId("orders_dt");
        SourceRef orders = source("orders");
        SourceRef customers = source("customers");

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DefinitionRevision(
                        object, 1, SQL, binding(SQL), outputSchema(), Set.of(orders, customers), Set.of(),
                        policies(Set.of(orders)), SecurityMode.DEFINER, DEFINER, Map.of(), NOW))
                .withMessageContaining("exactly one entry per source");
    }

    @Test
    void unsupportedSecurityModeIsRejectedAsInvalidDefinition() {
        MaterializationId object = objectId("orders_dt");
        Set<SourceRef> sources = Set.of(source("orders"));

        MaterializationException exception = org.assertj.core.api.Assertions.catchThrowableOfType(
                () -> new DefinitionRevision(
                        object, 1, SQL, binding(SQL), outputSchema(), sources, Set.of(),
                        policies(sources), SecurityMode.INVOKER, DEFINER, Map.of(), NOW),
                MaterializationException.class);

        assertThat(exception).isNotNull();
        assertThat(exception.getMessage()).contains("unsupported security mode");
        assertThat(exception.getErrorCode()).isEqualTo(PangrovaErrorCode.MATERIALIZATION_DEFINITION_INVALID);
    }

    @Test
    void revisionAndSqlAreMandatory() {
        MaterializationId object = objectId("orders_dt");
        Set<SourceRef> sources = Set.of(source("orders"));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> definition(object, 0, SQL, sources))
                .withMessageContaining("positive");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new DefinitionRevision(
                        object, 1, " ", binding(SQL), outputSchema(), sources, Set.of(),
                        policies(sources), SecurityMode.DEFINER, DEFINER, Map.of(), NOW))
                .withMessageContaining("definition sql");
    }

    @Test
    void policySnapshotFingerprintIsOrderIndependent() {
        SourceRef orders = source("orders");
        SourceRef customers = source("customers");
        Map<SourceRef, String> forward = new LinkedHashMap<>();
        forward.put(orders, "p1");
        forward.put(customers, "p2");
        Map<SourceRef, String> reverse = new LinkedHashMap<>();
        reverse.put(customers, "p2");
        reverse.put(orders, "p1");

        assertThat(new PolicySnapshot(forward).fingerprint())
                .isEqualTo(new PolicySnapshot(reverse).fingerprint());
    }
}
