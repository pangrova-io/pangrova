package org.pangrova.materialization.domain;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.pangrova.materialization.MaterializationTestFixtures.NOW;
import static org.pangrova.materialization.MaterializationTestFixtures.source;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.materialization.SourceRef;
import org.pangrova.spi.connector.materialization.SourceSnapshotRef;

class SourceVectorTest {

    @Test
    void canonicalHashIsIndependentOfInsertionOrder() {
        SourceRef orders = source("orders");
        SourceRef customers = source("customers");
        SourceSnapshotRef ordersSnapshot = snapshot(orders, "snap-orders");
        SourceSnapshotRef customersSnapshot = snapshot(customers, "snap-customers");

        Map<SourceRef, SourceSnapshotRef> forward = new LinkedHashMap<>();
        forward.put(orders, ordersSnapshot);
        forward.put(customers, customersSnapshot);

        Map<SourceRef, SourceSnapshotRef> reverse = new LinkedHashMap<>();
        reverse.put(customers, customersSnapshot);
        reverse.put(orders, ordersSnapshot);

        SourceVector first = new SourceVector(forward, 3);
        SourceVector second = new SourceVector(reverse, 3);

        assertThat(first.canonicalHash()).isEqualTo(second.canonicalHash());
        assertThat(first.sameSnapshots(second)).isTrue();
        assertThat(first.snapshotOf(orders)).contains(ordersSnapshot);
        assertThat(first.snapshotOf(source("missing"))).isEmpty();
    }

    @Test
    void everyIdentityDimensionChangesTheCanonicalHash() {
        SourceRef orders = source("orders");
        SourceVector base = new SourceVector(Map.of(orders, snapshot(orders, "snap-1")), 1);

        SourceVector otherVersion = new SourceVector(Map.of(orders, snapshot(orders, "snap-1")), 2);
        SourceVector otherToken = new SourceVector(Map.of(orders, snapshot(orders, "snap-2")), 1);
        SourceVector otherSchema = new SourceVector(
                Map.of(orders, new SourceSnapshotRef(
                        orders.connectorId(), "snap-1", "other-schema", "policy-snap-1", NOW, true)),
                1);
        SourceVector otherPolicy = new SourceVector(
                Map.of(orders, new SourceSnapshotRef(
                        orders.connectorId(), "snap-1", "schema-snap-1", "other-policy", NOW, true)),
                1);
        SourceVector otherSource = new SourceVector(
                Map.of(source("customers"), snapshot(source("customers"), "snap-1")), 1);

        assertThat(base.canonicalHash())
                .isNotEqualTo(otherVersion.canonicalHash())
                .isNotEqualTo(otherToken.canonicalHash())
                .isNotEqualTo(otherSchema.canonicalHash())
                .isNotEqualTo(otherPolicy.canonicalHash())
                .isNotEqualTo(otherSource.canonicalHash());
        assertThat(base.sameSnapshots(otherToken)).isFalse();
        assertThat(base.sameSnapshots(null)).isFalse();
    }

    @Test
    void rejectsUnpinnableSources() {
        SourceRef orders = source("orders");
        SourceSnapshotRef unpinnable = new SourceSnapshotRef(
                orders.connectorId(), "snap-1", "schema-snap-1", "policy-snap-1", NOW, false);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new SourceVector(Map.of(orders, unpinnable), 1))
                .withMessageContaining("not pinnable");
    }

    @Test
    void rejectsInconsistentOrIncompleteEntries() {
        SourceRef orders = source("orders");
        SourceSnapshotRef foreignConnector = new SourceSnapshotRef(
                "paimon", "snap-1", "schema", "policy", NOW, true);
        Map<SourceRef, SourceSnapshotRef> incomplete = new LinkedHashMap<>();
        incomplete.put(orders, null);

        assertThatIllegalArgumentException()
                .isThrownBy(() -> new SourceVector(Map.of(orders, foreignConnector), 1))
                .withMessageContaining("does not match");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new SourceVector(incomplete, 1))
                .withMessageContaining("complete");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new SourceVector(Map.of(orders, snapshot(orders, "snap-1")), -1))
                .withMessageContaining("negative");
    }

    @Test
    void sourceVectorShapeIsASetOfPinnedSources() {
        SourceRef orders = source("orders");
        SourceRef customers = source("customers");
        SourceVector vector = new SourceVector(Map.of(
                orders, snapshot(orders, "snap-orders"),
                customers, snapshot(customers, "snap-customers")), 1);

        assertThat(vector.snapshots()).hasSize(2);
        assertThatThrownBy(() -> vector.snapshots().put(orders, snapshot(orders, "x")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Property(tries = 50)
    void canonicalHashIgnoresPermutation(@ForAll("tokens") List<String> tokens) {
        List<SourceRef> sources = IntStream.range(0, tokens.size())
                .mapToObj(index -> source("table_" + index))
                .collect(Collectors.toList());
        Map<SourceRef, SourceSnapshotRef> snapshots = new LinkedHashMap<>();
        for (int i = 0; i < sources.size(); i++) {
            snapshots.put(sources.get(i), snapshot(sources.get(i), tokens.get(i)));
        }

        List<Integer> order = IntStream.range(0, sources.size()).boxed().collect(Collectors.toList());
        Collections.shuffle(order, new Random(tokens.hashCode()));
        Map<SourceRef, SourceSnapshotRef> permuted = new LinkedHashMap<>();
        for (int index : order) {
            permuted.put(sources.get(index), snapshots.get(sources.get(index)));
        }

        assertThat(new SourceVector(permuted, 9).canonicalHash())
                .isEqualTo(new SourceVector(snapshots, 9).canonicalHash());
    }

    @Provide
    Arbitrary<List<String>> tokens() {
        return Arbitraries.strings()
                .alpha()
                .numeric()
                .ofMinLength(1)
                .ofMaxLength(8)
                .list()
                .ofSize(5);
    }

    private static SourceSnapshotRef snapshot(SourceRef source, String token) {
        return new SourceSnapshotRef(
                source.connectorId(), token, "schema-" + token, "policy-" + token, NOW, true);
    }
}
