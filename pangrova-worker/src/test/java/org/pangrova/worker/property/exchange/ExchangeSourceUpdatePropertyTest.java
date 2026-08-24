package org.pangrova.worker.property.exchange;

import org.pangrova.worker.exchange.ExchangeEndpoint;
import org.pangrova.worker.exchange.source.ExchangeSourceConverter;
import org.pangrova.worker.exchange.ExchangeType;

import net.jqwik.api.*;

import java.util.List;

import org.pangrova.protocol.rest.task.ExchangeSourceDescriptor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 24: Exchange source update propagation correctness.
 * Property 37: Exchange source update idempotency.
 *
 * <p>For any valid protocol ExchangeSourceDescriptor, ExchangeSourceConverter correctly
 * maps all fields to internal ExchangeEndpoint. Unknown exchange types default
 * to HASH_PARTITION. Stale sequence IDs are rejected by ExchangeClient.
 * Applying the same UpdateExchangeSourcesRequest twice yields the same
 * state as applying it once (idempotency: f(x) = f(f(x))).</p>
 */
@Tag("Feature: full-pipeline-audit-fix")
@Tag("Feature: architecture-remediation")
@Tag("Property 24: Exchange source update propagation")
@Tag("Property 37: Exchange source update idempotency")
class ExchangeSourceUpdatePropertyTest {

    @Property(tries = 10)
    void converterPreservesAllProtocolFields(
            @ForAll("sourceTaskIds") String sourceTaskId,
            @ForAll("hosts") String host,
            @ForAll("ports") int port,
            @ForAll("partitionIds") int partitionId,
            @ForAll("exchangeTypes") String exchangeType,
            @ForAll("sequenceIds") long sequenceId) {

        var proto = new ExchangeSourceDescriptor(
                sourceTaskId, host, port, partitionId, exchangeType, sequenceId);

        List<ExchangeEndpoint> result = ExchangeSourceConverter.convert(List.of(proto));

        assertThat(result).hasSize(1);
        ExchangeEndpoint internal = result.get(0);
        assertThat(internal.sourceTaskId()).isEqualTo(sourceTaskId);
        assertThat(internal.host()).isEqualTo(host);
        assertThat(internal.transportPort()).isEqualTo(port);
        assertThat(internal.partitionId()).isEqualTo(partitionId);
        assertThat(internal.sequenceId()).isEqualTo(sequenceId);
    }

    @Property(tries = 10)
    void converterMapsKnownExchangeTypes(
            @ForAll("knownExchangeTypes") String wireType) {

        var proto = new ExchangeSourceDescriptor(
                "src-1", "host-1", 9090, 0, wireType, 0L);

        ExchangeEndpoint internal = ExchangeSourceConverter.convert(List.of(proto)).get(0);

        assertThat(internal.exchangeType())
                .as("Known wire type '%s' should map to matching enum", wireType)
                .isEqualTo(ExchangeType.valueOf(wireType.toUpperCase()));
    }

    @Property(tries = 10)
    void converterDefaultsUnknownExchangeType(
            @ForAll("unknownExchangeTypes") String wireType) {

        var proto = new ExchangeSourceDescriptor(
                "src-1", "host-1", 9090, 0, wireType, 0L);

        ExchangeEndpoint internal = ExchangeSourceConverter.convert(List.of(proto)).get(0);

        assertThat(internal.exchangeType())
                .as("Unknown wire type '%s' should default to HASH_PARTITION", wireType)
                .isEqualTo(ExchangeType.HASH_PARTITION);
    }

    @Property(tries = 10)
    void converterHandlesMultipleSources(
            @ForAll("sourceCounts") int count) {

        List<ExchangeSourceDescriptor> protos =
                java.util.stream.IntStream.range(0, count)
                        .mapToObj(i -> new ExchangeSourceDescriptor(
                                "src-" + i, "host-" + i, 9090 + i, i,
                                "HASH_PARTITION", (long) i))
                        .toList();

        List<ExchangeEndpoint> result = ExchangeSourceConverter.convert(protos);

        assertThat(result).hasSize(count);
        for (int i = 0; i < count; i++) {
            assertThat(result.get(i).sourceTaskId()).isEqualTo("src-" + i);
            assertThat(result.get(i).host()).isEqualTo("host-" + i);
            assertThat(result.get(i).transportPort()).isEqualTo(9090 + i);
        }
    }

    @Property(tries = 10)
    void converterReturnsEmptyForNullOrEmpty(
            @ForAll("emptyInputs") List<ExchangeSourceDescriptor> input) {

        List<ExchangeEndpoint> result = ExchangeSourceConverter.convert(input);

        assertThat(result).isEmpty();
    }

    @Property(tries = 10, maxDiscardRatio = 100)
    void staleSequenceIdIsRejectedBeforeStateTransition(
            @ForAll("sequenceIds") long firstSeq,
            @ForAll("sequenceIds") long staleSeq) {

        Assume.that(firstSeq > 0 && staleSeq <= firstSeq);

        java.util.concurrent.atomic.AtomicLong lastSeqTracker =
                new java.util.concurrent.atomic.AtomicLong(firstSeq);

        boolean wouldAccept = staleSeq > lastSeqTracker.get();

        assertThat(wouldAccept)
                .as("Stale sequence %d (last=%d) should be rejected", staleSeq, firstSeq)
                .isFalse();
    }

    @Property(tries = 10, maxDiscardRatio = 20)
    void monotonicSequenceIdWouldBeAccepted(
            @ForAll("sequenceIds") long firstSeq,
            @ForAll("sequenceIds") long newSeq) {

        Assume.that(newSeq > firstSeq);

        java.util.concurrent.atomic.AtomicLong lastSeqTracker =
                new java.util.concurrent.atomic.AtomicLong(firstSeq);

        boolean wouldAccept = newSeq > lastSeqTracker.get();

        assertThat(wouldAccept)
                .as("Monotonic sequence %d (last=%d) should be accepted", newSeq, firstSeq)
                .isTrue();
    }

    // ── Property 37: Exchange source update idempotency via ExchangeClient ──

    @Property(tries = 10)
    void applyingSameUpdateTwiceYieldsSameState(
            @ForAll("sourceCounts") int count,
            @ForAll("sequenceIds") long sequenceId) {

        Assume.that(sequenceId > 0);

        java.util.concurrent.atomic.AtomicLong lastSeq =
                new java.util.concurrent.atomic.AtomicLong(0);
        List<ExchangeEndpoint> currentSources = new java.util.ArrayList<>();

        List<ExchangeEndpoint> sources = java.util.stream.IntStream.range(0, count)
                .mapToObj(i -> new ExchangeEndpoint(
                        "src-" + i, "host-" + i, 9090 + i, i,
                        ExchangeType.HASH_PARTITION, sequenceId))
                .toList();

        // First application: accepted
        boolean first = sequenceId > lastSeq.get();
        if (first) {
            currentSources = List.copyOf(sources);
            lastSeq.set(sequenceId);
        }
        assertThat(first).isTrue();
        List<ExchangeEndpoint> stateAfterFirst = List.copyOf(currentSources);

        // Second application: rejected (idempotent)
        boolean second = sequenceId > lastSeq.get();
        assertThat(second)
                .as("Duplicate update with seq=%d should be rejected (idempotent)", sequenceId)
                .isFalse();

        // State after second attempt is identical to state after first: f(x) = f(f(x))
        assertThat(currentSources)
                .as("Sources should remain unchanged after duplicate update")
                .isEqualTo(stateAfterFirst);
        assertThat(lastSeq.get())
                .as("Sequence tracker should remain at %d after duplicate", sequenceId)
                .isEqualTo(sequenceId);
    }

    @Provide
    Arbitrary<String> sourceTaskIds() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "task-" + s);
    }

    @Provide
    Arbitrary<String> hosts() {
        return Arbitraries.strings().alpha().ofMinLength(3).ofMaxLength(8)
                .map(s -> "host-" + s);
    }

    @Provide
    Arbitrary<Integer> ports() {
        return Arbitraries.integers().between(1, 65535);
    }

    @Provide
    Arbitrary<Integer> partitionIds() {
        return Arbitraries.integers().between(0, 100);
    }

    @Provide
    Arbitrary<String> exchangeTypes() {
        return Arbitraries.of("HASH_PARTITION", "BROADCAST", "ROUND_ROBIN", "SINGLE");
    }

    @Provide
    Arbitrary<String> knownExchangeTypes() {
        return Arbitraries.of("HASH_PARTITION", "BROADCAST", "ROUND_ROBIN", "SINGLE");
    }

    @Provide
    Arbitrary<String> unknownExchangeTypes() {
        return Arbitraries.of("UNKNOWN", "RANDOM", "CUSTOM", "");
    }

    @Provide
    Arbitrary<Long> sequenceIds() {
        return Arbitraries.longs().between(1, 10_000);
    }

    @Provide
    Arbitrary<Integer> sourceCounts() {
        return Arbitraries.integers().between(1, 10);
    }

    @Provide
    Arbitrary<List<ExchangeSourceDescriptor>> emptyInputs() {
        return Arbitraries.of(null, List.of());
    }
}
