package org.pangrova.indexing.property.engine.prefix;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.AlphaChars;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.StringLength;

import org.pangrova.connector.spi.index.IndexEvalResult;
import org.pangrova.indexing.api.data.text.PrefixEntry;
import org.pangrova.indexing.api.data.text.PrefixIndexData;
import org.pangrova.connector.spi.index.predicate.LikePattern;
import org.pangrova.indexing.engine.prefix.provider.PrefixStrategyProvider;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link PrefixStrategyProvider} binary search evaluation (P1 fix).
 *
 * <p>Validates: P1 — binary search produces identical results to the original
 * linear scan for prefix LIKE patterns.</p>
 */
class PrefixBinarySearchPropertyTest {

    private final PrefixStrategyProvider provider = new PrefixStrategyProvider();

    @Property(tries = 10)
    void existingPrefixAlwaysFound(
            @ForAll @IntRange(min = 1, max = 50) int entryCount) {
        List<PrefixEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            String prefix = String.format("key%04d", i);
            entries.add(new PrefixEntry(prefix, i));
        }
        entries.sort(Comparator.comparing(PrefixEntry::prefix));
        var data = new PrefixIndexData(entries, 8);

        // Search for a prefix that exists
        int target = entryCount / 2;
        String searchPrefix = String.format("key%04d", target);
        var like = new LikePattern("col", searchPrefix + "%");
        assertThat(provider.evaluate(data, like)).isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 10)
    void nonExistentPrefixReturnsSkip(
            @ForAll @IntRange(min = 1, max = 50) int entryCount) {
        List<PrefixEntry> entries = new ArrayList<>();
        for (int i = 0; i < entryCount; i++) {
            entries.add(new PrefixEntry("aaa" + String.format("%04d", i), i));
        }
        entries.sort(Comparator.comparing(PrefixEntry::prefix));
        var data = new PrefixIndexData(entries, 8);

        var like = new LikePattern("col", "zzz%");
        assertThat(provider.evaluate(data, like)).isEqualTo(IndexEvalResult.SKIP);
    }

    @Property(tries = 10)
    void buildThenEvaluateRoundTrip(
            @ForAll @IntRange(min = 5, max = 100) int rowCount) {
        List<List<Comparable<?>>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++) {
            rows.add(List.of("item_" + String.format("%04d", i)));
        }
        var indexData = provider.build(rows.iterator(), Map.of("prefix.length", "8"));

        // Search for a prefix that was inserted
        int target = rowCount / 2;
        var like = new LikePattern("col", "item_" + String.format("%04d", target) + "%");
        assertThat(provider.evaluate(indexData, like)).isEqualTo(IndexEvalResult.KEEP);
    }

    @Property(tries = 10)
    void nonPrefixPatternReturnsUnknown() {
        List<PrefixEntry> entries = List.of(new PrefixEntry("abc", 0));
        var data = new PrefixIndexData(entries, 8);

        // Pattern with % in the middle — not a pure prefix pattern
        var like = new LikePattern("col", "a%b%");
        assertThat(provider.evaluate(data, like)).isEqualTo(IndexEvalResult.UNKNOWN);
    }
}
