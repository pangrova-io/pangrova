package org.pangrova.indexing.builtin.test.engine;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.enums.IndexEvalResult;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.LikePattern;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.bitmap.BitmapIndexData;
import org.pangrova.indexing.api.data.text.NGramBloomIndexData;
import org.pangrova.indexing.builtin.bitmap.provider.BitmapStrategyProvider;
import org.pangrova.indexing.builtin.bitmap.serializer.BitmapSerializer;
import org.pangrova.indexing.builtin.bloom.provider.BloomFilterStrategyProvider;
import org.pangrova.indexing.builtin.bloom.serializer.BloomFilterSerializer;
import org.pangrova.indexing.builtin.ngram.provider.NGramBloomStrategyProvider;
import org.pangrova.indexing.builtin.ngram.serializer.NGramBloomSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IndexSnapshotEvaluationTest {

    @Test
    void bloomSnapshotMutationCannotDivergeCachedAndUncachedEvaluation() {
        var provider = new BloomFilterStrategyProvider();
        var codec = new BloomFilterSerializer();
        var data = (BloomFilterIndexData) provider.build(
                List.<List<Comparable<?>>>of(List.of("present")).iterator(), Map.of());
        byte[] before = codec.serialize(data);
        var prepared = provider.prepareForEvaluation(data);
        Arrays.fill(data.filterData(), (byte) 0);
        data.filterDataView().position(5);
        var predicate = new EqualTo("key", "present");
        assertThat(provider.evaluate(data, predicate)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(prepared, predicate)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(codec.deserialize(before), predicate)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(codec.serialize(data)).containsExactly(before);
    }

    @Test
    void ngramSnapshotMutationCannotIntroduceFalseNegatives() {
        var provider = new NGramBloomStrategyProvider();
        var codec = new NGramBloomSerializer();
        var data = (NGramBloomIndexData) provider.build(
                List.<List<Comparable<?>>>of(List.of("haystack needle haystack")).iterator(), Map.of());
        byte[] before = codec.serialize(data);
        Arrays.fill(data.filterData(), (byte) 0);
        var predicate = new LikePattern("key", "%needle%");
        assertThat(provider.evaluate(data, predicate)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(codec.deserialize(before), predicate)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(codec.serialize(data)).containsExactly(before);
    }

    @Test
    void bitmapMembershipAndPartialCoverageSurviveSnapshotMutation() {
        var provider = new BitmapStrategyProvider();
        var codec = new BitmapSerializer();
        var data = (BitmapIndexData) provider.build(
                List.<List<Comparable<?>>>of(List.of("present"), List.of("unrecorded")).iterator(),
                Map.of("bitmap.max_cardinality", "1"));
        byte[] before = codec.serialize(data);
        Arrays.fill(data.bitmaps().get("present"), (byte) 0);
        assertThat(provider.evaluate(data, new EqualTo("key", "present"))).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(data, new EqualTo("key", "unrecorded"))).isEqualTo(IndexEvalResult.UNKNOWN);
        assertThat(codec.serialize(data)).containsExactly(before);
        var restored = codec.deserialize(before);
        assertThat(provider.evaluate(restored, new EqualTo("key", "present"))).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.evaluate(restored, new EqualTo("key", "unrecorded"))).isEqualTo(IndexEvalResult.UNKNOWN);
    }
}
