package org.pangrova.indexing.eval;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.connector.api.index.IndexEvalResult;
import org.pangrova.connector.api.index.predicate.EqualTo;
import org.pangrova.connector.api.index.predicate.IndexPredicate;
import org.pangrova.indexing.api.data.BloomFilterIndexData;
import org.pangrova.indexing.api.data.IndexData;
import org.pangrova.indexing.api.descriptor.IndexType;
import org.pangrova.indexing.api.evaluation.IndexEvalDescriptor;
import org.pangrova.indexing.builtin.bloom.data.CachedBloomFilterIndexData;
import org.pangrova.indexing.builtin.bloom.provider.BloomFilterStrategyProvider;
import org.pangrova.indexing.builtin.codec.BuiltinIndexSerializers;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.fs.LocalFileSystemAccessor;
import org.pangrova.indexing.registry.IndexEngineRegistry;
import org.pangrova.indexing.spi.strategy.IndexStrategyProvider;
import org.pangrova.indexing.storage.IndexCache;
import org.pangrova.indexing.storage.IndexStorageManager;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexEvaluationPreparationTest {
    @TempDir Path directory;
    private final BloomFilterStrategyProvider bloom = new BloomFilterStrategyProvider();
    private final IndexSerializer serializer = new IndexSerializer(BuiltinIndexSerializers.createRegistry());
    private final LocalFileSystemAccessor filesystem = new LocalFileSystemAccessor();
    private final IndexCache cache = new IndexCache(1_048_576);

    @Test
    void firstEvaluationAndCacheHitSharePreparedDataAndGenerationReloads() throws Exception {
        Path file = write(validData());
        var provider = new TrackingProvider(bloom::prepareForEvaluation);
        var service = service(provider, cache);
        assertThat(evaluate(service, file, 1)).isEqualTo(IndexEvalResult.KEEP);
        IndexData first = provider.lastEvaluated;
        assertThat(first).isInstanceOf(CachedBloomFilterIndexData.class);
        assertThat(cache.get("index", "data", 1)).isSameAs(first);
        assertThat(evaluate(service, file, 1)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.lastEvaluated).isSameAs(first);
        assertThat(provider.preparations).isEqualTo(1);
        assertThat(evaluate(service, file, 2)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(provider.preparations).isEqualTo(2);
        assertThat(provider.lastEvaluated).isNotSameAs(first);
        assertThat(cache.get("index", "data", 1)).isNull();
    }

    @Test
    void malformedBloomIsUnknownAndNeverCached() throws Exception {
        Path file = write(new BloomFilterIndexData(new byte[]{1, 2, 3}, 3, 24));
        var provider = new TrackingProvider(bloom::prepareForEvaluation);
        assertThat(evaluate(service(provider, cache), file, 1)).isEqualTo(IndexEvalResult.UNKNOWN);
        assertThat(cache.size()).isZero();
        assertThat(provider.lastEvaluated).isNull();
    }

    @Test
    void invalidPreparationCannotPopulateCacheOrReachEvaluator() throws Exception {
        Path file = write(validData());
        for (Function<IndexData, IndexData> preparation : List.<Function<IndexData, IndexData>>of(
                data -> null, data -> new WrongTypeData())) {
            var provider = new TrackingProvider(preparation);
            assertThat(evaluate(service(provider, cache), file, 1)).isEqualTo(IndexEvalResult.UNKNOWN);
            assertThat(cache.size()).isZero();
            assertThat(provider.lastEvaluated).isNull();
        }
    }

    @Test
    void descriptorTypeMismatchCannotInvokeProviderOrPopulateCache() throws Exception {
        Path file = write(validData());
        var provider = new TrackingProvider(bloom::prepareForEvaluation);
        var service = service(provider, cache);
        assertThat(service.evaluateFile(new IndexEvalDescriptor("index", IndexType.HASH, 1),
                "data", file.toString(), new EqualTo("c", "present"), filesystem))
                .isEqualTo(IndexEvalResult.UNKNOWN);
        assertThat(provider.preparations).isZero();
        assertThat(cache.size()).isZero();
    }

    @Test
    void cancellationDuringPreparationIsPropagatedAndDoesNotCache() throws Exception {
        Path file = write(validData());
        var provider = new TrackingProvider(data -> { throw new CancellationException("cancelled"); });
        assertThatThrownBy(() -> evaluate(service(provider, cache), file, 1))
                .isInstanceOf(CancellationException.class);
        assertThat(cache.size()).isZero();
        assertThat(provider.lastEvaluated).isNull();
    }

    @Test
    void parsedBufferCountsTowardEvictionBudget() throws Exception {
        BloomFilterIndexData raw = validData();
        var prepared = (CachedBloomFilterIndexData) bloom.prepareForEvaluation(raw);
        assertThat(prepared.estimatedSizeInBytes())
                .isGreaterThanOrEqualTo(raw.estimatedSizeInBytes() + prepared.filter().sizeInBytes());
        var smallCache = new IndexCache(raw.estimatedSizeInBytes());
        var provider = new TrackingProvider(bloom::prepareForEvaluation);
        Path file = write(raw);
        assertThat(evaluate(service(provider, smallCache), file, 1)).isEqualTo(IndexEvalResult.KEEP);
        assertThat(smallCache.currentSizeBytes()).isLessThanOrEqualTo(raw.estimatedSizeInBytes());
        assertThat(smallCache.get("index", "data", 1)).isNull();
    }

    private BloomFilterIndexData validData() {
        return (BloomFilterIndexData) bloom.build(
                List.<List<Comparable<?>>>of(List.of("present")).iterator(), Map.of());
    }

    private Path write(IndexData data) throws Exception {
        Path file = directory.resolve("index.bin");
        Files.write(file, serializer.serialize(data));
        return file;
    }

    private IndexEvaluationService service(IndexStrategyProvider provider, IndexCache targetCache) {
        return new IndexEvaluationService(new IndexEngineRegistry(List.of(provider)), targetCache,
                new IndexStorageManager(), serializer);
    }

    private IndexEvalResult evaluate(IndexEvaluationService service, Path file, int generation) {
        return service.evaluateFile(new IndexEvalDescriptor("index", IndexType.BLOOM_FILTER, generation),
                "data", file.toString(), new EqualTo("c", "present"), filesystem);
    }

    private final class TrackingProvider implements IndexStrategyProvider {
        private final Function<IndexData, IndexData> preparation;
        private int preparations;
        private IndexData lastEvaluated;
        TrackingProvider(Function<IndexData, IndexData> preparation) { this.preparation = preparation; }
        public IndexType supportedType() { return IndexType.BLOOM_FILTER; }
        public IndexData build(Iterator<List<Comparable<?>>> values, Map<String, String> properties) {
            return bloom.build(values, properties);
        }
        public IndexData prepareForEvaluation(IndexData data) {
            preparations++;
            return preparation.apply(data);
        }
        public IndexEvalResult evaluate(IndexData data, IndexPredicate predicate) {
            lastEvaluated = data;
            return bloom.evaluate(data, predicate);
        }
    }

    private record WrongTypeData() implements IndexData {
        public IndexType indexType() { return IndexType.HASH; }
        public long estimatedSizeInBytes() { return 32; }
    }
}
