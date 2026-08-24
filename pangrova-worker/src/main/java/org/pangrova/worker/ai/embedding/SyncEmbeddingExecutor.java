package org.pangrova.worker.ai.embedding;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.pangrova.worker.ai.blob.BlobEmbeddingResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Executes synchronous embedding computation for DML operations.
 *
 * <p>Receives a list of {@link EmbedRequest} directives and invokes the appropriate
 * {@link EmbeddingProvider} to compute vectors before the DML commits. Handles
 * batching (default 64 per request), error handling (abort on failure), and
 * skip-unchanged logic for UPDATE operations.</p>
 *
 * <p>Thread safety: instances are safe for concurrent use from multiple worker threads,
 * provided the underlying {@link EmbeddingProvider} implementations are thread-safe
 * (as required by the SPI contract).</p>
 */
public final class SyncEmbeddingExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(SyncEmbeddingExecutor.class);

    /** Default batch size for bulk embedding requests. */
    static final int DEFAULT_BATCH_SIZE = 64;

    private final EmbeddingProviderLookup providerLookup;
    private final BlobEmbeddingResolver blobEmbeddingResolver;
    private final int batchSize;

    /**
     * Creates an executor with the default batch size (64).
     *
     * @param providerLookup resolves model names to embedding providers
     */
    public SyncEmbeddingExecutor(EmbeddingProviderLookup providerLookup) {
        this(providerLookup, DEFAULT_BATCH_SIZE);
    }

    /**
     * Creates an executor with a custom batch size.
     *
     * @param providerLookup resolves model names to embedding providers
     * @param batchSize      maximum texts per embedding request (must be positive)
     */
    public SyncEmbeddingExecutor(EmbeddingProviderLookup providerLookup, int batchSize) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup must not be null");
        }
        if (batchSize <= 0) {
            throw new IllegalArgumentException("batchSize must be positive, got: " + batchSize);
        }
        this.providerLookup = providerLookup;
        this.blobEmbeddingResolver = new BlobEmbeddingResolver(providerLookup::resolve);
        this.batchSize = batchSize;
    }

    /**
     * Computes embeddings for a single row (INSERT or UPDATE of one row).
     *
     * <p>For each request, concatenates source column values and calls the embedding
     * provider. Returns a map from vector column name to the computed embedding.</p>
     *
     * @param requests     embedding directives for the target table
     * @param sourceValues map of column name to value for the row being written
     * @return map of vector column name to computed embedding; empty if requests is empty
     * @throws PangrovaException with {@link PangrovaErrorCode#AI_EMBEDDING_PROVIDER_UNAVAILABLE}
     *                         if the embedding provider fails
     */
    public Map<String, float[]> embedRow(List<EmbedRequest> requests,
                                         Map<String, String> sourceValues) {
        Objects.requireNonNull(requests, "requests must not be null");
        Objects.requireNonNull(sourceValues, "sourceValues must not be null");

        if (requests.isEmpty()) {
            return Map.of();
        }

        Map<String, float[]> results = new HashMap<>();
        for (EmbedRequest request : requests) {
            if (request.isFileSource()) {
                // FILE source: extract Claim-Check struct fields and embed via blob path
                float[] embedding = embedFileRow(request, sourceValues);
                if (embedding != null) {
                    results.put(request.vectorColumn(), embedding);
                }
            } else {
                // TEXT source: concatenate text columns and embed via text path
                String inputText = concatenateSourceColumns(request.sourceColumns(), sourceValues);
                if (inputText.isEmpty()) {
                    LOG.debug("event=sync_embed_skip_empty column={}", request.vectorColumn());
                    continue;
                }

                EmbeddingProvider provider = resolveProvider(request.modelName());
                float[] embedding = callProvider(provider, inputText, request.vectorColumn());
                results.put(request.vectorColumn(), embedding);
            }
        }

        return Collections.unmodifiableMap(results);
    }

    /**
     * Computes embeddings for a batch of rows (bulk INSERT).
     *
     * <p>Groups texts by request, batches them according to the configured batch size,
     * and calls {@link EmbeddingProvider#embedBatch(List)}. If any batch fails, the entire
     * operation is aborted (atomic semantics for SYNC mode).</p>
     *
     * @param requests   embedding directives for the target table
     * @param rowSources list of per-row source value maps, one per row being inserted
     * @return list of per-row embedding maps (vector column → embedding), in input order
     * @throws PangrovaException with {@link PangrovaErrorCode#AI_EMBEDDING_PROVIDER_UNAVAILABLE}
     *                         if the embedding provider fails
     */
    public List<Map<String, float[]>> embedBatch(List<EmbedRequest> requests,
                                                  List<Map<String, String>> rowSources) {
        Objects.requireNonNull(requests, "requests must not be null");
        Objects.requireNonNull(rowSources, "rowSources must not be null");

        if (requests.isEmpty() || rowSources.isEmpty()) {
            return Collections.nCopies(rowSources.size(), Map.of());
        }

        int rowCount = rowSources.size();
        List<Map<String, float[]>> results = new ArrayList<>(rowCount);
        for (int i = 0; i < rowCount; i++) {
            results.add(new HashMap<>());
        }

        for (EmbedRequest request : requests) {
            if (request.isFileSource()) {
                // FILE source: embed each row's Claim-Check struct via blob path
                embedFileBatch(request, rowSources, results, rowCount);
            } else {
                // TEXT source: concatenate and batch embed
                embedTextBatch(request, rowSources, results, rowCount);
            }
        }

        return Collections.unmodifiableList(results);
    }

    private void embedFileBatch(EmbedRequest request, List<Map<String, String>> rowSources,
                                List<Map<String, float[]>> results, int rowCount) {
        List<Map<String, String>> blobFieldsList = new ArrayList<>(rowCount);
        for (Map<String, String> rowSource : rowSources) {
            blobFieldsList.add(extractBlobStructFields(request.sourceColumns(), rowSource));
        }
        List<float[]> embeddings = blobEmbeddingResolver.embedBlobBatch(
                request.modelName(), blobFieldsList);
        applyEmbeddings(results, embeddings, request.vectorColumn(), rowCount);
    }

    private void embedTextBatch(EmbedRequest request, List<Map<String, String>> rowSources,
                                List<Map<String, float[]>> results, int rowCount) {
        EmbeddingProvider provider = resolveProvider(request.modelName());
        List<String> allTexts = buildTextsForBatch(request.sourceColumns(), rowSources);
        List<float[]> embeddings = batchEmbed(provider, allTexts, request.vectorColumn());
        applyEmbeddings(results, embeddings, request.vectorColumn(), rowCount);
    }

    private static void applyEmbeddings(List<Map<String, float[]>> results, List<float[]> embeddings,
                                        String vectorColumn, int rowCount) {
        for (int i = 0; i < rowCount; i++) {
            if (embeddings.get(i) != null) {
                results.get(i).put(vectorColumn, embeddings.get(i));
            }
        }
    }

    /**
     * Determines whether a row needs re-embedding on UPDATE.
     *
     * <p>Compares the old and new source column values for a request. If all source
     * columns are unchanged, the existing vector is retained (skip recompute).</p>
     *
     * @param request   the embedding request to check
     * @param oldValues previous source column values
     * @param newValues new source column values
     * @return true if source columns changed and re-embedding is needed
     */
    public boolean needsRecompute(EmbedRequest request,
                                  Map<String, String> oldValues,
                                  Map<String, String> newValues) {
        Objects.requireNonNull(request, "request must not be null");
        Objects.requireNonNull(oldValues, "oldValues must not be null");
        Objects.requireNonNull(newValues, "newValues must not be null");

        for (String sourceCol : request.sourceColumns()) {
            String oldVal = oldValues.get(sourceCol);
            String newVal = newValues.get(sourceCol);
            if (!Objects.equals(oldVal, newVal)) {
                return true;
            }
        }
        return false;
    }

    private List<String> buildTextsForBatch(List<String> sourceColumns,
                                            List<Map<String, String>> rowSources) {
        List<String> texts = new ArrayList<>(rowSources.size());
        for (Map<String, String> rowSource : rowSources) {
            texts.add(concatenateSourceColumns(sourceColumns, rowSource));
        }
        return texts;
    }

    private List<float[]> batchEmbed(EmbeddingProvider provider, List<String> texts,
                                     String vectorColumn) {
        int totalRows = texts.size();
        float[][] resultArray = new float[totalRows][];

        List<Integer> nonEmptyIndices = new ArrayList<>();
        List<String> nonEmptyTexts = new ArrayList<>();
        for (int i = 0; i < totalRows; i++) {
            if (!texts.get(i).isEmpty()) {
                nonEmptyIndices.add(i);
                nonEmptyTexts.add(texts.get(i));
            }
        }

        if (!nonEmptyTexts.isEmpty()) {
            int effectiveBatchSize = Math.min(batchSize, provider.maxBatchSize());
            for (int offset = 0; offset < nonEmptyTexts.size(); offset += effectiveBatchSize) {
                int end = Math.min(offset + effectiveBatchSize, nonEmptyTexts.size());
                List<String> batch = nonEmptyTexts.subList(offset, end);

                LOG.debug("event=sync_embed_batch column={} offset={} size={}",
                        vectorColumn, offset, batch.size());

                List<float[]> batchResults = callProviderBatch(provider, batch, vectorColumn);

                for (int i = 0; i < batchResults.size(); i++) {
                    int originalIndex = nonEmptyIndices.get(offset + i);
                    resultArray[originalIndex] = batchResults.get(i);
                }
            }
        }

        List<float[]> allEmbeddings = new ArrayList<>(totalRows);
        for (float[] embedding : resultArray) {
            allEmbeddings.add(embedding);
        }
        return allEmbeddings;
    }

    private EmbeddingProvider resolveProvider(String modelName) {
        EmbeddingProvider provider = providerLookup.resolve(modelName);
        if (provider == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "No embedding provider available for model: " + modelName);
        }
        return provider;
    }

    private float[] callProvider(EmbeddingProvider provider, String text, String vectorColumn) {
        try {
            return provider.embed(text);
        } catch (PangrovaException e) {
            throw e;
        } catch (Exception e) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "Embedding failed for column=" + vectorColumn
                            + " model=" + provider.modelName() + ": " + e.getMessage(),
                    e);
        }
    }

    private List<float[]> callProviderBatch(EmbeddingProvider provider, List<String> texts,
                                            String vectorColumn) {
        try {
            return provider.embedBatch(texts);
        } catch (PangrovaException e) {
            throw e;
        } catch (Exception e) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "Batch embedding failed for column=" + vectorColumn
                            + " model=" + provider.modelName()
                            + " batchSize=" + texts.size() + ": " + e.getMessage(),
                    e);
        }
    }

    private String concatenateSourceColumns(List<String> sourceColumns, Map<String, String> values) {
        return EmbeddingTextConcatenator.concatenate(sourceColumns, values);
    }

    /**
     * Embeds a single FILE row by extracting the Claim-Check struct fields
     * from the source column and delegating to BlobEmbeddingResolver.
     */
    private float[] embedFileRow(EmbedRequest request, Map<String, String> sourceValues) {
        Map<String, String> blobFields = extractBlobStructFields(request.sourceColumns(), sourceValues);
        if (blobFields == null) {
            LOG.debug("event=sync_embed_skip_null_file column={}", request.vectorColumn());
            return null;
        }
        return blobEmbeddingResolver.embedSingleBlob(request.modelName(), blobFields);
    }

    /**
     * Extracts Claim-Check struct fields from source values for a FILE source column.
     *
     * <p>For FILE sources, the first source column is the FILE column. Its struct fields
     * (uri, mime_type, size, checksum) are expected as prefixed keys in the source values map:
     * {@code file_col.uri}, {@code file_col.mime_type}, etc. If the column value itself
     * is present as a JSON/struct representation, the fields are extracted accordingly.</p>
     *
     * @return map of struct field names to values, or null if the FILE column is absent
     */
    private Map<String, String> extractBlobStructFields(List<String> sourceColumns,
                                                        Map<String, String> sourceValues) {
        // For FILE source, the first source column is the FILE column name
        String fileColumn = sourceColumns.get(0);

        // Try prefixed field access pattern: <column>.uri, <column>.mime_type, etc.
        String uri = sourceValues.get(fileColumn + ".uri");
        if (uri == null || uri.isBlank()) {
            // Fall back to direct field names (when flattened as uri, mime_type, size, checksum)
            uri = sourceValues.get("uri");
        }
        if (uri == null || uri.isBlank()) {
            return null;
        }

        Map<String, String> fields = new HashMap<>();
        fields.put("uri", uri);

        String mimeType = sourceValues.getOrDefault(fileColumn + ".mime_type",
                sourceValues.getOrDefault("mime_type", "application/octet-stream"));
        fields.put("mime_type", mimeType);

        String size = sourceValues.getOrDefault(fileColumn + ".size",
                sourceValues.getOrDefault("size", "0"));
        fields.put("size", size);

        String checksum = sourceValues.getOrDefault(fileColumn + ".checksum",
                sourceValues.getOrDefault("checksum", "unknown"));
        fields.put("checksum", checksum);

        return fields;
    }
}
