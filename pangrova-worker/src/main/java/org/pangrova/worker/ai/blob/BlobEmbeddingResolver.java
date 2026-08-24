package org.pangrova.worker.ai.blob;

import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.spi.ai.embedding.BlobReference;
import org.pangrova.spi.ai.embedding.EmbeddingProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Resolves BlobReference from Claim-Check struct column values and delegates
 * to {@link EmbeddingProvider#embedBlob(List)} for multimodal embedding.
 *
 * <p>When {@code ai_embed('clip', blob_col)} is evaluated, the blob column value
 * is a Claim-Check struct containing {@code uri}, {@code mime_type}, {@code size},
 * and {@code checksum} fields. This resolver extracts those fields, constructs
 * {@link BlobReference} instances, and passes them to the provider for streaming
 * embedding computation.</p>
 *
 * <p>No blob content is loaded into JVM heap. The provider reads content via
 * streaming from the URI (max 64MB in-memory buffer; larger blobs use chunked
 * streaming).</p>
 *
 * <p>Thread safety: instances are safe for concurrent use from multiple worker
 * threads, provided the underlying {@link EmbeddingProvider} implementations
 * are thread-safe (as required by the SPI contract).</p>
 */
public final class BlobEmbeddingResolver {

    private static final Logger LOG = LoggerFactory.getLogger(BlobEmbeddingResolver.class);

    /** Claim-Check struct field: storage URI of the blob. */
    static final String FIELD_URI = "uri";

    /** Claim-Check struct field: MIME type of the content. */
    static final String FIELD_MIME_TYPE = "mime_type";

    /** Claim-Check struct field: content length in bytes. */
    static final String FIELD_SIZE = "size";

    /** Claim-Check struct field: SHA-256 hex digest. */
    static final String FIELD_CHECKSUM = "checksum";

    private final Function<String, EmbeddingProvider> providerLookup;

    /**
     * Creates a resolver that uses the given lookup to obtain embedding providers.
     *
     * @param providerLookup resolves model names to embedding providers (must not be null)
     */
    public BlobEmbeddingResolver(Function<String, EmbeddingProvider> providerLookup) {
        if (providerLookup == null) {
            throw new IllegalArgumentException("providerLookup must not be null");
        }
        this.providerLookup = providerLookup;
    }

    /**
     * Resolves a single blob column value to a BlobReference and computes its embedding.
     *
     * @param modelName       registered embedding model name (e.g., "clip_vit")
     * @param blobStructFields Claim-Check struct fields from the data file column value;
     *                         must contain keys: uri, mime_type, size, checksum
     * @return computed embedding vector
     * @throws PangrovaException if the model is unavailable or embedding fails
     * @throws IllegalArgumentException if modelName is blank or struct fields are invalid
     */
    public float[] embedSingleBlob(String modelName, Map<String, String> blobStructFields) {
        Objects.requireNonNull(modelName, "modelName must not be null");
        Objects.requireNonNull(blobStructFields, "blobStructFields must not be null");
        if (modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }

        BlobReference ref = resolveReference(blobStructFields);
        EmbeddingProvider provider = resolveProvider(modelName);

        LOG.debug("event=blob_embed_single model={} uri={} size={}",
                modelName, ref.uri(), ref.size());

        return callProviderBlob(provider, List.of(ref)).get(0);
    }

    /**
     * Resolves a batch of blob column values to BlobReferences and computes embeddings.
     *
     * <p>Each element in {@code blobStructFieldsList} represents one row's Claim-Check
     * struct. Rows with null struct fields (no blob present) produce a null embedding
     * in the result list at the corresponding index.</p>
     *
     * @param modelName            registered embedding model name
     * @param blobStructFieldsList list of per-row Claim-Check struct field maps;
     *                             null entries indicate absent blobs
     * @return list of embedding vectors in input order; null for rows with absent blobs
     * @throws PangrovaException if the model is unavailable or embedding fails
     */
    public List<float[]> embedBlobBatch(String modelName,
                                        List<Map<String, String>> blobStructFieldsList) {
        Objects.requireNonNull(modelName, "modelName must not be null");
        Objects.requireNonNull(blobStructFieldsList, "blobStructFieldsList must not be null");
        if (modelName.isBlank()) {
            throw new IllegalArgumentException("modelName must not be blank");
        }

        if (blobStructFieldsList.isEmpty()) {
            return List.of();
        }

        EmbeddingProvider provider = resolveProvider(modelName);

        List<Integer> nonNullIndices = new ArrayList<>();
        List<BlobReference> refs = new ArrayList<>();
        for (int i = 0; i < blobStructFieldsList.size(); i++) {
            Map<String, String> fields = blobStructFieldsList.get(i);
            if (fields != null) {
                nonNullIndices.add(i);
                refs.add(resolveReference(fields));
            }
        }

        if (refs.isEmpty()) {
            return Collections.nCopies(blobStructFieldsList.size(), null);
        }

        LOG.debug("event=blob_embed_batch model={} totalRows={} nonNullRows={}",
                modelName, blobStructFieldsList.size(), refs.size());

        List<float[]> blobEmbeddings = callProviderBlobBatched(provider, refs);

        float[][] results = new float[blobStructFieldsList.size()][];
        for (int i = 0; i < nonNullIndices.size(); i++) {
            results[nonNullIndices.get(i)] = blobEmbeddings.get(i);
        }

        List<float[]> resultList = new ArrayList<>(blobStructFieldsList.size());
        for (float[] result : results) {
            resultList.add(result);
        }
        return Collections.unmodifiableList(resultList);
    }

    /**
     * Extracts Claim-Check struct fields and constructs a BlobReference.
     *
     * @param fields map of struct field names to string values
     * @return resolved BlobReference
     * @throws IllegalArgumentException if required fields are missing or invalid
     */
    BlobReference resolveReference(Map<String, String> fields) {
        String uri = requireField(fields, FIELD_URI);
        String mimeType = requireField(fields, FIELD_MIME_TYPE);
        String sizeStr = requireField(fields, FIELD_SIZE);
        String checksum = requireField(fields, FIELD_CHECKSUM);

        long size;
        try {
            size = Long.parseLong(sizeStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid blob size value: '" + sizeStr + "' — must be a non-negative integer");
        }

        return new BlobReference(uri, mimeType, size, checksum);
    }

    private EmbeddingProvider resolveProvider(String modelName) {
        EmbeddingProvider provider = providerLookup.apply(modelName);
        if (provider == null) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "No embedding provider available for model: " + modelName);
        }
        return provider;
    }

    private List<float[]> callProviderBlob(EmbeddingProvider provider, List<BlobReference> refs) {
        try {
            return provider.embedBlob(refs);
        } catch (PangrovaException e) {
            throw e;
        } catch (Exception e) {
            throw new PangrovaException(
                    PangrovaErrorCode.AI_EMBEDDING_PROVIDER_UNAVAILABLE,
                    "Blob embedding failed for model=" + provider.modelName()
                            + " blobCount=" + refs.size() + ": " + e.getMessage(),
                    e);
        }
    }

    private List<float[]> callProviderBlobBatched(EmbeddingProvider provider,
                                                  List<BlobReference> refs) {
        int maxBatch = provider.maxBatchSize();
        if (refs.size() <= maxBatch) {
            return callProviderBlob(provider, refs);
        }

        List<float[]> allResults = new ArrayList<>(refs.size());
        for (int offset = 0; offset < refs.size(); offset += maxBatch) {
            int end = Math.min(offset + maxBatch, refs.size());
            List<BlobReference> batch = refs.subList(offset, end);

            LOG.debug("event=blob_embed_batch_chunk model={} offset={} size={}",
                    provider.modelName(), offset, batch.size());

            allResults.addAll(callProviderBlob(provider, batch));
        }
        return allResults;
    }

    private static String requireField(Map<String, String> fields, String fieldName) {
        String value = fields.get(fieldName);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "Blob Claim-Check struct missing required field: " + fieldName);
        }
        return value;
    }
}
