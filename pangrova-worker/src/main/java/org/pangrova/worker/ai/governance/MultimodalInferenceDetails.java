package org.pangrova.worker.ai.governance;

import org.pangrova.spi.ai.inference.Modality;

/**
 * Details of a single multimodal AI inference event for audit recording.
 *
 * <p>Groups the identity, table and model attributes passed to
 * {@link MultimodalAuditEnricher#recordMultimodalInference}.</p>
 *
 * @param principal       the user identity who invoked the model
 * @param queryId         the query that triggered the inference
 * @param tableName       the qualified table name
 * @param columnName      the FILE column used as input
 * @param blobUri         the blob URI of the input file
 * @param mimeType        the MIME type of the input
 * @param modelName       the AI model invoked
 * @param modality        the input modality
 * @param estimatedTokens estimated token cost of the operation
 */
public record MultimodalInferenceDetails(
        String principal,
        String queryId,
        String tableName,
        String columnName,
        String blobUri,
        String mimeType,
        String modelName,
        Modality modality,
        long estimatedTokens) {
}
