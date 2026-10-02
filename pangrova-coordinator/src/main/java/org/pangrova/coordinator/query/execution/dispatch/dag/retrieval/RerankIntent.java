package org.pangrova.coordinator.query.execution.dispatch.dag.retrieval;

/** Query-scoped control-plane metadata for one bounded Native rerank stage. */
public record RerankIntent(
        int inputLimit,
        int outputLimit,
        String modelId,
        String modelVersion,
        byte[] modelSignature,
        org.pangrova.protocol.v2.LogicalType modelOutputType,
        org.pangrova.protocol.v2.CapabilityLimits modelLimits,
        String query,
        String documentField) {

    public RerankIntent {
        if (inputLimit <= 0
                || inputLimit > org.pangrova.protocol.retrieval.RerankBinding.MAX_INPUT_ROWS
                || outputLimit <= 0 || outputLimit > inputLimit
                || blank(modelId) || blank(modelVersion)
                || modelSignature == null || modelSignature.length != 32
                || modelOutputType == null || !modelOutputType.hasSubstraitType()
                || blank(query) || blank(documentField)) {
            throw new IllegalArgumentException("rerank intent is invalid");
        }
        org.pangrova.protocol.ModelResourceEnvelopes.validate(modelLimits);
        org.pangrova.protocol.ModelResourceEnvelopes.resources(modelLimits, inputLimit, false);
        modelSignature = modelSignature.clone();
    }

    @Override
    public byte[] modelSignature() {
        return modelSignature.clone();
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
