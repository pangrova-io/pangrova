/**
 * SPI contracts for retrieval result reranking.
 *
 * <p>Defines the reranker interface and factory used by the RERANK BY SQL clause
 * to fuse and reorder candidates from multiple retrieval sources (vector search,
 * full-text search). Implementations are discovered via {@link java.util.ServiceLoader}.</p>
 */
package org.pangrova.spi.ai.rerank;
