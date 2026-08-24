---
title: AI-Native Architecture
sidebar_label: AI-Native Architecture
---

# Pangrova — AI-Native Architecture

> Status: Draft
> Author: Pangrova Team
> Date: 2026-03-31

---

## §1 Design Philosophy

Pangrova's AI-native architecture operates on two distinct layers, each with a different direction of intelligence:

```
Layer 1 (Database WITH AI):  AI → Database    (Users and agents call AI functions from SQL)
Layer 2 (Database AS AI):    Database → AI    (The database uses AI to enhance itself)

Together:                    Database ⇄ AI    (Bidirectional intelligence)
```

**Layer 1 — Database WITH AI** provides AI capabilities as first-class SQL primitives: embedding generation (`ai_embed`), text generation (`ai_generate`), classification and entity extraction, full-text BM25 scoring, vector similarity search, hybrid score fusion, and auto-embedding DDL. The user or agent queries data using AI functions. The database is a passive executor.

**Layer 2 — Database AS AI** makes the database itself an intelligent system that observes, learns, and self-optimizes without user intervention. The database monitors its own retrieval quality, detects index staleness, learns cardinality from execution history, advises data layout, and maintains multi-turn conversation context for agent sessions.

### Layer 1: Five User-Facing Capabilities

| # | Capability | SQL Surface |
|---|-----------|-------------|
| 1 | Full-Text MATCH + BM25 Scoring | `MATCH(col, 'q')`, `text_score(col, 'q')` |
| 2 | In-DB Embedding Generation | `ai_embed(model, text)` |
| 3 | Unified Hybrid Search | `rrf(text_score(...), vector_score(...))` |
| 4 | External Model Invocation UDF | `ai_generate`, `ai_classify`, `ai_extract` |
| 5 | Universal Agent Skill | `pangrova-data/` agentskills.io package |

### Layer 2: Six Self-Optimizing Capabilities

| # | Capability | One-Sentence Description |
|---|-----------|--------------------------|
| 1 | Semantic Cache | Cache query results by meaning, not by exact SQL text |
| 2 | Retrieval Quality Observability | Measure and monitor the quality of AI search results |
| 3 | Vector Index Lifecycle Management | Auto-detect staleness, recommend rebuild, advise index type |
| 4 | AI-Driven Cardinality Estimation | Replace histogram-based estimation with learned models for stale-stats scenarios |
| 5 | Intelligent Data Layout Advisor | Recommend partition/sort/compaction strategies from query patterns |
| 6 | Conversation-Aware Query Context | Multi-turn session memory for agent interactions |

### Module Impact Overview (Layer 2)

All six self-optimizing capabilities follow existing dependency directions. No new modules created.

| Capability | SPI (pangrova-spi) | Planner | Coordinator | Worker | Indexing | Observability | Streaming |
|-----------|-------------------|---------|-------------|--------|----------|---------------|-----------|
| 1. Semantic Cache | + SemanticCacheProvider | + SemanticCacheKey | + SemanticCacheCoordinator | — | — | + metrics | — |
| 2. Retrieval Quality | + RetrievalQualitySnapshot | — | + RetrievalQualityCollector | + sampling executor | — | + metrics | — |
| 3. Vector Index Lifecycle | + VectorIndexHealthReport | — | + VectorIndexAdvisor | + staleness detector | + recall estimator | + metrics | — |
| 4. Learned Cardinality | — | + LearnedCardinalityEstimator | + feedback collector | — | — | + metrics | — |
| 5. Data Layout Advisor | + LayoutRecommendation | + QueryPatternRecorder | + LayoutAdvisor | — | — | + metrics | — |
| 6. Conversation Context | + ConversationContext | — | + ConversationManager | — | — | — | — |

---

## §2 Vector Search

### 2.1 Existing Capabilities (Verified Against Code)

| Capability | Key Components |
|---|---|
| VECTOR type | `VectorType(dimension, elementType)` — FLOAT/DOUBLE, dim [1, 65535] |
| HNSW index | `HnswEngine` — M=[4,64], efConstruction=[M,4096], efSearch=[topK,4096] |
| IVFPQ index | `IvfpqEngine` — nlist=[1,65536], pqM, nbits; `encodeIncremental()` with 30% staleness |
| Distance functions | `DistanceMetric` enum: L2/COSINE/INNER_PRODUCT; Java + AVX512/AVX2/NEON C++ |
| ANN query rewrite | `VectorSearchRewriter` → `VectorSearchNode` on ORDER BY distance() LIMIT K |
| Hybrid search planning | `VectorHybridSearchPlanner` — selectivity >0.5 → ANN_THEN_FILTER (1.5× over-fetch) |
| VECTOR DDL syntax | `VECTOR(dim, FLOAT\|DOUBLE)` in PangrovaSql.g4 |
| Distance function SQL | L2_DISTANCE/COSINE_DISTANCE/INNER_PRODUCT/COSINE_SIMILARITY in `PangrovaSqlOperatorTable` |

### 2.2 ANN Query Rewrite

The `VectorSearchRewriter` detects `ORDER BY distance_func(col, vec) ASC LIMIT K` and rewrites it into a `VectorSearchNode` that uses the HNSW or IVFPQ index. The `VectorHybridSearchPlanner` chooses between two strategies:
- **ANN_THEN_FILTER** (selectivity > 0.5): fetch 1.5× topK candidates from ANN, then apply WHERE predicates
- **FILTER_THEN_BRUTEFORCE** (selectivity ≤ 0.5): filter rows first, then brute-force distance computation on the surviving set

### 2.3 vector_score() Function (pangrova-planner)

New SQL function that converts distance to a normalized similarity score (higher = more similar).

**Registration in `PangrovaSqlOperatorTable`**:

```java
// VECTOR_SCORE(vector_column, query_vector) → DOUBLE NULLABLE
// Returns normalized similarity: 1/(1+distance) for L2, raw cosine_similarity for COSINE
public static final SqlFunction VECTOR_SCORE = new SqlFunction(
        "VECTOR_SCORE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
```

**Score normalization logic** (in worker-side execution):

```java
/**
 * Converts raw distance to normalized similarity score [0, 1].
 * Higher score = more similar.
 */
public static double distanceToScore(double distance, DistanceMetric metric) {
    return switch (metric) {
        case L2 -> 1.0 / (1.0 + distance);
        case COSINE -> 1.0 - distance;  // cosine_distance = 1 - similarity
        case INNER_PRODUCT -> -distance; // inner_product distance = -dot(a,b)
    };
}
```

**SQL examples**:

```sql
-- vector_score returns normalized similarity (higher = more similar)
SELECT id, vector_score(embedding, ARRAY[0.1, 0.2, ...]) AS similarity
FROM products
ORDER BY similarity DESC
LIMIT 10;

-- Combined with ai_embed:
SELECT id, vector_score(embedding, ai_embed('bge-m3', 'wireless headphones')) AS sim
FROM products
ORDER BY sim DESC
LIMIT 10;
```

### 2.4 HybridSearchFusionRule (pangrova-planner)

**New class**: `HybridSearchFusionRule` in `org.pangrova.planner.vector`

This is the core planner rule that detects hybrid search patterns and rewrites them into a two-path retrieval plan with score fusion.

```java
package org.pangrova.planner.vector;

/**
 * Calcite optimizer rule that detects hybrid search patterns
 * (text_score + vector_score in same query) and rewrites into
 * a two-path retrieval plan with score fusion.
 *
 * <h3>Detection Pattern</h3>
 * <pre>
 * LogicalSort(ORDER BY hybrid_score DESC, LIMIT K)
 *   └─ LogicalProject(rrf(text_score(col1, 'q'), vector_score(col2, vec)) AS hybrid_score)
 *       └─ LogicalFilter(MATCH(col1, 'q'))  [optional]
 *           └─ LogicalTableScan
 * </pre>
 *
 * <h3>Rewrite Output</h3>
 * <pre>
 * HybridFusionNode(fusionMethod=RRF, topK=K)
 *   ├─ TextRetrievalNode(col1, 'q', INVERTED index, over-fetch=3K)
 *   └─ VectorRetrievalNode(col2, vec, VECTOR index, over-fetch=3K)
 * </pre>
 *
 * <h3>Over-fetch Strategy</h3>
 * Each retrieval path fetches 3× topK candidates to ensure sufficient
 * overlap for accurate fusion. The fusion node then selects the final topK.
 *
 * <h3>Fallback</h3>
 * If either index is missing, falls back to sequential execution:
 * full scan with per-row scoring. The planner logs a warning.
 */
public final class HybridSearchFusionRule {

    /** Over-fetch multiplier for each retrieval path. */
    static final int OVER_FETCH_MULTIPLIER = 3;

    private HybridSearchFusionRule() {}

    /**
     * Attempts to rewrite a sort node into a hybrid fusion plan.
     *
     * @param sortNode    Calcite sort node (ORDER BY ... DESC LIMIT K)
     * @param indexLookup (tableName, columnName) → matching IndexDescriptors
     * @return rewritten HybridFusionNode, or empty if pattern does not match
     */
    public static Optional<HybridFusionNode> tryRewrite(
            LogicalSort sortNode,
            BiFunction<String, String, List<IndexDescriptor>> indexLookup) { ... }
}
```

**New plan node**: `HybridFusionNode` in `org.pangrova.planner.vector`

```java
package org.pangrova.planner.vector;

/**
 * Logical plan node for hybrid score fusion.
 *
 * @param tableName       fully qualified table name
 * @param textRetrieval   text retrieval sub-plan (BM25 via inverted index)
 * @param vectorRetrieval vector retrieval sub-plan (ANN via vector index)
 * @param fusionMethod    RRF or WEIGHTED_LINEAR
 * @param weights         weights for WEIGHTED_LINEAR (null for RRF)
 * @param topK            final number of results to return
 * @param preFilters      additional WHERE predicates applied to both paths
 */
public record HybridFusionNode(
        String tableName,
        TextRetrievalNode textRetrieval,
        VectorSearchNode vectorRetrieval,
        FusionMethod fusionMethod,
        double[] weights,
        int topK,
        List<String> preFilters
) {
    public enum FusionMethod { RRF, WEIGHTED_LINEAR }

    public HybridFusionNode {
        preFilters = preFilters == null ? List.of() : List.copyOf(preFilters);
        if (weights != null) weights = weights.clone();
    }
}
```

### 2.5 Module Impact Summary (Vector Search)

| Module | File | Change |
|---|---|---|
| `pangrova-planner` | `PangrovaSqlOperatorTable.java` | + VECTOR_SCORE function |
| `pangrova-planner` | `HybridSearchFusionRule.java` (new) | Hybrid search pattern detection + rewrite |
| `pangrova-planner` | `HybridFusionNode.java` (new) | Plan node for hybrid fusion |

---

## §3 Full-Text Search and Hybrid

### 3.1 Parser Extension (pangrova-parser)

**Grammar changes to `PangrovaSql.g4`**:

MATCH is integrated as a new expression alternative in the existing `expression` rule, keeping it composable with AND/OR/NOT in WHERE clauses.

```antlr
// Add to the expression rule (between likeExpr and notExpr):
expression
    : ...existing alternatives...
    | MATCH LPAREN columnRef=qualifiedName COMMA query=STRING_LITERAL
      (COMMA matchMode=(ALL | ANY | PHRASE))? RPAREN              #matchExpr
    ;

// Add new lexer token:
MATCH_KW    : M A T C H;
// Note: disambiguate from MATCHED via parser context (MATCH always followed by LPAREN).

// PHRASE is new:
PHRASE      : P H R A S E;

// Add to nonReservedKeyword:
nonReservedKeyword
    : ...existing...
    | MATCH_KW | PHRASE
    ;
```

**New AST node** in `pangrova-parser` (package `org.pangrova.parser.ast.expression`):

```java
/**
 * AST node for MATCH(column, 'query text', mode?) expression.
 * Produces boolean semantics in WHERE, used alongside text_score() for ranking.
 *
 * @param column    target column (must have INVERTED index)
 * @param queryText search query string literal
 * @param mode      MATCH_ALL (default), MATCH_ANY, or MATCH_PHRASE
 */
public record MatchExpression(
        Identifier column,
        String queryText,
        TextMatchMode mode   // reuses existing SPI enum
) implements Expression {}
```

**Visitor update** in `PangrovaSqlVisitor`:

```java
@Override
public Expression visitMatchExpr(PangrovaSqlParser.MatchExprContext ctx) {
    Identifier column = visitQualifiedName(ctx.columnRef);
    String queryText = unquote(ctx.query.getText());
    TextMatchMode mode = ctx.matchMode == null ? TextMatchMode.MATCH_ALL
            : switch (ctx.matchMode.getType()) {
                case PangrovaSqlParser.ANY -> TextMatchMode.MATCH_ANY;
                case PangrovaSqlParser.PHRASE -> TextMatchMode.MATCH_PHRASE;
                default -> TextMatchMode.MATCH_ALL;
            };
    return new MatchExpression(column, queryText, mode);
}
```

**Calcite conversion** in `ExpressionToSqlNodeConverter`:

```java
// In ExpressionToSqlNodeConverter.buildExprDispatch(), add:
entry(MatchExpression.class, this::convertMatch)

private SqlNode convertMatch(Expression expr) {
    MatchExpression match = (MatchExpression) expr;
    SqlNode col = id(match.column());
    SqlNode query = SqlLiteral.createCharString(match.queryText(), POS);
    SqlNode mode = SqlLiteral.createCharString(match.mode().name(), POS);
    return new SqlBasicCall(PangrovaSqlOperatorTable.MATCH_FUNCTION,
            new SqlNode[]{col, query, mode}, POS);
}
```

**New SQL functions** registered in `PangrovaSqlOperatorTable`:

```java
// MATCH(column, query, mode?) → BOOLEAN NULLABLE
// Used in WHERE clause for boolean filtering
public static final SqlFunction MATCH_FUNCTION = new SqlFunction(
        "MATCH",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

// TEXT_SCORE(column, query) → DOUBLE NULLABLE
// Returns BM25 relevance score for ranking
public static final SqlFunction TEXT_SCORE = new SqlFunction(
        "TEXT_SCORE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
```

**SQL examples**:

```sql
-- Boolean filter: does this row match?
SELECT * FROM docs WHERE MATCH(content, 'noise cancelling');

-- Boolean filter with mode:
SELECT * FROM docs WHERE MATCH(content, 'noise cancelling headphones', PHRASE);

-- Scored retrieval: how relevant is this row?
SELECT title, text_score(content, 'noise cancelling') AS relevance
FROM docs
WHERE MATCH(content, 'noise cancelling')
ORDER BY relevance DESC
LIMIT 10;

-- Combine with relational predicates:
SELECT title, text_score(content, 'wireless') AS relevance
FROM products
WHERE MATCH(content, 'wireless') AND price < 100.0
ORDER BY relevance DESC
LIMIT 20;
```

### 3.2 BM25 Scoring Engine (pangrova-indexing)

**New class**: `BM25Scorer` in `org.pangrova.indexing.engine.inverted`

```java
package org.pangrova.indexing.engine.inverted;

import java.util.List;
import java.util.Map;

/**
 * Stateless BM25 relevance scorer for inverted index queries.
 *
 * <p>BM25(q, d) = Σ IDF(qi) · (tf(qi, d) · (k1 + 1)) / (tf(qi, d) + k1 · (1 - b + b · |d|/avgdl))</p>
 *
 * <p>Default parameters follow Robertson & Zaragoza (2009):
 * k1 = 1.2 (term saturation), b = 0.75 (length normalization).</p>
 *
 * <p>Thread-safe: all methods are pure functions with no shared mutable state.</p>
 */
public final class BM25Scorer {

    static final double DEFAULT_K1 = 1.2;
    static final double DEFAULT_B = 0.75;

    private final double k1;
    private final double b;

    public BM25Scorer() {
        this(DEFAULT_K1, DEFAULT_B);
    }

    public BM25Scorer(double k1, double b) {
        if (k1 < 0) throw new IllegalArgumentException("k1 must be non-negative, got: " + k1);
        if (b < 0 || b > 1) throw new IllegalArgumentException("b must be in [0, 1], got: " + b);
        this.k1 = k1;
        this.b = b;
    }

    /**
     * Computes BM25 score for a single document against a query.
     *
     * @param queryTokens    tokenized query terms (from AnalyzerFactory.tokenize())
     * @param termFrequencies per-term frequency in this document: term → count
     * @param docLength       number of tokens in this document
     * @param avgDocLength    average document length across the corpus (from InvertedIndexData)
     * @param totalDocs       total number of documents in the corpus (from InvertedIndexData)
     * @param docFrequencies  per-term document frequency: term → number of docs containing term
     * @return BM25 score (higher = more relevant); 0.0 if no query terms match
     */
    public double score(List<String> queryTokens,
                        Map<String, Integer> termFrequencies,
                        int docLength,
                        double avgDocLength,
                        long totalDocs,
                        Map<String, Long> docFrequencies) {
        double score = 0.0;
        for (String term : queryTokens) {
            int tf = termFrequencies.getOrDefault(term, 0);
            if (tf == 0) continue;

            long df = docFrequencies.getOrDefault(term, 0L);
            double idf = idf(totalDocs, df);
            double tfNorm = (tf * (k1 + 1.0))
                    / (tf + k1 * (1.0 - b + b * docLength / avgDocLength));
            score += idf * tfNorm;
        }
        return score;
    }

    /**
     * IDF with smoothing to avoid negative values for very common terms.
     * IDF(qi) = ln((N - n(qi) + 0.5) / (n(qi) + 0.5) + 1)
     */
    static double idf(long totalDocs, long docFreq) {
        return Math.log((totalDocs - docFreq + 0.5) / (docFreq + 0.5) + 1.0);
    }
}
```

### 3.3 Extended InvertedIndexData (pangrova-spi)

BM25 needs corpus-level statistics. The existing `InvertedIndexData` record is extended with backward-compatible new fields.

**Current** (2 fields): `postings`, `storePositions`
**New** (5 fields): `postings`, `storePositions`, `totalDocuments`, `avgDocumentLength`, `docFrequencies`

```java
package org.pangrova.spi.index.data.text;

/**
 * Inverted index data with optional BM25 corpus statistics.
 *
 * <p>When BM25 statistics are absent (legacy indexes), scoring falls back
 * to binary SKIP/KEEP. Statistics are populated during index build when
 * the {@code inverted.store_stats=true} property is set.</p>
 *
 * @param postings           term → posting list (row positions or empty list for term-set mode)
 * @param storePositions     whether row positions are stored (for PHRASE matching)
 * @param totalDocuments     total documents indexed in this file (0 = stats unavailable)
 * @param avgDocumentLength  average document length in tokens (0.0 = stats unavailable)
 * @param docFrequencies     per-term document frequency: term → doc count (empty = stats unavailable)
 */
public record InvertedIndexData(
        Map<String, List<Integer>> postings,
        boolean storePositions,
        long totalDocuments,
        double avgDocumentLength,
        Map<String, Long> docFrequencies
) implements IndexData {

    /** Backward-compatible constructor for existing callers (no BM25 stats). */
    public InvertedIndexData(Map<String, List<Integer>> postings, boolean storePositions) {
        this(postings, storePositions, 0, 0.0, Map.of());
    }

    /** Convenience constructor with storePositions=false and no stats. */
    public InvertedIndexData(Map<String, List<Integer>> postings) {
        this(postings, false, 0, 0.0, Map.of());
    }

    public InvertedIndexData {
        postings = deepCopyPostings(postings);
        docFrequencies = docFrequencies == null ? Map.of() : Map.copyOf(docFrequencies);
        if (totalDocuments < 0) throw new IllegalArgumentException("totalDocuments must be >= 0");
        if (avgDocumentLength < 0) throw new IllegalArgumentException("avgDocumentLength must be >= 0");
    }

    /** Returns true if BM25 corpus statistics are available for scoring. */
    public boolean hasStatistics() {
        return totalDocuments > 0 && avgDocumentLength > 0 && !docFrequencies.isEmpty();
    }

    @Override
    public IndexType indexType() { return IndexType.INVERTED; }

    @Override
    public long estimatedSizeInBytes() {
        long base = postings.entrySet().stream()
                .mapToLong(e -> e.getKey().length() * 2L + 32L
                        + (storePositions ? e.getValue().size() * 4L : 0L))
                .sum();
        long statsSize = docFrequencies.entrySet().stream()
                .mapToLong(e -> e.getKey().length() * 2L + 8L + 32L)
                .sum();
        return base + statsSize;
    }
}
```

**Migration**: Existing serialized indexes (via `InvertedSerializer`) deserialize with `totalDocuments=0`, `avgDocumentLength=0.0`, `docFrequencies=Map.of()`. The `hasStatistics()` method returns false, and scoring falls back to binary SKIP/KEEP. No data migration needed.

### 3.4 Score Fusion Functions (pangrova-planner)

**Registration in `PangrovaSqlOperatorTable`**:

```java
// RRF(score1, score2, ...) → DOUBLE NULLABLE
// Reciprocal Rank Fusion: parameter-free, robust across score distributions
public static final SqlFunction RRF = new SqlFunction(
        "RRF",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

// WEIGHTED_SCORE(weight1, score1, weight2, score2, ...) → DOUBLE NULLABLE
// Weighted linear combination with min-max normalization
public static final SqlFunction WEIGHTED_SCORE = new SqlFunction(
        "WEIGHTED_SCORE",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
```

**SQL examples**:

```sql
-- Reciprocal Rank Fusion (industry standard for hybrid search)
SELECT title,
    rrf(text_score(content, 'noise cancelling'),
        vector_score(embedding, ai_embed('bge-m3', 'noise cancelling'))) AS hybrid_score
FROM products
WHERE MATCH(content, 'noise cancelling')
ORDER BY hybrid_score DESC
LIMIT 10;

-- Weighted linear combination (tunable weights)
SELECT title,
    weighted_score(
        0.3, text_score(content, 'noise cancelling'),
        0.7, vector_score(embedding, ai_embed('bge-m3', 'noise cancelling'))) AS hybrid_score
FROM products
WHERE MATCH(content, 'noise cancelling')
ORDER BY hybrid_score DESC
LIMIT 10;

-- Three-way fusion (text + vector + recency)
SELECT title,
    rrf(text_score(content, 'wireless'),
        vector_score(embedding, ai_embed('bge-m3', 'wireless')),
        1.0 / (EXTRACT(DAY FROM CURRENT_DATE - publish_date) + 1)) AS score
FROM articles
WHERE MATCH(content, 'wireless')
ORDER BY score DESC
LIMIT 20;
```

### 3.5 Score Fusion Algorithms (pangrova-indexing)

**New class**: `ScoreFusion` in `org.pangrova.indexing.eval`

```java
package org.pangrova.indexing.eval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Score fusion algorithms for combining ranked lists from different retrieval methods.
 *
 * <p>All methods are stateless and thread-safe.</p>
 */
public final class ScoreFusion {

    /** Default RRF constant (Cormack et al., 2009). */
    static final int DEFAULT_RRF_K = 60;

    private ScoreFusion() {}

    /**
     * Reciprocal Rank Fusion.
     *
     * <p>score(d) = Σ 1/(k + rank_i(d)) where k is a constant (default 60)
     * and rank_i is the 1-based rank of document d in the i-th ranked list.</p>
     *
     * @param rankedLists list of ranked result lists (each sorted by score descending)
     * @param topK        number of results to return
     * @param k           RRF constant (typically 60)
     * @return fused results sorted by RRF score descending, size ≤ topK
     */
    public static List<ScoredDocument> rrf(
            List<List<ScoredDocument>> rankedLists, int topK, int k) {
        Map<Long, Double> fusedScores = new HashMap<>();

        for (List<ScoredDocument> list : rankedLists) {
            for (int rank = 0; rank < list.size(); rank++) {
                long rowId = list.get(rank).rowId();
                fusedScores.merge(rowId, 1.0 / (k + rank + 1), Double::sum);
            }
        }

        return fusedScores.entrySet().stream()
                .map(e -> new ScoredDocument(e.getKey(), e.getValue()))
                .sorted((a, b) -> Double.compare(b.score(), a.score()))
                .limit(topK)
                .toList();
    }

    /** RRF with default k=60. */
    public static List<ScoredDocument> rrf(
            List<List<ScoredDocument>> rankedLists, int topK) {
        return rrf(rankedLists, topK, DEFAULT_RRF_K);
    }

    /**
     * Weighted linear combination with min-max normalization.
     *
     * <p>Each score list is normalized to [0, 1] via min-max scaling,
     * then combined: score(d) = Σ weight_i × normalized_score_i(d).</p>
     *
     * @param rankedLists list of ranked result lists
     * @param weights     weight for each list (must sum to 1.0, same length as rankedLists)
     * @param topK        number of results to return
     * @return fused results sorted by weighted score descending, size ≤ topK
     * @throws IllegalArgumentException if weights.length != rankedLists.size()
     */
    public static List<ScoredDocument> weightedLinear(
            List<List<ScoredDocument>> rankedLists,
            double[] weights, int topK) {
        if (weights.length != rankedLists.size()) {
            throw new IllegalArgumentException(
                    "weights length (" + weights.length + ") must match rankedLists size ("
                    + rankedLists.size() + ")");
        }

        List<Map<Long, Double>> normalizedMaps = new ArrayList<>();
        for (List<ScoredDocument> list : rankedLists) {
            normalizedMaps.add(minMaxNormalize(list));
        }

        Map<Long, Double> fusedScores = new HashMap<>();
        for (int i = 0; i < normalizedMaps.size(); i++) {
            double weight = weights[i];
            for (var entry : normalizedMaps.get(i).entrySet()) {
                fusedScores.merge(entry.getKey(), weight * entry.getValue(), Double::sum);
            }
        }

        return fusedScores.entrySet().stream()
                .map(e -> new ScoredDocument(e.getKey(), e.getValue()))
                .sorted((a, b) -> Double.compare(b.score(), a.score()))
                .limit(topK)
                .toList();
    }

    private static Map<Long, Double> minMaxNormalize(List<ScoredDocument> list) {
        if (list.isEmpty()) return Map.of();
        double min = list.stream().mapToDouble(ScoredDocument::score).min().orElse(0);
        double max = list.stream().mapToDouble(ScoredDocument::score).max().orElse(0);
        double range = max - min;
        Map<Long, Double> normalized = new HashMap<>(list.size());
        for (ScoredDocument doc : list) {
            normalized.put(doc.rowId(), range == 0 ? 0.5 : (doc.score() - min) / range);
        }
        return normalized;
    }
}
```

**New record** in `org.pangrova.indexing.eval`:

```java
/**
 * A document with a fusion score. Used as the output of score fusion algorithms.
 *
 * @param rowId  row identifier (byte offset in data file, consistent with VectorSearchResult.ScoredRow)
 * @param score  fused relevance score (higher = more relevant)
 */
public record ScoredDocument(long rowId, double score) {}
```

### 3.6 Module Impact Summary (Full-Text Search and Hybrid)

| Module | File | Change |
|---|---|---|
| `pangrova-parser` | `PangrovaSql.g4` | + MATCH expression rule, + MATCH_KW/PHRASE tokens |
| `pangrova-parser` | `MatchExpression.java` (new) | AST node for MATCH() |
| `pangrova-parser` | `PangrovaSqlVisitor.java` | + visitMatchExpr() |
| `pangrova-spi` | `InvertedIndexData.java` | + totalDocuments, avgDocumentLength, docFrequencies fields |
| `pangrova-spi` | `IndexStrategyProvider.java` | + evaluateWithScore() default method |
| `pangrova-indexing` | `BM25Scorer.java` (new) | BM25 scoring algorithm |
| `pangrova-indexing` | `InvertedStrategyProvider.java` | + stats collection in build(), + evaluateWithScore() impl |
| `pangrova-indexing` | `InvertedSerializer.java` | + v2 format with stats |
| `pangrova-indexing` | `ScoreFusion.java` (new) | RRF + weighted linear algorithms |
| `pangrova-indexing` | `ScoredDocument.java` (new) | Fusion result record |
| `pangrova-planner` | `PangrovaSqlOperatorTable.java` | + MATCH_FUNCTION, TEXT_SCORE, RRF, WEIGHTED_SCORE operators |
| `pangrova-planner` | `ExpressionToSqlNodeConverter.java` | + convertMatch() |
| `pangrova-planner` | `TextSearchRewriter.java` (new) | ORDER BY text_score() rewrite |
| `pangrova-planner` | `TextRetrievalNode.java` (new) | Plan node for text retrieval |
| `pangrova-worker` | `TextSearchExecutor.java` (new) | BM25 execution on worker |
| `pangrova-coordinator` | `HybridSearchAggregator.java` (new) | Coordinator-side fusion execution |

---

## §4 AI SQL Functions

### 4.1 All Nine AI Functions

| Function | Signature | Returns | Use Case |
|---|---|---|---|
| `ai_embed` | `ai_embed(model, text)` | ARRAY (float[]) | Generate embedding vector |
| `ai_generate` | `ai_generate(model, prompt)` | VARCHAR | Text generation, summarization |
| `ai_classify` | `ai_classify(model, text, categories)` | VARCHAR | Classification |
| `ai_extract` | `ai_extract(model, text, schema)` | VARCHAR (JSON) | Entity extraction |
| `text_score` | `text_score(column, query)` | DOUBLE | BM25 relevance score |
| `vector_score` | `vector_score(column, vector)` | DOUBLE | Normalized similarity score |
| `rrf` | `rrf(score1, score2, ...)` | DOUBLE | Reciprocal Rank Fusion |
| `weighted_score` | `weighted_score(w1, s1, w2, s2, ...)` | DOUBLE | Weighted linear combination |
| `hybrid_score` | composed via `rrf`/`weighted_score` | DOUBLE | Multi-signal ranking |

### 4.2 EmbeddingProvider SPI (pangrova-spi)

**New package**: `org.pangrova.spi.ai`

```java
package org.pangrova.spi.ai;

import java.util.List;

/**
 * SPI contract for embedding model providers.
 *
 * <p>Implementations handle HTTP/gRPC calls to external model services.
 * Discovered via {@link java.util.ServiceLoader}, selected by model name.</p>
 *
 * <p>Implementations must be thread-safe. A single provider instance
 * serves all concurrent queries.</p>
 *
 * <p>Error handling: implementations throw {@link EmbeddingGenerationException}
 * (extends RuntimeException) on failure. The exception carries the model name,
 * HTTP status code (if applicable), and a user-facing message.</p>
 */
public interface EmbeddingModelProvider {

    /** Model identifier (e.g., "text-embedding-3-small", "bge-m3"). */
    String modelName();

    /** Output vector dimension for this model. Used for type validation. */
    int dimension();

    /**
     * Generate embedding for a single text input.
     *
     * @param text input text to embed (must not be null or blank)
     * @return float array of exactly {@link #dimension()} length
     * @throws EmbeddingGenerationException on model invocation failure
     * @throws IllegalArgumentException if text is null or blank
     */
    float[] embed(String text);

    /**
     * Batch embedding for multiple texts.
     *
     * <p>Default implementation calls {@link #embed(String)} sequentially.
     * Providers should override for efficiency (single HTTP call with batch payload).</p>
     *
     * @param texts list of input texts (each must not be null or blank)
     * @return list of float arrays, same order and size as input
     * @throws EmbeddingGenerationException on model invocation failure
     */
    default List<float[]> embedBatch(List<String> texts) {
        return texts.stream().map(this::embed).toList();
    }

    /**
     * Maximum number of texts per batch call. Providers override based on
     * model API limits (e.g., OpenAI allows 2048 per batch).
     *
     * @return max batch size (default 100)
     */
    default int maxBatchSize() {
        return 100;
    }
}
```

**New exception** in `org.pangrova.spi.ai`:

```java
package org.pangrova.spi.ai;

/**
 * Thrown when embedding generation fails.
 * Carries structured context for error reporting.
 */
public class EmbeddingGenerationException extends RuntimeException {

    private final String modelName;
    private final int httpStatusCode; // -1 if not HTTP-related

    public EmbeddingGenerationException(String modelName, String message, Throwable cause) {
        this(modelName, -1, message, cause);
    }

    public EmbeddingGenerationException(String modelName, int httpStatusCode,
                                         String message, Throwable cause) {
        super("Embedding generation failed for model '" + modelName + "': " + message, cause);
        this.modelName = modelName;
        this.httpStatusCode = httpStatusCode;
    }

    public String modelName() { return modelName; }
    public int httpStatusCode() { return httpStatusCode; }
}
```

### 4.3 InferenceProvider SPI (pangrova-spi)

**New interface** in `org.pangrova.spi.ai`:

```java
package org.pangrova.spi.ai;

import java.util.Map;

/**
 * SPI for invoking external AI models from SQL.
 *
 * <p>Broader than {@link EmbeddingModelProvider} — supports text generation,
 * classification, and structured output. Implementations handle HTTP/gRPC
 * calls to LLM services.</p>
 *
 * <p>Discovered via {@link java.util.ServiceLoader}. Multiple providers
 * can coexist (e.g., OpenAI provider + Ollama provider).</p>
 *
 * <p>Thread-safe: implementations must handle concurrent invocations.</p>
 */
public interface ModelInvocationProvider {

    /** Provider identifier (e.g., "openai", "ollama", "bedrock"). */
    String providerName();

    /** Returns the list of model names this provider supports. */
    java.util.Set<String> supportedModels();

    /**
     * Invoke a model with a text prompt and return text output.
     *
     * @param modelName  model identifier (e.g., "gpt-4o-mini", "llama3")
     * @param prompt     input prompt text
     * @param parameters optional parameters (temperature, max_tokens, etc.)
     * @return model output text
     * @throws ModelInvocationException on failure
     */
    String invoke(String modelName, String prompt, Map<String, String> parameters);

    /**
     * Invoke a model and return structured JSON output.
     *
     * <p>Uses JSON mode or structured output API when available.
     * Falls back to prompt-based JSON extraction otherwise.</p>
     *
     * @param modelName  model identifier
     * @param prompt     input prompt text
     * @param jsonSchema expected output JSON schema (for validation)
     * @param parameters optional parameters
     * @return JSON string conforming to the schema
     * @throws ModelInvocationException on failure
     */
    String invokeStructured(String modelName, String prompt,
                            String jsonSchema, Map<String, String> parameters);
}
```

**New exception** in `org.pangrova.spi.ai`:

```java
/**
 * Thrown when model invocation fails.
 */
public class ModelInvocationException extends RuntimeException {
    private final String providerName;
    private final String modelName;
    private final int httpStatusCode;

    public ModelInvocationException(String providerName, String modelName,
                                     int httpStatusCode, String message, Throwable cause) {
        super("Model invocation failed [" + providerName + "/" + modelName + "]: " + message, cause);
        this.providerName = providerName;
        this.modelName = modelName;
        this.httpStatusCode = httpStatusCode;
    }

    public String providerName() { return providerName; }
    public String modelName() { return modelName; }
    public int httpStatusCode() { return httpStatusCode; }
}
```

### 4.4 Model Provider Registry (pangrova-coordinator)

**New package**: `org.pangrova.coordinator.ai`

```java
package org.pangrova.coordinator.ai;

import org.pangrova.spi.ai.EmbeddingModelProvider;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Registry of available embedding model providers.
 *
 * <p>Loaded at bootstrap from YAML configuration. Immutable after construction.
 * Thread-safe: the internal map is unmodifiable.</p>
 */
public final class EmbeddingModelRegistry {

    private final Map<String, EmbeddingModelProvider> providers;

    public EmbeddingModelRegistry(List<EmbeddingModelProvider> providers) {
        this.providers = providers.stream()
                .collect(Collectors.toUnmodifiableMap(
                        EmbeddingModelProvider::modelName, Function.identity()));
    }

    /**
     * Resolves a provider by model name.
     *
     * @throws ModelNotFoundException if no provider is registered for the given name
     */
    public EmbeddingModelProvider resolve(String modelName) {
        EmbeddingModelProvider provider = providers.get(modelName);
        if (provider == null) {
            throw new ModelNotFoundException(modelName, providers.keySet());
        }
        return provider;
    }

    /** Returns all registered model names (for error messages and SHOW MODELS). */
    public Set<String> registeredModels() {
        return providers.keySet();
    }
}
```

### 4.5 Embedding Pipeline

**SQL function registration** in `PangrovaSqlOperatorTable`:

```java
// AI_EMBED(model_name, text) → ARRAY of DOUBLE (represents VECTOR)
// At planning time: if both args are literals, evaluate immediately (constant folding)
// At execution time: if text is a column ref, batch-evaluate on worker
public static final SqlFunction AI_EMBED = new SqlFunction(
        "AI_EMBED",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TO_ARRAY,
        null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
```

**Constant folding** in `ConstantFoldingRewriter` (existing class in pangrova-planner):

When `ai_embed('model', 'literal text')` is detected with both arguments as string literals, the planner resolves the embedding at planning time:

```java
// In ConstantFoldingRewriter, add case for AI_EMBED:
if (call.getOperator().getName().equals("AI_EMBED")
        && allOperandsAreLiterals(call)) {
    String modelName = extractStringLiteral(call.operand(0));
    String text = extractStringLiteral(call.operand(1));
    EmbeddingModelProvider provider = embeddingRegistry.resolve(modelName);
    float[] embedding = provider.embed(text);
    return createArrayLiteral(embedding);
}
```

**Execution-time embedding** — when `ai_embed()` references a column (not a literal), the worker batches calls via the coordinator's internal embedding endpoint:

```java
package org.pangrova.worker.execution;

/**
 * Executes ai_embed() calls at task execution time.
 *
 * <p>When ai_embed() references a column value (not a literal), the planner
 * cannot constant-fold it. Instead, the worker batches column values and
 * calls the coordinator's embedding endpoint.</p>
 *
 * <p>Batching strategy:
 * <ol>
 *   <li>Accumulate text values from the current batch (up to maxBatchSize)</li>
 *   <li>Call coordinator embedding endpoint: POST /v1/internal/embed</li>
 *   <li>Receive float[][] response</li>
 *   <li>Attach embeddings to output rows</li>
 * </ol>
 */
public final class EmbeddingExecutor {

    private final String coordinatorEmbedEndpoint;
    private final java.net.http.HttpClient httpClient;

    public EmbeddingExecutor(String coordinatorHost, int coordinatorPort) {
        this.coordinatorEmbedEndpoint = "http://" + coordinatorHost + ":" + coordinatorPort
                + "/v1/internal/embed";
        this.httpClient = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(5))
                .build();
    }

    /**
     * Generates embeddings for a batch of text values.
     *
     * @param modelName  embedding model identifier
     * @param texts      batch of text values to embed
     * @return float[][] embeddings in same order as input
     */
    public float[][] embedBatch(String modelName, List<String> texts) { ... }
}
```

**SQL examples**:

```sql
-- Literal text: embedding resolved at planning time (constant folding)
SELECT * FROM products
ORDER BY l2_distance(embedding, ai_embed('text-embedding-3-small', 'wireless headphones'))
LIMIT 10;

-- Column reference: embedding resolved at execution time (per-row)
INSERT INTO docs (title, content, embedding)
VALUES ('AirPods', 'Wireless earbuds', ai_embed('bge-m3', 'Wireless earbuds'));
```

**Model invocation SQL functions**:

```java
// AI_GENERATE(model, prompt) → VARCHAR
public static final SqlFunction AI_GENERATE = new SqlFunction(
        "AI_GENERATE", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);

// AI_CLASSIFY(model, text, categories_array) → VARCHAR
public static final SqlFunction AI_CLASSIFY = new SqlFunction(
        "AI_CLASSIFY", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.VARIADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

// AI_EXTRACT(model, text, schema_json) → VARCHAR (JSON)
public static final SqlFunction AI_EXTRACT = new SqlFunction(
        "AI_EXTRACT", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_2000, null,
        OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
```

**Model invocation SQL examples**:

```sql
-- Text generation (summarization)
SELECT title, ai_generate('gpt-4o-mini', 'Summarize in one sentence: ' || content) AS summary
FROM articles
WHERE publish_date >= '2026-01-01'
LIMIT 10;

-- Classification (sentiment analysis)
SELECT title,
    ai_classify('gpt-4o-mini', content, ARRAY['positive', 'negative', 'neutral']) AS sentiment
FROM reviews
WHERE product_id = 42;

-- Entity extraction (returns JSON)
SELECT title,
    ai_extract('gpt-4o-mini', content,
        '{"entities": ["person", "organization", "location"]}') AS entities
FROM news
WHERE MATCH(content, 'acquisition merger')
LIMIT 20;
```

### 4.6 Auto-Embedding DDL (pangrova-parser + pangrova-metadata)

**Grammar extension** in `PangrovaSql.g4`:

```antlr
columnDefinition
    : identifier columnType
    | identifier columnType GENERATED ALWAYS AS LPAREN expression RPAREN STORED
    ;

GENERATED   : G E N E R A T E D;
ALWAYS      : A L W A Y S;
STORED      : S T O R E D;
```

**Extended AST node**:

```java
public record ColumnDefinition(
        String name,
        String type,
        Expression generatedExpression,  // null if not generated
        boolean stored                    // true for STORED generated columns
) {}
```

**DML enrichment** — when processing INSERT/UPDATE on a table with generated vector columns, `DdlExecutor` delegates to `GeneratedColumnPopulator`:

```java
/**
 * Populates generated vector columns during DML operations.
 *
 * <p>Sits between the SQL executor and the connector write path.
 * Only activates when the target table has GENERATED ALWAYS AS columns.</p>
 */
final class GeneratedColumnPopulator {

    private final EmbeddingModelRegistry embeddingRegistry;
    private final CatalogRegistry catalogRegistry;

    /**
     * Enriches INSERT values with generated column values.
     *
     * @param tableMeta  table metadata (includes generated column definitions)
     * @param rows       original INSERT values
     * @return enriched rows with generated columns populated
     */
    List<List<Object>> enrichInsertValues(
            ConnectorTableMetadata tableMeta,
            List<List<Object>> rows) { ... }
}
```

**Auto-embedding DDL example**:

```sql
CREATE TABLE docs (
    id BIGINT,
    content VARCHAR,
    embedding VECTOR(1536) GENERATED ALWAYS AS (ai_embed('text-embedding-3-small', content)) STORED
);

-- Subsequent INSERT automatically computes and stores the embedding:
INSERT INTO docs (id, content) VALUES (1, 'Wireless noise-cancelling headphones');
```

### 4.7 Embedding Provider Configuration (YAML)

```yaml
pangrova:
  ai:
    embedding:
      providers:
        - name: "text-embedding-3-small"
          endpoint: "https://api.openai.com/v1/embeddings"
          api-key-env: "OPENAI_API_KEY"
          dimension: 1536
          max-batch-size: 100
          timeout-ms: 5000
          max-retries: 3
        - name: "bge-m3"
          endpoint: "http://localhost:11434/v1/embeddings"
          dimension: 1024
          max-batch-size: 32
          timeout-ms: 10000
          max-retries: 2
    invocation:
      providers:
        - name: "openai"
          endpoint: "https://api.openai.com/v1/chat/completions"
          api-key-env: "OPENAI_API_KEY"
          models: ["gpt-4o-mini", "gpt-4o"]
          timeout-ms: 30000
          max-retries: 2
        - name: "ollama"
          endpoint: "http://localhost:11434/v1/chat/completions"
          models: ["llama3", "mistral"]
          timeout-ms: 60000
          max-retries: 1
    rate-limits:
      default:
        requests-per-second: 100
        tokens-per-minute: 100000
      per-model:
        gpt-4o-mini:
          requests-per-second: 50
          tokens-per-minute: 200000
    cache:
      embedding-ttl-minutes: 60
      generation-ttl-minutes: 5
      max-entries: 10000
```

### 4.8 Module Impact Summary (AI SQL Functions)

| Module | File | Change |
|---|---|---|
| `pangrova-spi` | `EmbeddingModelProvider.java` (new) | SPI contract |
| `pangrova-spi` | `EmbeddingGenerationException.java` (new) | Exception type |
| `pangrova-spi` | `ModelInvocationProvider.java` (new) | SPI contract |
| `pangrova-spi` | `ModelInvocationException.java` (new) | Exception type |
| `pangrova-coordinator` | `EmbeddingModelRegistry.java` (new) | Provider registry |
| `pangrova-coordinator` | `ModelNotFoundException.java` (new) | Error type |
| `pangrova-coordinator` | `HttpEmbeddingProvider.java` (new) | OpenAI-compatible HTTP impl |
| `pangrova-coordinator` | `ModelInvocationRegistry.java` (new) | Provider registry |
| `pangrova-coordinator` | `HttpModelInvocationProvider.java` (new) | OpenAI-compatible HTTP impl |
| `pangrova-coordinator` | `GeneratedColumnPopulator.java` (new) | DML enrichment |
| `pangrova-coordinator` | Internal endpoints | POST /v1/internal/embed, POST /v1/internal/invoke |
| `pangrova-planner` | `PangrovaSqlOperatorTable.java` | + AI_EMBED, AI_GENERATE, AI_CLASSIFY, AI_EXTRACT functions |
| `pangrova-planner` | `ConstantFoldingRewriter.java` | + ai_embed constant folding |
| `pangrova-planner` | `StatisticsBasedCostEstimator.java` | + MODEL_INVOCATION_COST |
| `pangrova-parser` | `PangrovaSql.g4` | + GENERATED ALWAYS AS syntax |
| `pangrova-worker` | `EmbeddingExecutor.java` (new) | Execution-time embedding |
| `pangrova-worker` | `ModelInvocationExecutor.java` (new) | Batch model invocation |

---

## §5 Semantic Cache

### 5.1 Problem

The existing `QueryResultCache` uses `CacheKey` based on `normalizedSql + tenantId + principal + sessionProperties + policyVersion`. This is exact-match: `"wireless headphones"` and `"bluetooth earbuds"` produce completely different SHA-256 digests despite being semantically equivalent.

In RAG/agent scenarios, natural language queries almost never repeat exactly. Cache hit rate approaches zero. Meanwhile, the embedding for `"wireless headphones"` costs ~$0.0001 per call and takes ~50ms — a semantic cache that recognizes similar queries can eliminate redundant model invocations and query executions.

### 5.2 Design Principle

Reuse Pangrova's own HNSW vector index infrastructure to build a query embedding index. The semantic cache sits as an optional layer in front of `QueryResultCache`, not replacing it.

```
Query arrives
    │
    ▼
┌─────────────────────────────┐
│ 1. Exact match (existing)   │ ← CacheKey SHA-256 lookup in QueryResultCache
│    Hit? → return immediately │
└──────────┬──────────────────┘
           │ Miss
           ▼
┌─────────────────────────────┐
│ 2. Semantic match (NEW)     │ ← Embed query → HNSW search → cosine < threshold?
│    Hit? → return cached     │
└──────────┬──────────────────┘
           │ Miss
           ▼
┌─────────────────────────────┐
│ 3. Execute query            │
│    → Cache result (exact)   │
│    → Index embedding (sem)  │
└─────────────────────────────┘
```

### 5.3 SemanticCache Interface SPI (pangrova-spi)

New package: `org.pangrova.spi.ai.cache`

```java
package org.pangrova.spi.ai.cache;

import java.util.Optional;

/**
 * SPI for semantic similarity-based query result caching.
 *
 * <p>Implementations maintain a vector index of query embeddings
 * and map similar queries to cached results. The similarity threshold
 * controls the trade-off between cache hit rate and result accuracy.</p>
 *
 * <p>Thread-safe: implementations must handle concurrent get/put.</p>
 */
public interface SemanticCacheProvider {

    /**
     * Searches for a semantically similar cached result.
     *
     * @param queryEmbedding  embedding vector of the incoming query
     * @param tenantId        tenant isolation dimension (nullable)
     * @param principal       user isolation dimension (nullable)
     * @param threshold       cosine similarity threshold in (0, 1]; higher = stricter
     * @return cached result if a similar query exists above threshold; empty otherwise
     */
    Optional<SemanticCacheHit> findSimilar(float[] queryEmbedding,
                                            String tenantId,
                                            String principal,
                                            double threshold);

    /**
     * Stores a query embedding and its associated result for future similarity lookups.
     *
     * @param entry the cache entry to store
     */
    void put(SemanticCacheEntry entry);

    /**
     * Invalidates all entries referencing the given table.
     * Called on DDL/DML that may affect cached results.
     *
     * @param tableName fully qualified table name
     */
    void invalidateByTable(String tableName);

    /**
     * Returns the number of entries currently in the semantic cache.
     */
    long size();
}
```

```java
package org.pangrova.spi.ai.cache;

import java.time.Instant;
import java.util.Set;

/**
 * Immutable entry in the semantic cache.
 *
 * @param queryEmbedding  embedding vector of the original query
 * @param normalizedSql   the original SQL (for debugging/audit)
 * @param tenantId        tenant isolation (empty string if none)
 * @param principal       user isolation (empty string if none)
 * @param resultDigest    SHA-256 of the cached result (pointer to QueryResultCache L2)
 * @param referencedTables tables referenced by this query (for invalidation)
 * @param createdAt       when this entry was created
 */
public record SemanticCacheEntry(
        float[] queryEmbedding,
        String normalizedSql,
        String tenantId,
        String principal,
        String resultDigest,
        Set<String> referencedTables,
        Instant createdAt
) {
    public SemanticCacheEntry {
        if (queryEmbedding == null || queryEmbedding.length == 0) {
            throw new IllegalArgumentException("queryEmbedding must not be empty");
        }
        referencedTables = referencedTables == null ? Set.of() : Set.copyOf(referencedTables);
    }
}
```

```java
package org.pangrova.spi.ai.cache;

/**
 * Result of a semantic cache hit.
 *
 * @param resultDigest    pointer to the actual result in QueryResultCache L2
 * @param similarity      cosine similarity score between query and cached entry
 * @param originalSql     the SQL that produced the cached result (for audit)
 */
public record SemanticCacheHit(
        String resultDigest,
        double similarity,
        String originalSql
) {
    public SemanticCacheHit {
        if (similarity < 0 || similarity > 1) {
            throw new IllegalArgumentException("similarity must be in [0, 1]");
        }
    }
}
```

### 5.4 HNSW-Backed Implementation (pangrova-coordinator)

New package: `org.pangrova.coordinator.ai.cache`

```java
package org.pangrova.coordinator.ai.cache;

import org.pangrova.spi.ai.cache.SemanticCacheEntry;
import org.pangrova.spi.ai.cache.SemanticCacheHit;
import org.pangrova.spi.ai.cache.SemanticCacheProvider;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory semantic cache using brute-force cosine similarity search.
 *
 * <p>For production scale (>100K entries), this should be replaced with
 * an HNSW-backed implementation using {@code HnswEngine} from pangrova-indexing.
 * The brute-force approach is chosen for Phase 1 to avoid a coordinator
 * dependency on pangrova-indexing.</p>
 *
 * <p>Capacity is bounded by {@code maxEntries}. When full, the oldest
 * entry is evicted (FIFO).</p>
 *
 * <p>Tenant/principal isolation: entries are filtered by exact match
 * on tenantId and principal before similarity search. This ensures
 * no cross-tenant result leakage even if embeddings are similar.</p>
 */
@ThreadSafe
public final class InMemorySemanticCache implements SemanticCacheProvider {

    private final List<SemanticCacheEntry> entries;
    private final int maxEntries;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public InMemorySemanticCache(int maxEntries) {
        if (maxEntries < 1) {
            throw new IllegalArgumentException("maxEntries must be positive");
        }
        this.maxEntries = maxEntries;
        this.entries = new ArrayList<>(Math.min(maxEntries, 1024));
    }

    @Override
    public Optional<SemanticCacheHit> findSimilar(float[] queryEmbedding,
                                                    String tenantId,
                                                    String principal,
                                                    double threshold) {
        lock.readLock().lock();
        try {
            SemanticCacheHit bestHit = null;
            double bestSimilarity = threshold;

            for (SemanticCacheEntry entry : entries) {
                if (!matchesIsolation(entry, tenantId, principal)) {
                    continue;
                }
                double similarity = cosineSimilarity(queryEmbedding, entry.queryEmbedding());
                if (similarity > bestSimilarity) {
                    bestSimilarity = similarity;
                    bestHit = new SemanticCacheHit(
                            entry.resultDigest(), similarity, entry.normalizedSql());
                }
            }
            return Optional.ofNullable(bestHit);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void put(SemanticCacheEntry entry) {
        lock.writeLock().lock();
        try {
            if (entries.size() >= maxEntries) {
                entries.removeFirst();
            }
            entries.add(entry);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void invalidateByTable(String tableName) {
        String lowerTable = tableName.toLowerCase();
        lock.writeLock().lock();
        try {
            entries.removeIf(e -> e.referencedTables().stream()
                    .anyMatch(t -> t.toLowerCase().equals(lowerTable)));
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            return entries.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    private static boolean matchesIsolation(SemanticCacheEntry entry,
                                             String tenantId, String principal) {
        String entryTenant = entry.tenantId() != null ? entry.tenantId() : "";
        String entryPrincipal = entry.principal() != null ? entry.principal() : "";
        String queryTenant = tenantId != null ? tenantId : "";
        String queryPrincipal = principal != null ? principal : "";
        return entryTenant.equals(queryTenant) && entryPrincipal.equals(queryPrincipal);
    }

    /**
     * Cosine similarity: dot(a, b) / (||a|| * ||b||).
     * Returns value in [-1, 1]; higher = more similar.
     */
    static double cosineSimilarity(float[] a, float[] b) {
        if (a.length != b.length) {
            return -1.0;
        }
        double dot = 0.0, normA = 0.0, normB = 0.0;
        for (int i = 0; i < a.length; i++) {
            dot += (double) a[i] * b[i];
            normA += (double) a[i] * a[i];
            normB += (double) b[i] * b[i];
        }
        double denom = Math.sqrt(normA) * Math.sqrt(normB);
        return denom == 0 ? 0.0 : dot / denom;
    }
}
```

### 5.5 Similarity Threshold and Eviction

The similarity threshold controls the trade-off between cache hit rate and result accuracy. The default `0.95` is deliberately strict to prevent semantically distinct queries from sharing results.

Eviction policy: FIFO (oldest entry removed when `maxEntries` is exceeded). Entries also carry a `ttlMinutes` expiry — expired entries are skipped on lookup and cleaned lazily.

Both exact-match and semantic invalidation are triggered from the same invalidation point in `DdlExecutor`/`DmlExecutor`:
- `QueryResultCache.invalidateByTable(table)` — clears exact-match cache (existing)
- `SemanticCacheProvider.invalidateByTable(table)` — clears semantic cache entries (new)

### 5.6 Tenant Isolation

Tenant isolation is enforced by exact matching on `(tenantId, principal)` before any similarity search. A cross-tenant query will never produce a cache hit even if embeddings are identical.

### 5.7 Configuration (YAML)

```yaml
pangrova:
  ai:
    semantic-cache:
      enabled: false                    # opt-in, disabled by default
      model: "text-embedding-3-small"   # model from embedding providers
      similarity-threshold: 0.95        # cosine similarity threshold (0.95 = very strict)
      max-entries: 10000                # max cached query embeddings
      ttl-minutes: 60                   # entry TTL (0 = no expiry)
```

### 5.8 Metrics (pangrova-observability)

| Metric | Type | Description |
|--------|------|-------------|
| `pangrova.semantic_cache.hits` | Counter | Semantic cache hits |
| `pangrova.semantic_cache.misses` | Counter | Semantic cache misses (below threshold) |
| `pangrova.semantic_cache.embed_latency_ms` | Histogram | Time to embed query for cache lookup |
| `pangrova.semantic_cache.similarity_score` | Histogram | Distribution of similarity scores on hits |
| `pangrova.semantic_cache.size` | Gauge | Current number of entries |
| `pangrova.semantic_cache.evictions` | Counter | Entries evicted (FIFO or TTL) |

### 5.9 Module Impact

| Module | File | Change |
|--------|------|--------|
| `pangrova-spi` | `SemanticCacheProvider.java` (new) | SPI contract |
| `pangrova-spi` | `SemanticCacheEntry.java` (new) | Cache entry record |
| `pangrova-spi` | `SemanticCacheHit.java` (new) | Cache hit record |
| `pangrova-coordinator` | `InMemorySemanticCache.java` (new) | Brute-force implementation |
| `pangrova-coordinator` | Query execution path | + semantic cache lookup/store |
| `pangrova-coordinator` | DDL/DML executors | + semantic cache invalidation |
| `pangrova-observability` | `SemanticCacheMetrics.java` (new) | 6 metrics |

---

## §6 Retrieval Quality Observability

### 6.1 Problem

Hybrid search (BM25 + vector + RRF fusion) has latency and throughput observability, but no quality metrics. Without quality observability:

- Users cannot tell if their HNSW index has degraded after data updates
- The 3× over-fetch multiplier in `HybridSearchFusionRule` is a guess — no data to validate it
- BM25 scores and vector scores may have wildly different distributions, making RRF fusion ineffective — but nobody would know
- Embedding model drift (new data has different semantic distribution than training data) is invisible

### 6.2 Design: Sampling-Based Quality Estimation

Full brute-force recall computation is O(N) per query — too expensive for production. Instead, statistical sampling is used:

1. For a configurable fraction of queries (default 1%), execute both ANN search and exact brute-force search
2. Compare results to compute Recall@K
3. Record score distributions for anomaly detection

### 6.3 RetrievalQualitySampler SPI (pangrova-spi)

New package: `org.pangrova.spi.ai.quality`

```java
package org.pangrova.spi.ai.quality;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of retrieval quality metrics for a single sampled query.
 *
 * @param queryId           the query that was sampled
 * @param tableName         target table
 * @param retrievalType     VECTOR, TEXT, or HYBRID
 * @param recallAtK         fraction of true top-K results found by ANN (0.0 to 1.0)
 * @param topK              the K value used
 * @param annResultCount    number of results returned by ANN path
 * @param exactResultCount  number of results from brute-force (ground truth)
 * @param scoreDistribution ordered list of scores from the ANN path (descending)
 * @param scoreSpread       max_score - min_score in the result set (0 = all same score)
 * @param sampledAt         when this sample was taken
 */
public record RetrievalQualitySnapshot(
        String queryId,
        String tableName,
        RetrievalType retrievalType,
        double recallAtK,
        int topK,
        int annResultCount,
        int exactResultCount,
        List<Double> scoreDistribution,
        double scoreSpread,
        Instant sampledAt
) {
    public enum RetrievalType { VECTOR, TEXT, HYBRID }

    public RetrievalQualitySnapshot {
        scoreDistribution = scoreDistribution == null
                ? List.of() : List.copyOf(scoreDistribution);
        if (recallAtK < 0 || recallAtK > 1) {
            throw new IllegalArgumentException("recallAtK must be in [0, 1]");
        }
    }

    /** Returns true if recall has degraded below the acceptable threshold. */
    public boolean isRecallDegraded(double threshold) {
        return recallAtK < threshold;
    }

    /** Returns true if all scores are nearly identical (poor discrimination). */
    public boolean isPoorDiscrimination(double minSpread) {
        return scoreSpread < minSpread;
    }
}
```

```java
package org.pangrova.spi.ai.quality;

/**
 * SPI for collecting and querying retrieval quality snapshots.
 *
 * <p>Implementations store snapshots in a ring buffer (bounded memory)
 * and expose aggregated statistics for monitoring dashboards.</p>
 */
public interface RetrievalQualityCollector {

    /** Records a quality snapshot from a sampled query. */
    void record(RetrievalQualitySnapshot snapshot);

    /** Returns the average Recall@K over the last N snapshots for a table. */
    double averageRecall(String tableName, int lastN);

    /** Returns the average score spread over the last N snapshots for a table. */
    double averageScoreSpread(String tableName, int lastN);

    /** Returns the total number of snapshots collected. */
    long snapshotCount();
}
```

### 6.4 Recall@K, NDCG, and MRR Metrics

Quality observability exposes three standard IR metrics:

| Metric | Description | Formula |
|--------|-------------|---------|
| **Recall@K** | Fraction of true top-K results found by ANN | `|ANN ∩ BruteForce| / K` |
| **NDCG** | Normalized Discounted Cumulative Gain — measures result ordering quality | `DCG / IDCG` |
| **MRR** | Mean Reciprocal Rank — measures first-relevant-result position | `1/rank(first relevant result)` |

**RecallEstimator** (new class in `org.pangrova.indexing.eval`):

```java
package org.pangrova.indexing.eval;

import org.pangrova.spi.index.vector.VectorSearchResult;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Estimates Recall@K by comparing ANN results against brute-force ground truth.
 *
 * <p>Thread-safe: stateless, all state in method parameters.</p>
 */
public final class RecallEstimator {

    private RecallEstimator() {}

    /**
     * Computes Recall@K.
     *
     * @param annResults        results from ANN search
     * @param bruteForceResults results from exact brute-force search
     * @param topK              the K value
     * @return recall in [0.0, 1.0]
     */
    public static double computeRecall(
            List<VectorSearchResult.ScoredRow> annResults,
            List<VectorSearchResult.ScoredRow> bruteForceResults,
            int topK) {
        if (topK <= 0 || bruteForceResults.isEmpty()) {
            return 0.0;
        }
        Set<Long> groundTruth = new HashSet<>();
        for (int i = 0; i < Math.min(topK, bruteForceResults.size()); i++) {
            groundTruth.add(bruteForceResults.get(i).rowOffset());
        }
        long hits = 0;
        for (int i = 0; i < Math.min(topK, annResults.size()); i++) {
            if (groundTruth.contains(annResults.get(i).rowOffset())) {
                hits++;
            }
        }
        return (double) hits / Math.min(topK, groundTruth.size());
    }

    /**
     * Computes score spread (max - min) from a scored result list.
     *
     * @param results scored results (any order)
     * @return spread value; 0.0 if results is empty or single element
     */
    public static double computeScoreSpread(List<VectorSearchResult.ScoredRow> results) {
        if (results.size() <= 1) {
            return 0.0;
        }
        double min = Double.MAX_VALUE, max = Double.MIN_VALUE;
        for (VectorSearchResult.ScoredRow row : results) {
            if (row.distance() < min) min = row.distance();
            if (row.distance() > max) max = row.distance();
        }
        return max - min;
    }
}
```

### 6.5 SQL Exposure

```sql
-- System table for quality monitoring
SELECT * FROM system.retrieval_quality
WHERE table_name = 'catalog.schema.docs'
ORDER BY sampled_at DESC
LIMIT 20;

-- Aggregated view
SELECT table_name,
       AVG(recall_at_k) AS avg_recall,
       AVG(score_spread) AS avg_spread,
       COUNT(*) AS sample_count
FROM system.retrieval_quality
WHERE sampled_at > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY table_name;
```

### 6.6 Alerting Thresholds

| Metric | Warning | Critical | Action |
|--------|---------|----------|--------|
| Recall@K | < 0.90 | < 0.80 | Recommend index rebuild |
| Score Spread | < 0.01 | < 0.001 | Warn: poor embedding quality or wrong distance metric |
| Score Distribution Skew | > 0.8 Gini | > 0.9 Gini | Warn: fusion may be dominated by one signal |

### 6.7 Metrics (pangrova-observability)

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pangrova.retrieval.recall` | Histogram | table, type | Recall@K distribution |
| `pangrova.retrieval.score_spread` | Histogram | table, type | Score spread distribution |
| `pangrova.retrieval.samples` | Counter | table, type | Total quality samples taken |
| `pangrova.retrieval.degraded` | Counter | table, type | Queries where recall < warning threshold |

### 6.8 Configuration

```yaml
pangrova:
  ai:
    retrieval-quality:
      enabled: true
      sample-rate: 0.01              # 1% of vector/hybrid queries
      max-snapshots: 10000           # ring buffer size
      recall-warning-threshold: 0.90
      recall-critical-threshold: 0.80
      spread-warning-threshold: 0.01
```

### 6.9 Module Impact

| Module | File | Change |
|--------|------|--------|
| `pangrova-spi` | `RetrievalQualitySnapshot.java` (new) | Quality snapshot record |
| `pangrova-spi` | `RetrievalQualityCollector.java` (new) | Collector SPI |
| `pangrova-indexing` | `RecallEstimator.java` (new) | Recall@K computation |
| `pangrova-coordinator` | `RingBufferQualityCollector.java` (new) | Ring buffer implementation |
| `pangrova-coordinator` | Vector/hybrid search dispatch | + sampling logic |
| `pangrova-observability` | `RetrievalQualityMetrics.java` (new) | 4 metrics |

---

## §7 Vector Index Lifecycle

### 7.1 Problem

Vector indexes degrade silently. After data updates:
- HNSW graph connections become suboptimal (new vectors inserted at leaf level only)
- IVFPQ centroids drift from the actual data distribution
- Recall drops but nobody notices until users complain about search quality

Currently, `VectorIndexData` stores `vectorCount` but has no staleness metadata. There is no mechanism to detect when an index needs rebuilding.

### 7.2 Design: Health Report + Advisor

```
┌─────────────────────────────────────────────────────┐
│ VectorIndexHealthMonitor (pangrova-coordinator)        │
│                                                      │
│ Periodically (or on-demand via SQL):                 │
│ 1. Read VectorIndexData.vectorCount                  │
│ 2. Read current table row count from statistics      │
│ 3. Compute staleness = |indexed - current| / current │
│ 4. Read Recall@K from RetrievalQualityCollector      │
│ 5. Generate VectorIndexHealthReport                  │
│ 6. If degraded → emit advisory via structured log    │
└─────────────────────────────────────────────────────┘
```

### 7.3 VectorIndexHealthAssessor SPI (pangrova-spi)

New file in `org.pangrova.spi.ai.index`:

```java
package org.pangrova.spi.ai.index;

import java.time.Instant;

/**
 * Health report for a single vector index.
 *
 * @param indexId          index identifier
 * @param tableName        table the index belongs to
 * @param columnName       indexed column
 * @param algorithmType    HNSW or IVFPQ
 * @param indexedVectorCount vectors in the index at build time
 * @param currentRowCount  current table row count (from statistics)
 * @param stalenessRatio   |indexed - current| / max(current, 1) — 0.0 = fresh, 1.0 = completely stale
 * @param estimatedRecall  average Recall@K from quality sampling (NaN if no samples)
 * @param health           computed health status
 * @param recommendation   human-readable recommendation
 * @param assessedAt       when this report was generated
 */
public record VectorIndexHealthReport(
        String indexId,
        String tableName,
        String columnName,
        String algorithmType,
        long indexedVectorCount,
        long currentRowCount,
        double stalenessRatio,
        double estimatedRecall,
        HealthStatus health,
        String recommendation,
        Instant assessedAt
) {
    public enum HealthStatus { HEALTHY, WARNING, CRITICAL, UNKNOWN }

    public VectorIndexHealthReport {
        if (stalenessRatio < 0) {
            throw new IllegalArgumentException("stalenessRatio must be >= 0");
        }
    }
}
```

### 7.4 Staleness Detection — VectorIndexHealthAssessor (pangrova-coordinator)

New class in `org.pangrova.coordinator.ai.index`:

```java
package org.pangrova.coordinator.ai.index;

import org.pangrova.spi.ai.index.VectorIndexHealthReport;
import org.pangrova.spi.ai.index.VectorIndexHealthReport.HealthStatus;
import org.pangrova.spi.ai.quality.RetrievalQualityCollector;

import java.time.Instant;

/**
 * Assesses vector index health and generates recommendations.
 *
 * <p>Assessment rules:</p>
 * <ul>
 *   <li>Staleness > 30% OR recall < 0.80 → CRITICAL: "Rebuild index"</li>
 *   <li>Staleness > 10% OR recall < 0.90 → WARNING: "Consider rebuilding"</li>
 *   <li>HNSW with > 10M vectors → WARNING: "Consider switching to IVFPQ"</li>
 *   <li>IVFPQ with < 100K vectors → WARNING: "HNSW may be more efficient"</li>
 *   <li>Dimension > 1536 → INFO: "Consider dimension reduction"</li>
 * </ul>
 *
 * <p>Thread-safe: stateless assessor, all state in parameters.</p>
 */
public final class VectorIndexHealthAssessor {

    static final double STALENESS_WARNING = 0.10;
    static final double STALENESS_CRITICAL = 0.30;
    static final double RECALL_WARNING = 0.90;
    static final double RECALL_CRITICAL = 0.80;
    static final long HNSW_SCALE_THRESHOLD = 10_000_000L;
    static final long IVFPQ_MIN_THRESHOLD = 100_000L;

    private VectorIndexHealthAssessor() {}

    /**
     * Generates a health report for a vector index.
     *
     * @param indexId          index identifier
     * @param tableName        table name
     * @param columnName       column name
     * @param algorithmType    "HNSW" or "IVFPQ"
     * @param indexedCount     vectors in the index
     * @param currentRowCount  current table row count
     * @param dimension        vector dimension
     * @param qualityCollector retrieval quality collector (nullable)
     * @return health report with status and recommendation
     */
    public static VectorIndexHealthReport assess(
            String indexId, String tableName, String columnName,
            String algorithmType, long indexedCount, long currentRowCount,
            int dimension, RetrievalQualityCollector qualityCollector) {

        double staleness = currentRowCount > 0
                ? Math.abs(indexedCount - currentRowCount) / (double) currentRowCount
                : 0.0;

        double recall = Double.NaN;
        if (qualityCollector != null) {
            recall = qualityCollector.averageRecall(tableName, 100);
        }

        HealthStatus status;
        String recommendation;

        if (staleness > STALENESS_CRITICAL || (!Double.isNaN(recall) && recall < RECALL_CRITICAL)) {
            status = HealthStatus.CRITICAL;
            recommendation = "Rebuild index: staleness=" + formatPct(staleness)
                    + ", recall=" + formatRecall(recall)
                    + ". Run: ALTER INDEX " + indexId + " REBUILD;";
        } else if (staleness > STALENESS_WARNING || (!Double.isNaN(recall) && recall < RECALL_WARNING)) {
            status = HealthStatus.WARNING;
            recommendation = "Consider rebuilding: staleness=" + formatPct(staleness)
                    + ", recall=" + formatRecall(recall);
        } else if ("HNSW".equals(algorithmType) && currentRowCount > HNSW_SCALE_THRESHOLD) {
            status = HealthStatus.WARNING;
            recommendation = "Table has " + currentRowCount + " rows. "
                    + "Consider switching to IVFPQ for better memory efficiency at scale.";
        } else if ("IVFPQ".equals(algorithmType) && currentRowCount < IVFPQ_MIN_THRESHOLD) {
            status = HealthStatus.WARNING;
            recommendation = "Table has only " + currentRowCount + " rows. "
                    + "HNSW may provide better recall at this scale.";
        } else {
            status = HealthStatus.HEALTHY;
            recommendation = "Index is healthy. Staleness=" + formatPct(staleness)
                    + ", recall=" + formatRecall(recall);
        }

        return new VectorIndexHealthReport(
                indexId, tableName, columnName, algorithmType,
                indexedCount, currentRowCount, staleness, recall,
                status, recommendation, Instant.now());
    }

    private static String formatPct(double v) {
        return String.format("%.1f%%", v * 100);
    }

    private static String formatRecall(double v) {
        return Double.isNaN(v) ? "N/A" : String.format("%.3f", v);
    }
}
```

### 7.5 IndexRebuildScheduler and SQL Exposure

```sql
-- Check health of all vector indexes
SELECT * FROM system.vector_index_health;

-- Output example:
-- index_id | table_name | column | algorithm | indexed_count | current_count | staleness | recall | health | recommendation
-- idx_emb  | docs       | emb    | HNSW      | 50000         | 72000         | 30.6%     | 0.823  | CRITICAL | Rebuild index...

-- Rebuild a degraded index
ALTER INDEX idx_emb REBUILD;

-- Show indexes needing attention
SELECT * FROM system.vector_index_health WHERE health IN ('WARNING', 'CRITICAL');
```

### 7.6 Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pangrova.vector_index.staleness` | Gauge | index_id, table | Current staleness ratio |
| `pangrova.vector_index.health_checks` | Counter | status | Health check results by status |
| `pangrova.vector_index.rebuilds` | Counter | table, algorithm | Index rebuild count |

### 7.7 Configuration

```yaml
pangrova:
  ai:
    vector-index-health:
      check-interval-minutes: 30
      staleness-warning: 0.10
      staleness-critical: 0.30
```

### 7.8 Module Impact

| Module | File | Change |
|--------|------|--------|
| `pangrova-spi` | `VectorIndexHealthReport.java` (new) | Health report record |
| `pangrova-coordinator` | `VectorIndexHealthAssessor.java` (new) | Assessment logic |
| `pangrova-coordinator` | System table registration | + `system.vector_index_health` |
| `pangrova-observability` | `VectorIndexMetrics.java` (new) | 3 metrics |

---

## §8 AI-Driven Optimization

### 8.1 CardinalityFeedbackStore

#### 8.1.1 Problem

Pangrova's `StatisticsBasedCostEstimator` uses NDV-based selectivity estimation with a confidence threshold (`MIN_CONFIDENCE_THRESHOLD = 0.6`). When statistics are stale (confidence < 0.6), it falls back to `DEFAULT_SELECTIVITY = 0.5` and `DEFAULT_ROW_COUNT = 1_000_000`.

In Lakehouse scenarios, statistics are inherently stale — Iceberg/Paimon stats depend on compaction cycles (hours to days). When confidence drops below 0.6, the CBO is essentially blind. The result: bad JOIN ordering, wrong scan strategies, unnecessary shuffles.

#### 8.1.2 Design: Query Execution Feedback Loop

Instead of a heavyweight ML model, a lightweight feedback loop is used:

```
1. BEFORE execution: Planner estimates cardinality
   → estimatedRows = StatisticsBasedCostEstimator.estimate()

2. AFTER execution: Coordinator collects actual rows
   → actualRows from TaskStatusReport

3. FEEDBACK: Store (tableName, predicate_hash, estimated, actual) in CardinalityFeedbackStore

4. NEXT QUERY: If feedback exists for same predicate pattern,
   use actual_rows as estimate (with decay)
   → Overrides stale statistics with real execution data
```

#### 8.1.3 Feedback Store (pangrova-planner)

New package: `org.pangrova.planner.feedback`

```java
package org.pangrova.planner.feedback;

import java.time.Instant;

/**
 * A single cardinality feedback record from a completed query.
 *
 * @param tableName       fully qualified table name
 * @param predicateHash   hash of the normalized predicate pattern (not values)
 * @param estimatedRows   what the planner predicted
 * @param actualRows      what the execution actually produced
 * @param qError          max(estimated/actual, actual/estimated) — 1.0 = perfect
 * @param recordedAt      when this feedback was recorded
 */
public record CardinalityFeedback(
        String tableName,
        String predicateHash,
        double estimatedRows,
        double actualRows,
        double qError,
        Instant recordedAt
) {
    public CardinalityFeedback {
        if (estimatedRows < 0 || actualRows < 0) {
            throw new IllegalArgumentException("row counts must be non-negative");
        }
    }

    /** Computes q-error: the standard metric for cardinality estimation quality. */
    public static double computeQError(double estimated, double actual) {
        if (estimated <= 0 || actual <= 0) return Double.MAX_VALUE;
        return Math.max(estimated / actual, actual / estimated);
    }
}
```

### 8.2 EMA Learned Estimation

```java
package org.pangrova.planner.feedback;

import org.pangrova.common.concurrent.ThreadSafe;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores cardinality feedback from completed queries.
 *
 * <p>Key = (tableName, predicateHash). Value = exponential moving average
 * of actual row counts. This provides a "learned" cardinality estimate
 * that improves with each query execution.</p>
 *
 * <p>Decay: entries older than {@code maxAge} are ignored. This prevents
 * stale feedback from persisting after data changes.</p>
 *
 * <p>Thread-safe: uses ConcurrentHashMap for lock-free reads.</p>
 */
@ThreadSafe
public final class CardinalityFeedbackStore {

    /** Exponential moving average smoothing factor. 0.3 = recent queries weighted 30%. */
    static final double EMA_ALPHA = 0.3;

    private final ConcurrentHashMap<String, FeedbackEntry> store = new ConcurrentHashMap<>();
    private final Duration maxAge;

    public CardinalityFeedbackStore(Duration maxAge) {
        this.maxAge = maxAge;
    }

    /**
     * Records feedback from a completed query.
     * Updates the EMA of actual row counts for the (table, predicate) pair.
     */
    public void recordFeedback(CardinalityFeedback feedback) {
        String key = feedbackKey(feedback.tableName(), feedback.predicateHash());
        store.compute(key, (k, existing) -> {
            if (existing == null) {
                return new FeedbackEntry(feedback.actualRows(), 1, Instant.now());
            }
            double newEma = EMA_ALPHA * feedback.actualRows()
                    + (1 - EMA_ALPHA) * existing.emaActualRows();
            return new FeedbackEntry(newEma, existing.sampleCount() + 1, Instant.now());
        });
    }

    /**
     * Returns the learned cardinality estimate for a (table, predicate) pair.
     * Returns empty if no feedback exists or feedback is too old.
     */
    public Optional<Double> getLearnedEstimate(String tableName, String predicateHash) {
        String key = feedbackKey(tableName, predicateHash);
        FeedbackEntry entry = store.get(key);
        if (entry == null) {
            return Optional.empty();
        }
        if (Duration.between(entry.lastUpdated(), Instant.now()).compareTo(maxAge) > 0) {
            store.remove(key);
            return Optional.empty();
        }
        return Optional.of(entry.emaActualRows());
    }

    /** Returns the number of feedback entries currently stored. */
    public long size() {
        return store.size();
    }

    private static String feedbackKey(String tableName, String predicateHash) {
        return tableName + "|" + predicateHash;
    }

    /**
     * Internal feedback entry with EMA state.
     *
     * @param emaActualRows exponential moving average of actual row counts
     * @param sampleCount   number of feedback samples incorporated
     * @param lastUpdated   when this entry was last updated
     */
    record FeedbackEntry(double emaActualRows, long sampleCount, Instant lastUpdated) {}
}
```

The feedback store is injected into `StatisticsBasedCostEstimator`:

```java
// In StatisticsBasedCostEstimator, modified estimateRowCount():
public double estimateRowCount(String tableName, String predicateHash) {
    // Priority 1: Learned estimate from feedback (if available and fresh)
    if (feedbackStore != null && predicateHash != null) {
        Optional<Double> learned = feedbackStore.getLearnedEstimate(tableName, predicateHash);
        if (learned.isPresent()) {
            return learned.get();
        }
    }
    // Priority 2: Confident statistics (existing logic)
    return firstConfidentColumnRowCount(tableName).orElse(DEFAULT_ROW_COUNT);
}
```

### 8.3 Predicate Pattern Hashing

To match similar predicates across queries, the predicate structure (operators + column names) is hashed but not the literal values:

```
WHERE price > 100 AND category = 'electronics'
→ hash("price > ? AND category = ?") = "a3f2..."

WHERE price > 200 AND category = 'clothing'
→ hash("price > ? AND category = ?") = "a3f2..."  (same hash!)
```

```java
package org.pangrova.planner.feedback;

/**
 * Normalizes Calcite RexNode predicates into hashable patterns.
 *
 * <p>Replaces all literal values with '?' placeholders, preserving
 * the predicate structure (operators, column references, AND/OR tree).
 * Two queries with the same predicate structure but different literal
 * values produce the same hash.</p>
 *
 * <p>Thread-safe: stateless utility.</p>
 */
public final class PredicatePatternHasher {

    private PredicatePatternHasher() {}

    /**
     * Normalizes a predicate string and returns its SHA-256 hash.
     *
     * @param predicateString string representation of the predicate
     *                        (from RexNode.toString() after literal replacement)
     * @return SHA-256 hex digest of the normalized pattern
     */
    public static String hash(String predicateString) {
        String normalized = predicateString
                .replaceAll("'[^']*'", "?")           // string literals
                .replaceAll("\\b\\d+(\\.\\d+)?\\b", "?")  // numeric literals
                .replaceAll("\\s+", " ")
                .strip()
                .toLowerCase();
        return org.pangrova.common.util.DigestUtils.sha256Hex(normalized);
    }
}
```

### 8.4 DataLayoutAdvisor

#### 8.4.1 Problem

Lakehouse performance is dominated by data layout: partition strategy, sort order, file size, and compaction timing. Most users use default partitioning (by date) even when their queries filter by other columns. Sort order is rarely set, missing opportunities for predicate pushdown.

#### 8.4.2 Design

```
QueryPatternRecorder (pangrova-planner)
 - For each query, extract: tables, WHERE columns (+ operator type), JOIN columns,
   ORDER BY columns, GROUP BY columns
 - Store in ring buffer (bounded memory)
          │
          ▼
DataLayoutAdvisor (pangrova-coordinator)
 - Aggregate column usage per table
 - Rank columns by filter/join/sort frequency
 - Generate recommendations:
     Partition key: most frequent equality filter column
     Sort key: most frequent range filter / ORDER BY column
     Compaction: based on write frequency vs read frequency
 - Expose via system.layout_recommendations
```

#### 8.4.3 LayoutRecommendation SPI (pangrova-spi)

New file in `org.pangrova.spi.ai.layout`:

```java
package org.pangrova.spi.ai.layout;

import java.time.Instant;
import java.util.List;

/**
 * A data layout recommendation for a single table.
 *
 * @param tableName              fully qualified table name
 * @param recommendedPartitionKeys columns recommended for partitioning (by frequency)
 * @param recommendedSortKeys    columns recommended for sort order (by range filter frequency)
 * @param recommendedCompaction  compaction strategy recommendation
 * @param confidence             confidence in [0, 1] based on sample size
 * @param queryCount             number of queries analyzed to produce this recommendation
 * @param generatedAt            when this recommendation was generated
 */
public record LayoutRecommendation(
        String tableName,
        List<ColumnRecommendation> recommendedPartitionKeys,
        List<ColumnRecommendation> recommendedSortKeys,
        CompactionRecommendation recommendedCompaction,
        double confidence,
        long queryCount,
        Instant generatedAt
) {
    public LayoutRecommendation {
        recommendedPartitionKeys = List.copyOf(recommendedPartitionKeys);
        recommendedSortKeys = List.copyOf(recommendedSortKeys);
    }

    /**
     * @param columnName  column name
     * @param frequency   fraction of queries that use this column in the relevant context
     * @param operatorType most common operator (EQUALS, RANGE, IN, LIKE)
     */
    public record ColumnRecommendation(
            String columnName,
            double frequency,
            String operatorType
    ) {}

    /**
     * @param strategy    AGGRESSIVE (high write rate), BALANCED, CONSERVATIVE (low write rate)
     * @param reason      human-readable explanation
     * @param writeRate   observed writes per hour
     * @param readRate    observed reads per hour
     */
    public record CompactionRecommendation(
            String strategy,
            String reason,
            double writeRate,
            double readRate
    ) {}
}
```

#### 8.4.4 DataLayoutAdvisor (pangrova-coordinator)

New class in `org.pangrova.coordinator.ai.layout`:

```java
package org.pangrova.coordinator.ai.layout;

/**
 * Generates data layout recommendations from recorded query patterns.
 *
 * <p>Algorithm:</p>
 * <ol>
 *   <li>Aggregate column usage across all queries for a table</li>
 *   <li>Partition key: column with highest EQUALS filter frequency</li>
 *   <li>Sort key: column with highest RANGE filter or ORDER BY frequency</li>
 *   <li>Compaction: based on write/read ratio</li>
 * </ol>
 *
 * <p>Minimum sample size: 50 queries. Below this, confidence is too low.</p>
 */
public final class DataLayoutAdvisor {

    static final int MIN_SAMPLE_SIZE = 50;

    private final QueryPatternRecorder recorder;

    public DataLayoutAdvisor(QueryPatternRecorder recorder) {
        this.recorder = recorder;
    }

    /**
     * Generates a layout recommendation for a table.
     *
     * @param tableName fully qualified table name
     * @return recommendation, or empty if insufficient data
     */
    public java.util.Optional<LayoutRecommendation> recommend(String tableName) { ... }
}
```

#### 8.4.5 SQL Exposure

```sql
-- Get layout recommendations for a table
SELECT * FROM system.layout_recommendations
WHERE table_name = 'catalog.schema.products';

-- Output:
-- table_name | partition_keys | sort_keys | compaction | confidence | query_count
-- products   | [category:82%] | [price:65%, created_at:41%] | AGGRESSIVE | 0.87 | 435

-- Apply recommendation (manual — advisor suggests, user decides)
ALTER TABLE catalog.schema.products
SET PROPERTIES ('partition_key' = 'category', 'sort_key' = 'price,created_at');
```

### 8.5 Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pangrova.cardinality.q_error` | Histogram | table | Q-error distribution (1.0 = perfect) |
| `pangrova.cardinality.feedback_entries` | Gauge | — | Number of feedback entries in store |
| `pangrova.cardinality.feedback_hits` | Counter | — | Times feedback was used instead of statistics |
| `pangrova.cardinality.feedback_misses` | Counter | — | Times feedback was unavailable |

### 8.6 Configuration

```yaml
pangrova:
  planner:
    cardinality-feedback:
      enabled: true
      max-age-hours: 24              # feedback entries expire after 24h
      ema-alpha: 0.3                 # EMA smoothing factor
      max-entries: 100000            # max feedback entries in memory
    query-pattern-recorder:
      enabled: true
      max-patterns: 100000           # ring buffer size
    layout-advisor:
      min-sample-size: 50            # minimum queries before generating recommendation
```

### 8.7 Module Impact

| Module | File | Change |
|--------|------|--------|
| `pangrova-spi` | `LayoutRecommendation.java` (new) | Recommendation record |
| `pangrova-planner` | `CardinalityFeedback.java` (new) | Feedback record |
| `pangrova-planner` | `CardinalityFeedbackStore.java` (new) | EMA-based feedback store |
| `pangrova-planner` | `PredicatePatternHasher.java` (new) | Predicate normalization |
| `pangrova-planner` | `StatisticsBasedCostEstimator.java` | + feedback store integration |
| `pangrova-planner` | `QueryPatternRecorder.java` (new) | Pattern recording |
| `pangrova-planner` | Planning pipeline | + pattern extraction from RelNode |
| `pangrova-coordinator` | `TaskStatusReceiver.java` | + cardinality feedback collection |
| `pangrova-coordinator` | `DataLayoutAdvisor.java` (new) | Recommendation generation |
| `pangrova-coordinator` | System table registration | + `system.layout_recommendations` |
| `pangrova-observability` | `CardinalityFeedbackMetrics.java` (new) | 4 metrics |

---

## §9 Conversation Context

### 9.1 Problem

AI agents interact with databases in multi-turn conversations, not single queries. Current Pangrova treats each SQL as an independent request. This means:

- Agent asks "Show me the top products" → gets results
- Agent asks "Filter those by price < 100" → Pangrova has no idea what "those" refers to
- Agent must re-generate the full SQL from scratch, increasing latency and error rate

### 9.2 Design: Session-Scoped Conversation Memory

```
┌─────────────────────────────────────────────────────────────┐
│ ConversationManager (pangrova-coordinator)                     │
│                                                              │
│ Per session (keyed by SessionContext):                        │
│ - Last N query results (configurable, default 5)             │
│ - Last N SQL statements                                      │
│ - Extracted table/column context                             │
│                                                              │
│ SQL extensions:                                               │
│ - $LAST_RESULT → reference to the last query's result set    │
│ - $RESULT(n) → reference to the n-th previous result         │
│                                                              │
│ Implementation:                                               │
│ - $LAST_RESULT is rewritten to a CTE wrapping the last SQL   │
│ - Memory is bounded and session-scoped (no cross-session)    │
└─────────────────────────────────────────────────────────────┘
```

### 9.3 ConversationContext SPI (pangrova-spi)

New file in `org.pangrova.spi.ai.conversation`:

```java
package org.pangrova.spi.ai.conversation;

import java.time.Instant;
import java.util.List;

/**
 * Immutable snapshot of a conversation turn (one query + its result summary).
 *
 * @param turnIndex     0-based index within the session
 * @param sql           the SQL statement executed
 * @param tableName     primary table accessed (nullable for multi-table queries)
 * @param columnNames   columns in the result set
 * @param rowCount      number of rows returned
 * @param executedAt    when this turn was executed
 */
public record ConversationTurn(
        int turnIndex,
        String sql,
        String tableName,
        List<String> columnNames,
        long rowCount,
        Instant executedAt
) {
    public ConversationTurn {
        columnNames = columnNames == null ? List.of() : List.copyOf(columnNames);
    }
}
```

```java
package org.pangrova.spi.ai.conversation;

import java.util.List;
import java.util.Optional;

/**
 * SPI for managing conversation context within a session.
 *
 * <p>Implementations maintain a bounded history of query turns
 * per session. The history is used to resolve $LAST_RESULT and
 * $RESULT(n) references in SQL.</p>
 *
 * <p>Thread-safe: a single session may have concurrent queries
 * (though this is unusual for agent interactions).</p>
 */
public interface ConversationContext {

    /**
     * Records a completed query turn in the conversation history.
     *
     * @param sessionId unique session identifier
     * @param turn      the conversation turn to record
     */
    void recordTurn(String sessionId, ConversationTurn turn);

    /**
     * Returns the last N turns for a session.
     *
     * @param sessionId unique session identifier
     * @param lastN     number of recent turns to return
     * @return turns in chronological order (oldest first), size ≤ lastN
     */
    List<ConversationTurn> getRecentTurns(String sessionId, int lastN);

    /**
     * Returns a specific previous turn by index (0 = most recent).
     *
     * @param sessionId unique session identifier
     * @param offset    0 = last query, 1 = second-to-last, etc.
     * @return the turn, or empty if offset exceeds history
     */
    Optional<ConversationTurn> getTurn(String sessionId, int offset);

    /**
     * Clears conversation history for a session.
     * Called on session close or explicit RESET CONVERSATION.
     */
    void clearSession(String sessionId);
}
```

### 9.4 ConversationManager (pangrova-coordinator)

New class in `org.pangrova.coordinator.ai.conversation`:

```java
package org.pangrova.coordinator.ai.conversation;

import org.pangrova.spi.ai.conversation.ConversationContext;
import org.pangrova.spi.ai.conversation.ConversationTurn;
import org.pangrova.common.concurrent.ThreadSafe;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory conversation context manager.
 *
 * <p>Maintains a bounded deque of conversation turns per session.
 * Sessions are evicted after {@code sessionTtlMs} of inactivity
 * (checked lazily on access).</p>
 *
 * <p>Memory bound: maxTurnsPerSession × maxSessions × ~1KB per turn.</p>
 */
@ThreadSafe
public final class InMemoryConversationManager implements ConversationContext {

    private final ConcurrentHashMap<String, SessionHistory> sessions = new ConcurrentHashMap<>();
    private final int maxTurnsPerSession;

    public InMemoryConversationManager(int maxTurnsPerSession) {
        this.maxTurnsPerSession = maxTurnsPerSession;
    }

    @Override
    public void recordTurn(String sessionId, ConversationTurn turn) {
        sessions.compute(sessionId, (k, existing) -> {
            SessionHistory history = existing != null ? existing : new SessionHistory(maxTurnsPerSession);
            history.addTurn(turn);
            return history;
        });
    }

    @Override
    public List<ConversationTurn> getRecentTurns(String sessionId, int lastN) {
        SessionHistory history = sessions.get(sessionId);
        if (history == null) {
            return List.of();
        }
        return history.getRecent(lastN);
    }

    @Override
    public Optional<ConversationTurn> getTurn(String sessionId, int offset) {
        SessionHistory history = sessions.get(sessionId);
        if (history == null) {
            return Optional.empty();
        }
        return history.getTurn(offset);
    }

    @Override
    public void clearSession(String sessionId) {
        sessions.remove(sessionId);
    }

    /** Returns the number of active sessions. */
    public long activeSessionCount() {
        return sessions.size();
    }

    private static final class SessionHistory {
        private final Deque<ConversationTurn> turns;
        private final int maxTurns;

        SessionHistory(int maxTurns) {
            this.maxTurns = maxTurns;
            this.turns = new ArrayDeque<>(maxTurns);
        }

        void addTurn(ConversationTurn turn) {
            if (turns.size() >= maxTurns) {
                turns.removeFirst();
            }
            turns.addLast(turn);
        }

        List<ConversationTurn> getRecent(int lastN) {
            List<ConversationTurn> result = new ArrayList<>();
            var it = turns.descendingIterator();
            int count = 0;
            while (it.hasNext() && count < lastN) {
                result.addFirst(it.next());
                count++;
            }
            return List.copyOf(result);
        }

        Optional<ConversationTurn> getTurn(int offset) {
            if (offset < 0 || offset >= turns.size()) {
                return Optional.empty();
            }
            var it = turns.descendingIterator();
            for (int i = 0; i < offset; i++) {
                if (!it.hasNext()) return Optional.empty();
                it.next();
            }
            return it.hasNext() ? Optional.of(it.next()) : Optional.empty();
        }
    }
}
```

### 9.5 $LAST_RESULT and $PREVIOUS_QUERY Markers

Grammar additions to `PangrovaSql.g4`:

```antlr
// Add to tableRef rule:
tableRef
    : qualifiedName (FOR (SYSTEM_TIME | SYSTEM_VERSION) AS OF expression)? (AS? alias)?
    | DOLLAR_LAST_RESULT (AS? alias)?                    // $LAST_RESULT
    | DOLLAR_RESULT LPAREN INTEGER_LITERAL RPAREN (AS? alias)?  // $RESULT(n)
    ;

// New tokens:
DOLLAR_LAST_RESULT : '$LAST_RESULT';
DOLLAR_RESULT      : '$RESULT';
```

### 9.6 ConversationContextRewriter (pangrova-planner)

```java
package org.pangrova.planner.conversation;

/**
 * Rewrites $LAST_RESULT and $RESULT(n) references into CTEs
 * using the conversation history from ConversationContext.
 *
 * <p>Called during the planning phase, after parsing and before
 * Calcite optimization. If the referenced turn does not exist,
 * throws a user-facing error: "No previous query result at offset N".</p>
 */
public final class ConversationRewriter {

    private final ConversationContext conversationContext;

    public ConversationRewriter(ConversationContext conversationContext) {
        this.conversationContext = conversationContext;
    }

    /**
     * Rewrites conversation references in the AST.
     *
     * @param statement  parsed AST
     * @param sessionId  current session ID
     * @return rewritten AST with CTEs replacing $LAST_RESULT/$RESULT(n)
     * @throws ConversationReferenceException if referenced turn does not exist
     */
    public Statement rewrite(Statement statement, String sessionId) { ... }
}
```

**Rewrite example**:

```sql
-- User query:
SELECT * FROM $LAST_RESULT WHERE price < 100;

-- Rewritten to (assuming last query was "SELECT * FROM products WHERE category = 'electronics'"):
WITH __last_result AS (
    SELECT * FROM products WHERE category = 'electronics'
)
SELECT * FROM __last_result WHERE price < 100;
```

### 9.7 SQL Examples

```sql
-- Turn 1: Explore data
SELECT * FROM products WHERE category = 'electronics' ORDER BY price DESC LIMIT 20;

-- Turn 2: Filter previous results (agent says "show me the cheap ones")
SELECT * FROM $LAST_RESULT WHERE price < 100;

-- Turn 3: Aggregate previous results
SELECT brand, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM $LAST_RESULT
GROUP BY brand
ORDER BY cnt DESC;

-- Reference earlier result (2 turns back)
SELECT * FROM $RESULT(2) WHERE rating > 4.5;

-- Reset conversation
RESET CONVERSATION;

-- Show conversation history
SELECT * FROM system.conversation_history;
```

### 9.8 Configuration

```yaml
pangrova:
  ai:
    conversation:
      enabled: true
      max-turns-per-session: 10       # keep last 10 queries per session
      session-ttl-minutes: 60         # expire inactive sessions after 1h
```

### 9.9 Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `pangrova.conversation.active_sessions` | Gauge | Active conversation sessions |
| `pangrova.conversation.turns_recorded` | Counter | Total conversation turns recorded |
| `pangrova.conversation.ref_resolved` | Counter | $LAST_RESULT/$RESULT(n) references resolved |
| `pangrova.conversation.ref_failed` | Counter | References that failed (no history) |

### 9.10 Module Impact

| Module | File | Change |
|--------|------|--------|
| `pangrova-spi` | `ConversationTurn.java` (new) | Turn record |
| `pangrova-spi` | `ConversationContext.java` (new) | Context SPI |
| `pangrova-parser` | `PangrovaSql.g4` | + $LAST_RESULT, $RESULT(n) tokens and rules |
| `pangrova-parser` | `ConversationResultRef.java` (new) | AST node |
| `pangrova-planner` | `ConversationRewriter.java` (new) | CTE rewriting |
| `pangrova-coordinator` | `InMemoryConversationManager.java` (new) | Context implementation |
| `pangrova-coordinator` | Query execution path | + turn recording after execution |
