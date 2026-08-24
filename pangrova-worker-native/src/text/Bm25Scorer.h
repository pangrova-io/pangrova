// Bm25Scorer: BM25 relevance scoring for full-text hybrid search.
//
// Implements the Okapi BM25 scoring function for postings list iteration.
// Designed for high throughput at TB-scale with batch scoring over
// contiguous term frequency arrays.
//
// BM25(D,Q) = sum over q in Q of:
//   IDF(q) * (tf(q,D) * (k1+1)) / (tf(q,D) + k1 * (1 - b + b * |D|/avgdl))
//
// Thread-safe: all functions are stateless.
// Placement: text/ — Native full-text search components.
#pragma once

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace pangrova::text {

struct Bm25Config {
  double k1 = 1.2;
  double b = 0.75;
  double avgDocLength = 100.0;
  int64_t totalDocs = 1;
};

struct Bm25TermStats {
  int64_t docFrequency;
  double idf;
};

inline double computeIdf(int64_t totalDocs, int64_t docFrequency) {
  if (docFrequency <= 0 || totalDocs <= 0) return 0.0;
  return std::log(1.0 + (static_cast<double>(totalDocs) - static_cast<double>(docFrequency) + 0.5) /
                            (static_cast<double>(docFrequency) + 0.5));
}

inline double scoreSingleTerm(
    double idf, uint32_t tf, uint32_t docLength, const Bm25Config& config) {
  double tfD = static_cast<double>(tf);
  double dlNorm = 1.0 - config.b + config.b * (static_cast<double>(docLength) / config.avgDocLength);
  return idf * (tfD * (config.k1 + 1.0)) / (tfD + config.k1 * dlNorm);
}

struct ScoredDocument {
  int64_t docId;
  double score;
};

/// Batch-scores documents for a single query term.
/// For each document, computes BM25 score from its term frequency and doc length.
///
/// @param docIds       Document IDs for matching documents.
/// @param termFreqs    Term frequency in each document (parallel to docIds).
/// @param docLengths   Document length in tokens (parallel to docIds).
/// @param numDocs      Number of documents in the batch.
/// @param termIdf      Pre-computed IDF for this term.
/// @param config       BM25 parameters.
/// @return Scored documents (same order as input).
inline std::vector<ScoredDocument> scoreBatch(
    const int64_t* docIds,
    const uint32_t* termFreqs,
    const uint32_t* docLengths,
    int64_t numDocs,
    double termIdf,
    const Bm25Config& config) {
  std::vector<ScoredDocument> results;
  results.reserve(static_cast<size_t>(numDocs));
  for (int64_t i = 0; i < numDocs; ++i) {
    double score = scoreSingleTerm(termIdf, termFreqs[i], docLengths[i], config);
    results.push_back({docIds[i], score});
  }
  return results;
}

/// Merges scored results from multiple query terms by summing scores per docId.
/// Input: one ScoredDocument vector per query term.
/// Output: merged scores sorted by score descending.
inline std::vector<ScoredDocument> mergeTermScores(
    const std::vector<std::vector<ScoredDocument>>& perTermResults) {
  // Simple map-based merge for correctness. Production would use
  // sorted merge for better cache behavior on large posting lists.
  struct DocAccumulator {
    int64_t docId;
    double totalScore;
  };

  std::vector<DocAccumulator> merged;
  // Use a flat vector + sort for small cardinalities, which is common
  // in keyword queries (typically 2-5 terms).
  for (const auto& termResults : perTermResults) {
    for (const auto& scored : termResults) {
      bool found = false;
      for (auto& acc : merged) {
        if (acc.docId == scored.docId) {
          acc.totalScore += scored.score;
          found = true;
          break;
        }
      }
      if (!found) {
        merged.push_back({scored.docId, scored.score});
      }
    }
  }

  // Sort by score descending.
  std::sort(merged.begin(), merged.end(),
            [](const DocAccumulator& a, const DocAccumulator& b) {
              return a.totalScore > b.totalScore;
            });

  std::vector<ScoredDocument> results;
  results.reserve(merged.size());
  for (const auto& acc : merged) {
    results.push_back({acc.docId, acc.totalScore});
  }
  return results;
}

}  // namespace pangrova::text
