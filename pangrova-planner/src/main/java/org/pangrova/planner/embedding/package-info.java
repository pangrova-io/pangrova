/**
 * Planner rules for auto-embedding column injection and query-time text embedding.
 *
 * <p>Contains rules that detect DML operations on tables with auto-embedding columns
 * and produce directives for the executor to compute embeddings synchronously or
 * asynchronously. Also detects text literals in vector similarity predicates and
 * rewrites them to use the column's registered embedding model.</p>
 */
package org.pangrova.planner.embedding;
