/**
 * Durable query/task state journals and their ordered SQL write owner.
 * Owned by Persistence JDBC; internal backend implementation consumed by process assembly
 * and transaction adapters. Query orchestration and authorization decisions belong to callers.
 */
package org.pangrova.persistence.jdbc.query.state;
