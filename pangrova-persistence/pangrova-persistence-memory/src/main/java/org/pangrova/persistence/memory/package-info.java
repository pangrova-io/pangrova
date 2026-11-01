/**
 * Process-local persistence implementations; no state survives process restart.
 * Owned by Persistence Memory; internal implementations composed by application assembly.
 * No JDBC, cloud SDK, query orchestration or process bootstrap dependency is permitted.
 */
package org.pangrova.persistence.memory;
