/**
 * Query-owned persistence implementations for execution anchors, journals, lineage and audit.
 *
 * <p>JDBC and in-memory stores implement the Query API contracts and own their
 * transactions, admitted operations and resource closure. They do not own SQL
 * planning or process bootstrap; application assembly selects and closes them.</p>
 */
package org.pangrova.query.store;
