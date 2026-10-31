/**
 * Table owner: durable control state, maintenance policy and runs, statistics,
 * governance bindings, and their JDBC/in-memory stores.
 *
 * <p>The module is an acyclic foundation for metadata and Coordinator adapters;
 * it never depends on catalog assembly, planning, scheduling, or data-plane
 * execution.</p>
 */
package org.pangrova.table;
