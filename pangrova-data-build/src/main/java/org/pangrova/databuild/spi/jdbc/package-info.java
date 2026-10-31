/**
 * Artifact-owned transaction ports for current domain authority.
 *
 * <p>These internal integration contracts are consumed by artifact JDBC stores and
 * their explicitly assembled domain adapters. Implementations use the caller's
 * connection and authorization lock order; they never acquire a replacement
 * transaction, commit, roll back, or close the connection. This package does not
 * issue execution leases or own tenant, table, or model policy.</p>
 */
package org.pangrova.databuild.spi.jdbc;
