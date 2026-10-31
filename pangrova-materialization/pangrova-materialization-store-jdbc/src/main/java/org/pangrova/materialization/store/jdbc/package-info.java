/**
 * JDBC-backed materialization repositories outside the pure kernel.
 *
 * <p>The materialization kernel owns definitions, generations, attempts, and the
 * publication protocol through ports. This artifact supplies the relational
 * implementation of those ports for deployments that externalize state.
 */
package org.pangrova.materialization.store.jdbc;
