/**
 * Artifact-owned reads of durable build dispatch, dependencies, pages, and splits.
 *
 * <p>This internal JDBC integration surface is shared by build stores and domain
 * transaction adapters. Queries use the supplied connection without owning its
 * transaction. They do not establish current authorization, acquire execution
 * ownership, or publish domain state; callers must enforce those boundaries.</p>
 */
package org.pangrova.databuild.store.jdbc.query;
