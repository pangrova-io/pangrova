/**
 * Internal PostgreSQL artifact-build state machine, owned by pangrova-metadata.
 * Consumers may use its store and same-transaction readers; row execution,
 * planner objects and provider credentials are prohibited here.
 */
package org.pangrova.metadata.artifact.jdbc.build;
