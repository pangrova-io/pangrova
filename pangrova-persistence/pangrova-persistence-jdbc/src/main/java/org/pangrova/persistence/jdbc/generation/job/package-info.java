/**
 * Internal PostgreSQL artifact-build state machine, owned by pangrova-artifact.
 * Consumers may use its store and same-transaction readers; row execution,
 * planner objects and provider credentials are prohibited here.
 */
package org.pangrova.persistence.jdbc.generation.job;
