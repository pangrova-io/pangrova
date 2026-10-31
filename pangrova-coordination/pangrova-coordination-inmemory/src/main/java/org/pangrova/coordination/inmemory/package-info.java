/**
 * Process-local lease and registry providers owned by Coordination In-Memory.
 * Internal assembly surface for single-process deployments and provider tests;
 * consumers depend on Coordination API for contracts and immutable snapshots.
 * Database persistence, quorum protocols and Coordinator process lifecycle are excluded.
 */
package org.pangrova.coordination.inmemory;
