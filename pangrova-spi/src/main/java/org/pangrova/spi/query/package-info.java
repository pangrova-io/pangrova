/**
 * Query-owned execution identities, journal facts, audit and recovery ports.
 *
 * <p>Consumers depend on this artifact without Query runtime or store implementations.
 * Fencing identities are still issued and checked by their backend; these contracts
 * never synthesize authority. Runtime and JDBC adapters remain in pangrova-query.</p>
 */
package org.pangrova.spi.query;
