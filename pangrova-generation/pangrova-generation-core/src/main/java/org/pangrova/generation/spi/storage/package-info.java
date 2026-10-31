/**
 * Durable build kernel storage contracts and immutable stage preparation evidence.
 *
 * <p>These {@link org.pangrova.common.api.annotation.Internal} contracts support
 * Pangrova runtime orchestration, storage providers and their composition roots;
 * they are not stable extension APIs for third-party implementations. The kernel
 * owns job, partition, producer-winner and exchange-evidence invariants.</p>
 *
 * <p>Contracts do not expose SQL, JDBC connections, driver types or provider
 * helpers. Providers establish fresh authority and exact execution ownership
 * inside each operation. Returned authority views are process-local evidence,
 * never caller-supplied credentials. Partition preparation callbacks execute
 * before the provider releases transaction locks and perform no network I/O.</p>
 *
 * <p>Tenant, table and model policy, business descriptors, catalog publication
 * pointers and outbox acknowledgement remain with their domain owners. Shared
 * database cooperation belongs to explicitly assembled transaction ports,
 * rather than domain writes added to these storage contracts.</p>
 */
package org.pangrova.generation.spi.storage;
