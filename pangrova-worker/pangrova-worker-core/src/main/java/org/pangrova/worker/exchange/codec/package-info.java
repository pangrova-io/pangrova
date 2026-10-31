/**
 * Bounded byte chunk encoding for canonical exchange batches.
 *
 * <p>These stateless helpers depend only on the wire contract and JDK byte APIs;
 * transport sessions, RPC channels and Native task lifetimes remain owned by
 * their respective execution and remote-shuffle components.</p>
 */
package org.pangrova.worker.exchange.codec;
