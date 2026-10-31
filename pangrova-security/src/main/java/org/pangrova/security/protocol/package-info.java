/**
 * Security-owned persisted wire contracts for deferred authorization observations.
 *
 * <p>Encoding, bounded decoding and content digests live here so API values remain
 * independent of serialization implementations. Restoring bytes never authorizes
 * replay: current subject, tenant, policy and model authority must still be checked.</p>
 */
package org.pangrova.security.protocol;
