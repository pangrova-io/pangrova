/**
 * Worker-owned remote-shuffle trust/key material loading. Consumed only by the
 * Worker's channel assembly; it neither owns channel admission/lifetime nor
 * imports task execution, remote service ownership, or Coordinator authority.
 */
package org.pangrova.worker.exchange.remote.security;
