/**
 * Coordinator-owned immutable query-session facts and conversions to existing SPI/metadata sessions.
 * Query admission, planning, statements and metadata binding may consume this package; it does not
 * own query state transitions, execution, resource release or persistence. Dependencies are limited
 * to shared primitives, SPI session/security contracts and metadata session/snapshot interfaces.
 */
package org.pangrova.coordinator.query.session;
