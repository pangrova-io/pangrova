/**
 * Framework-free public client contract for Pangrova.
 *
 * <p>This module owns the public REST DTOs and the client-facing API. It
 * deliberately depends on {@code pangrova-common} plus Jackson annotations
 * only: no protobuf, Arrow, Flight, server, connector, or transport classes.
 *
 * <p>Server endpoints serialize these types; clients deserialize them. Because
 * the JSON field names are the wire contract, every DTO documents its
 * {@code @JsonProperty} names and is covered by contract tests.
 *
 * <p>Internal Coordinator–Worker REST DTOs stay in {@code pangrova-protocol};
 * transport implementations stay in {@code pangrova-transport}.
 */
package org.pangrova.client.core;
