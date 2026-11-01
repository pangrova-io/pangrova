/**
 * Canonical durable security encodings owned by Persistence JDBC. Internal metadata writers
 * and replay adapters share these exact bytes; API values remain independent of JSON and JDBC.
 * Encoding records observed authority and never grants current execution permission.
 */
package org.pangrova.persistence.jdbc.security.codec;
