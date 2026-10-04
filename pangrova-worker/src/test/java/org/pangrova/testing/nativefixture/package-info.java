/**
 * Worker-owned, test-only packaged Native runtime fixtures shared with acceptance tests.
 * This exclusive package is the entire native-fixtures classifier surface; it contains
 * no executable test classes or alternative task/transport implementations. Consumers
 * must hold the documented runtime lock and close every real owner. Missing Native
 * prerequisites fail explicitly rather than selecting a simulated execution path.
 */
package org.pangrova.testing.nativefixture;
