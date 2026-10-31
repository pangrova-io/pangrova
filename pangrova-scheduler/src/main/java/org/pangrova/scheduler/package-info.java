/**
 * Worker placement, admission, quota, and speculative scheduling decisions.
 *
 * <p>{@code api} holds stable contracts consumed by the coordinator and workers,
 * {@code core} holds the decision algorithms, and {@code adapter} holds
 * transport-facing implementations. The module never parses SQL or catalog names.</p>
 */
package org.pangrova.scheduler;
