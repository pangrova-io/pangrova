/**
 * Streaming aggregation operators with retraction semantics and window support.
 *
 * <p>This package provides incremental aggregation for the declarative
 * incremental data transformation subsystem. Key capabilities:</p>
 * <ul>
 *   <li>AVG with retraction (correct results on INSERT/UPDATE/DELETE)</li>
 *   <li>COUNT DISTINCT with optional HyperLogLog approximation</li>
 *   <li>Window types: TUMBLE, HOP, SESSION, CUMULATE</li>
 *   <li>Watermark-driven window emission with late data handling</li>
 * </ul>
 */
package org.pangrova.streaming.aggregation;
