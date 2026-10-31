/**
 * Scheduler-owned worker performance scoring shared by placement and selection
 * strategies. This internal computation depends on neither orchestration nor
 * concrete strategies; changing it requires placement and strategy regression tests.
 */
package org.pangrova.scheduler.core.scoring;
