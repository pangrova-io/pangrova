/**
 * SPI contracts for resource management: auto-scaling, admission control,
 * workload classification, runaway query guard, and unified memory budget
 * enforcement.
 *
 * <p>These SPIs address gaps G-RES-03 through G-RES-07 identified in the
 * architecture review. All interfaces are {@code @Experimental} and may
 * evolve as implementations mature.</p>
 *
 * @see org.pangrova.spi.resource.scaling
 * @see org.pangrova.spi.resource.admission
 * @see org.pangrova.spi.resource.classification
 * @see org.pangrova.spi.resource.guard
 * @see org.pangrova.spi.resource.memory
 */
package org.pangrova.spi.resource;
