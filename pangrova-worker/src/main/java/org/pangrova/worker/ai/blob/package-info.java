/**
 * BLOB orphan cleanup for external blob storage.
 *
 * <p>Scans blob storage prefixes, identifies unreferenced blob files,
 * and deletes those older than a configurable grace period (default 24 hours).</p>
 */
package org.pangrova.worker.ai.blob;
