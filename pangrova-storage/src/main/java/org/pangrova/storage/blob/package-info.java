/**
 * Content-addressed blob storage backends shared by the Worker COPY path and the AI FILE layer.
 *
 * <p>This module owns local filesystem and managed S3 publication mechanics only. Tenant
 * authorization, admission budgets, and object-store policy stay with their callers.</p>
 */
package org.pangrova.storage.blob;
