/**
 * Security audit backend composition for application bootstrap consumers.
 * This assembly may construct Core and JDBC writers; contracts and writer
 * implementations must not depend on it. Explicit durable backend selections
 * require a DataSource and cannot degrade to logging.
 */
package org.pangrova.security.assembly.audit;
