/**
 * Paimon connector internal physical and declared schema conversion shared by handle codecs and
 * SDK write adapters. Conversion must preserve exact SDK units and must not depend on handle
 * transport or write orchestration.
 */
package org.pangrova.connector.paimon.schema;
