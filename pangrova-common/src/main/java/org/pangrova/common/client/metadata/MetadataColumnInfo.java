package org.pangrova.common.client.metadata;

/** Column metadata from the catalog REST API. */
public record MetadataColumnInfo(int id, String name, String type, boolean required) {}
