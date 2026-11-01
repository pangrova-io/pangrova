package org.pangrova.client.api;

/** Column metadata from the catalog REST API. */
public record MetadataColumnInfo(int id, String name, String type, boolean required) {}
