package org.pangrova.parser.ast;

/**
 * Represents SET property = value session configuration statement.
 */
public record SetSessionStatement(String propertyName, String value) implements Statement {}
