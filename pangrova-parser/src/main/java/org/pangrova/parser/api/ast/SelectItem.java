package org.pangrova.parser.api.ast;

/**
 * Base interface for select list items.
 */
public sealed interface SelectItem permits AllColumns, ExpressionItem {
}
