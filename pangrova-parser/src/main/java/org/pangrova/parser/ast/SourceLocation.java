package org.pangrova.parser.ast;

/**
 * Source position information for an AST node, enabling downstream validators
 * and planners to report precise error locations.
 *
 * @param line     1-based line number where the node starts
 * @param col      1-based column number where the node starts
 * @param startTok token index of the first token in this node
 * @param endTok   token index of the last token in this node (inclusive)
 */
public record SourceLocation(int line, int col, int startTok, int endTok) {}
