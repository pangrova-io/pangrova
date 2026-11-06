package org.pangrova.parser.core.build;

import org.pangrova.parser.api.ast.SourceLocation;

import java.util.IdentityHashMap;
import java.util.Optional;

/**
 * Registry that associates AST nodes (Statement/Expression) with their source
 * location in the original SQL text. Uses identity-based lookup since AST nodes
 * are immutable records and the same logical content may appear at different
 * positions.
 *
 * <p>Populated by AstBuilder during parse tree visitation. Downstream consumers
 * (validators, planners, error reporters) query this registry to obtain precise
 * source positions for diagnostics.</p>
 */
public final class SourceLocationRegistry {

    private final IdentityHashMap<Object, SourceLocation> locations = new IdentityHashMap<>();

    /**
     * Associates a source location with an AST node.
     *
     * @param node     the AST node (Statement or Expression instance)
     * @param location the source position extracted from the parse tree
     */
    public void register(Object node, SourceLocation location) {
        if (node != null && location != null) {
            locations.put(node, location);
        }
    }

    /**
     * Retrieves the source location for a given AST node.
     *
     * @param node the AST node to look up
     * @return the source location if registered, empty otherwise
     */
    public Optional<SourceLocation> lookup(Object node) {
        return Optional.ofNullable(locations.get(node));
    }

    /**
     * Returns the number of registered nodes.
     */
    public int size() {
        return locations.size();
    }
}
