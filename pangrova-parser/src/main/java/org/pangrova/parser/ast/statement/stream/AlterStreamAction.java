package org.pangrova.parser.ast.statement.stream;

import java.util.Map;

/**
 * Actions for {@code ALTER STREAM} statements.
 */
public sealed interface AlterStreamAction {

    /** Suspend the stream's CDC pipeline. */
    record Suspend() implements AlterStreamAction {}

    /** Resume the stream's CDC pipeline. */
    record Resume() implements AlterStreamAction {}

    /** Update stream-level configuration properties. */
    record SetProperties(Map<String, String> properties) implements AlterStreamAction {

        public SetProperties {
            properties = Map.copyOf(properties);
        }
    }
}
