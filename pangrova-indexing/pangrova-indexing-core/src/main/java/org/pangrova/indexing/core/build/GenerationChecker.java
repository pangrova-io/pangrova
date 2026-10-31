package org.pangrova.indexing.core.build;

@FunctionalInterface
public interface GenerationChecker {
    boolean isCurrentGeneration(String indexId, int generation);
}
