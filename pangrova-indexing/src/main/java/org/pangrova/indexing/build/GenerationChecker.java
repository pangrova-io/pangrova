package org.pangrova.indexing.build;

@FunctionalInterface
public interface GenerationChecker {
    boolean isCurrentGeneration(String indexId, int generation);
}
