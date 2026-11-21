package org.pangrova.testing.property;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Resource ownership is explicit; a resource-only declaration cannot exempt executable code. */
final class ResourceOnlyModuleLayout {
    private ResourceOnlyModuleLayout() {}

    static List<String> violations(Path module, List<String> declaredRoots) {
        List<String> errors = new ArrayList<>();
        Path resources = module.resolve("src/main/resources").toAbsolutePath().normalize();
        List<Path> roots = new ArrayList<>();
        if (declaredRoots.isEmpty()) errors.add("No resource roots declared");
        for (String root : declaredRoots) {
            Path path = resources.resolve(root).normalize();
            if (!path.startsWith(resources) || path.equals(resources)) {
                errors.add("Resource root escapes ownership: " + root);
            } else if (regularFiles(path).isEmpty()) {
                errors.add("Empty or missing resource root: " + root);
            } else {
                roots.add(path);
            }
        }
        for (Path file : regularFiles(module.resolve("src"))) {
            String name = file.getFileName().toString();
            Path absolute = file.toAbsolutePath().normalize();
            if (name.endsWith(".java") || name.endsWith(".class")) {
                errors.add("Executable source or bytecode in resource artifact: " + file);
            }
            if (absolute.startsWith(resources.resolve("META-INF/services"))) {
                errors.add("Service provider descriptor in resource artifact: " + file);
            }
            if (absolute.startsWith(resources) && roots.stream().noneMatch(absolute::startsWith)) {
                errors.add("Resource outside declared roots: " + file);
            }
        }
        return List.copyOf(errors);
    }

    private static List<Path> regularFiles(Path root) {
        if (!Files.exists(root)) return List.of();
        try (var files = Files.walk(root)) {
            return files.filter(Files::isRegularFile).toList();
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot inspect resource artifact " + root, failure);
        }
    }
}
