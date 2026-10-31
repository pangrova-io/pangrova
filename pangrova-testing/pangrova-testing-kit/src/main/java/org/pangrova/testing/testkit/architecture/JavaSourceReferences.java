package org.pangrova.testing.testkit.architecture;

import com.sun.source.tree.IdentifierTree;
import com.sun.source.tree.ImportTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.PackageTree;
import com.sun.source.tree.Tree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Shared source facts for module policy tests and the baseline generator; requires JDK 17. */
public final class JavaSourceReferences {
    private JavaSourceReferences() {}

    /** Package declarations and all direct Pangrova references, with the first deterministic source witness. */
    public record SourceFacts(Set<String> packages, Map<String, String> references) {
        public SourceFacts {
            packages = Set.copyOf(packages);
            references = Map.copyOf(references);
        }
    }

    /** Parses one source root; an absent optional source set is empty, malformed sources fail. */
    public static SourceFacts scan(Path root) throws IOException {
        return scan(List.of(root)).get(root);
    }

    /** Parses all requested source sets together without requiring their dependencies or generated classes. */
    public static Map<Path, SourceFacts> scan(Collection<Path> roots) throws IOException {
        Map<Path, Set<String>> packages = new LinkedHashMap<>();
        Map<Path, Map<String, String>> references = new LinkedHashMap<>();
        Map<Path, Path> owners = new HashMap<>();
        List<Path> files = new ArrayList<>();
        for (Path root : roots) {
            if (packages.putIfAbsent(root, new TreeSet<>()) != null) {
                throw new IllegalArgumentException("Repeated source root: " + root);
            }
            references.put(root, new TreeMap<>());
            if (Files.notExists(root)) continue;
            if (!Files.isDirectory(root)) throw new IOException("Not a source directory: " + root);
            try (var paths = Files.walk(root)) {
                for (Path file : paths.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
                    Path real = file.toRealPath();
                    if (owners.putIfAbsent(real, root) != null) {
                        throw new IllegalArgumentException("Overlapping source roots: " + real);
                    }
                    files.add(real);
                }
            }
        }
        if (!files.isEmpty()) parse(files, owners, packages, references);
        Map<Path, SourceFacts> result = new LinkedHashMap<>();
        for (var entry : packages.entrySet()) {
            result.put(entry.getKey(), new SourceFacts(entry.getValue(), references.get(entry.getKey())));
        }
        return Map.copyOf(result);
    }

    private static void parse(List<Path> files, Map<Path, Path> owners, Map<Path, Set<String>> packages,
            Map<Path, Map<String, String>> references) throws IOException {
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("JDK compiler is required for architecture source parsing");
        }
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics,
                    List.of("-proc:none", "--release", "17"), null, manager.getJavaFileObjectsFromPaths(files));
            var positions = Trees.instance(task).getSourcePositions();
            for (var unit : task.parse()) {
                Path file = Path.of(unit.getSourceFile().toUri());
                Path root = owners.get(file);
                Path relativeFile = root.toRealPath().relativize(file);
                if (unit.getPackageName() == null) throw new IllegalStateException("Missing package: " + file);
                packages.get(root).add(unit.getPackageName().toString());
                new TreeScanner<Void, Void>() {
                    private void reference(Tree tree, String name) {
                        if (name.startsWith("org.pangrova.")) {
                            long line = unit.getLineMap().getLineNumber(positions.getStartPosition(unit, tree));
                            references.get(root).putIfAbsent(name, relativeFile + ":" + line);
                        }
                    }

                    @Override
                    public Void visitPackage(PackageTree tree, Void unused) {
                        // A declaration is ownership, not a reference. Package annotations still are references.
                        return scan(tree.getAnnotations(), unused);
                    }

                    @Override
                    public Void visitImport(ImportTree tree, Void unused) {
                        reference(tree, tree.getQualifiedIdentifier().toString());
                        return null;
                    }

                    @Override
                    public Void visitMemberSelect(MemberSelectTree tree, Void unused) {
                        if (dottedName(tree)) {
                            reference(tree, tree.toString());
                            // Do not turn one qualified reference into unresolved package-prefix fragments.
                            return null;
                        }
                        return super.visitMemberSelect(tree, unused);
                    }
                }.scan(unit, null);
            }
            var errors = diagnostics.getDiagnostics().stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR).toList();
            if (!errors.isEmpty()) throw new IllegalStateException("Cannot parse architecture sources: " + errors);
        }
    }

    private static boolean dottedName(Tree tree) {
        while (tree instanceof MemberSelectTree select) tree = select.getExpression();
        return tree instanceof IdentifierTree;
    }

    /** Source-launchable CLI: one root per argument; UTF-8 tab-separated package/reference facts on stdout. */
    public static void main(String[] args) throws IOException {
        List<Path> roots = java.util.Arrays.stream(args).map(Path::of).toList();
        var facts = scan(roots);
        var output = new PrintWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
        for (int index = 0; index < roots.size(); index++) {
            var source = facts.get(roots.get(index));
            for (String pkg : new TreeSet<>(source.packages())) output.println("P\t" + index + "\t" + pkg);
            for (var reference : new TreeMap<>(source.references()).entrySet()) {
                String witness = Base64.getEncoder().encodeToString(
                        reference.getValue().getBytes(StandardCharsets.UTF_8));
                output.println("R\t" + index + "\t" + reference.getKey() + "\t" + witness);
            }
        }
        output.flush();
        if (output.checkError()) throw new IOException("Cannot write architecture source facts");
    }
}
