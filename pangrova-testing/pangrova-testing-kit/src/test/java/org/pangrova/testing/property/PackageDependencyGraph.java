package org.pangrova.testing.property;

import com.sun.source.tree.ClassTree;
import com.sun.source.tree.CompilationUnitTree;
import com.sun.source.tree.ImportTree;
import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.Tree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreeScanner;
import com.sun.source.util.Trees;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Source dependencies: imports (including unused imports) and qualified type references. */
final class PackageDependencyGraph {
    private final Map<String, Map<String, String>> edges = new TreeMap<>();
    private final Map<String, String> typePackages = new HashMap<>();

    static Set<String> imports(Path sourceRoot) throws IOException {
        return org.pangrova.testing.testkit.architecture.JavaSourceReferences.scan(sourceRoot).references().keySet();
    }

    static List<Path> sourceRoots(Path root) throws Exception {
        List<Path> roots = new ArrayList<>();
        visitModule(root.toRealPath(), new HashSet<>(), roots);
        return List.copyOf(roots);
    }

    private static void visitModule(Path module, Set<Path> visited, List<Path> roots) throws Exception {
        Path real = module.toRealPath();
        if (!visited.add(real)) {
            throw new IllegalStateException("Repeated reactor module: " + real);
        }
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        factory.setExpandEntityReferences(false);
        var document = factory.newDocumentBuilder().parse(real.resolve("pom.xml").toFile());
        Path source = real.resolve("src/main/java");
        if (Files.isDirectory(source)) {
            roots.add(source);
        }
        // Only direct modules: inactive profile modules do not belong to the default reactor.
        var children = document.getDocumentElement().getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            var child = children.item(i);
            if (!"modules".equals(child.getNodeName())) {
                continue;
            }
            var modules = child.getChildNodes();
            for (int j = 0; j < modules.getLength(); j++) {
                var entry = modules.item(j);
                if ("module".equals(entry.getNodeName())) {
                    visitModule(real.resolve(entry.getTextContent().trim()), visited, roots);
                }
            }
        }
    }

    static PackageDependencyGraph scan(Path root) throws Exception {
        Path realRoot = root.toRealPath();
        List<Path> files = new ArrayList<>();
        for (Path source : sourceRoots(realRoot)) {
            try (var paths = Files.walk(source)) {
                files.addAll(paths.filter(p -> p.toString().endsWith(".java")).sorted().toList());
            }
        }
        if (files.isEmpty()) {
            throw new IllegalStateException("No main Java sources in reactor: " + root);
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new IllegalStateException("JDK compiler is required for architecture source parsing");
        }
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        try (var manager = compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics,
                    List.of("-proc:none", "--release", "17"), null, manager.getJavaFileObjectsFromPaths(files));
            List<CompilationUnitTree> units = new ArrayList<>();
            task.parse().forEach(units::add);
            var errors = diagnostics.getDiagnostics().stream()
                    .filter(d -> d.getKind() == Diagnostic.Kind.ERROR).toList();
            if (!errors.isEmpty()) {
                throw new IllegalStateException("Cannot parse architecture sources: " + errors);
            }
            var result = new PackageDependencyGraph();
            for (var unit : units) {
                if (unit.getPackageName() == null) {
                    throw new IllegalStateException("Missing package: " + unit.getSourceFile().getName());
                }
                String pkg = unit.getPackageName().toString();
                result.edges.computeIfAbsent(pkg, ignored -> new TreeMap<>());
                for (Tree declaration : unit.getTypeDecls()) {
                    if (declaration instanceof ClassTree type) {
                        result.indexType(pkg + "." + type.getSimpleName(), pkg, type);
                    }
                }
            }
            var positions = Trees.instance(task).getSourcePositions();
            for (var unit : units) {
                String pkg = unit.getPackageName().toString();
                new TreeScanner<Void, Void>() {
                    private void reference(Tree tree, String name, boolean wildcard) {
                        String target = wildcard && result.edges.containsKey(name) ? name : result.owner(name);
                        if (target != null && !pkg.equals(target)) {
                            long line = unit.getLineMap().getLineNumber(positions.getStartPosition(unit, tree));
                            String file = realRoot.relativize(
                                    Path.of(unit.getSourceFile().toUri())).toString();
                            result.edges.get(pkg).putIfAbsent(target, file + ":" + line + " -> " + name);
                        }
                    }

                    @Override
                    public Void visitImport(ImportTree tree, Void unused) {
                        String name = tree.getQualifiedIdentifier().toString();
                        boolean wildcard = name.endsWith(".*");
                        reference(tree, wildcard ? name.substring(0, name.length() - 2) : name, wildcard);
                        return null;
                    }

                    @Override
                    public Void visitMemberSelect(MemberSelectTree tree, Void unused) {
                        String name = tree.toString();
                        if (name.startsWith("org.pangrova.")) {
                            reference(tree, name, false);
                        }
                        return super.visitMemberSelect(tree, unused);
                    }
                }.scan(unit, null);
            }
            return result;
        }
    }

    private void indexType(String name, String pkg, ClassTree type) {
        if (typePackages.putIfAbsent(name, pkg) != null) {
            throw new IllegalStateException("Duplicate production type: " + name);
        }
        for (Tree member : type.getMembers()) {
            if (member instanceof ClassTree nested) {
                indexType(name + "." + nested.getSimpleName(), pkg, nested);
            }
        }
    }

    private String owner(String reference) {
        for (String candidate = reference; candidate.contains(".");
                candidate = candidate.substring(0, candidate.lastIndexOf('.'))) {
            String owner = typePackages.get(candidate);
            if (owner != null) {
                return owner;
            }
        }
        return null;
    }

    Set<String> dependencies(String pkg) {
        return Set.copyOf(edges.getOrDefault(pkg, Map.of()).keySet());
    }

    List<List<String>> cycles() {
        var search = new Components();
        for (String pkg : edges.keySet()) {
            if (!search.index.containsKey(pkg)) {
                search.visit(pkg);
            }
        }
        search.cycles.sort(java.util.Comparator.comparing(cycle -> cycle.get(0)));
        return List.copyOf(search.cycles);
    }

    void writeReport(Path path) throws IOException {
        StringBuilder report = new StringBuilder("Main-source package dependency cycles\n");
        report.append("Packages: ").append(edges.size()).append("\n");
        for (var cycle : cycles()) {
            report.append("\nComponent: ").append(cycle).append("\n");
            for (String from : cycle) {
                edges.get(from).forEach((to, witness) -> {
                    if (cycle.contains(to)) {
                        report.append("  ").append(from).append(" -> ").append(to)
                                .append("\n    ").append(witness).append("\n");
                    }
                });
            }
        }
        Files.createDirectories(path.toAbsolutePath().getParent());
        Files.writeString(path, report, StandardCharsets.UTF_8);
    }

    private final class Components {
        private final Map<String, Integer> index = new HashMap<>();
        private final Map<String, Integer> low = new HashMap<>();
        private final Deque<String> stack = new ArrayDeque<>();
        private final Set<String> onStack = new HashSet<>();
        private final List<List<String>> cycles = new ArrayList<>();

        private void visit(String node) {
            int next = index.size();
            index.put(node, next);
            low.put(node, next);
            stack.push(node);
            onStack.add(node);
            for (String target : edges.get(node).keySet()) {
                if (!index.containsKey(target)) {
                    visit(target);
                    low.put(node, Math.min(low.get(node), low.get(target)));
                } else if (onStack.contains(target)) {
                    low.put(node, Math.min(low.get(node), index.get(target)));
                }
            }
            if (low.get(node).equals(index.get(node))) {
                Set<String> component = new TreeSet<>();
                String member;
                do {
                    member = stack.pop();
                    onStack.remove(member);
                    component.add(member);
                } while (!member.equals(node));
                if (component.size() > 1) {
                    cycles.add(List.copyOf(component));
                }
            }
        }
    }
}
