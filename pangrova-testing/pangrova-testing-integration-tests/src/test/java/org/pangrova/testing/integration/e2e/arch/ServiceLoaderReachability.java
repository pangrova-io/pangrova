package org.pangrova.testing.integration.e2e.arch;

import com.sun.source.tree.MemberSelectTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.util.JavacTask;
import com.sun.source.util.TreePathScanner;
import com.sun.source.util.Trees;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaMethodCall;
import org.pangrova.testing.testkit.TestProjectPaths;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/** Adds registered providers only at a reachable, actual production ServiceLoader instruction. */
final class ServiceLoaderReachability {
    private static final List<Path> SOURCE_ROOTS = sourceRoots();
    private static final Map<Path, List<LoadSite>> SOURCE_SITES = new ConcurrentHashMap<>();

    private ServiceLoaderReachability() {}

    static void enqueueProviders(
            String current, JavaClasses importedClasses,
            Queue<String> queue, Set<String> visited) {
        for (String contract : contractsOf(current, importedClasses)) {
            if (!contract.startsWith("org.pangrova.")) continue;
            try {
                Class<?> service = Class.forName(contract, false,
                        Thread.currentThread().getContextClassLoader());
                ServiceLoader.load(service).stream().map(provider -> provider.type().getName())
                        .filter(name -> name.startsWith("org.pangrova."))
                        .filter(name -> !visited.contains(name)).forEach(queue::add);
            } catch (ClassNotFoundException failure) {
                throw new AssertionError("Reachable ServiceLoader contract is absent: " + contract, failure);
            }
        }
    }

    static Set<String> contractsOf(String current, JavaClasses importedClasses) {
        Set<String> contracts = new LinkedHashSet<>();
        importedClasses.stream().filter(type -> type.getName().equals(current)).findFirst().ifPresent(type -> {
            for (JavaMethodCall call : type.getMethodCallsFromSelf()) {
                if (!call.getTarget().getOwner().getName().equals(ServiceLoader.class.getName())
                        || !call.getTarget().getName().equals("load")) {
                    continue;
                }
                Path source = sourceOf(type);
                var sites = SOURCE_SITES.computeIfAbsent(source, ServiceLoaderReachability::parseSites).stream()
                        .filter(site -> site.firstLine() <= call.getLineNumber()
                                && call.getLineNumber() <= site.lastLine()).toList();
                if (sites.isEmpty()) {
                    throw new AssertionError("Cannot identify ServiceLoader contract at "
                            + call.getSourceCodeLocation());
                }
                for (LoadSite site : sites) {
                    if (site.contract() == null) {
                        throw new AssertionError("Cannot identify ServiceLoader contract from dynamic arguments at "
                                + source + ":" + site.firstLine());
                    }
                    // ArchUnit resolves literal types, including imports and binary nested names;
                    // the AST identifies which literal is actually the load argument.
                    var candidates = type.getReferencedClassObjects().stream()
                            .map(reference -> reference.getValue().getName())
                            .filter(name -> name.replace('$', '.').equals(site.contract())
                                    || name.replace('$', '.').endsWith("." + site.contract()))
                            .distinct().toList();
                    if (candidates.size() != 1) {
                        throw new AssertionError("Cannot resolve ServiceLoader contract " + site.contract()
                                + " at " + call.getSourceCodeLocation() + ": " + candidates);
                    }
                    contracts.add(candidates.get(0));
                }
            }
        });
        return Set.copyOf(contracts);
    }

    private static Path sourceOf(JavaClass type) {
        String topLevel = type.getName().split("\\$", 2)[0].replace('.', '/') + ".java";
        var sources = SOURCE_ROOTS.stream().map(root -> root.resolve(topLevel)).filter(Files::isRegularFile).toList();
        if (sources.size() != 1) throw new AssertionError("Expected one source owner for " + type.getName() + sources);
        return sources.get(0);
    }

    private static List<LoadSite> parseSites(Path source) {
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new AssertionError("ServiceLoader source checks require JDK 17");
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        List<LoadSite> sites = new ArrayList<>();
        try (var manager = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics, List.of("-proc:none"),
                    null, manager.getJavaFileObjectsFromPaths(List.of(source)));
            var positions = Trees.instance(task).getSourcePositions();
            for (var unit : task.parse()) {
                boolean staticLoad = unit.getImports().stream().anyMatch(item -> item.isStatic()
                        && Set.of("java.util.ServiceLoader.load", "java.util.ServiceLoader.*")
                                .contains(item.getQualifiedIdentifier().toString()));
                new TreePathScanner<Void, Void>() {
                    @Override public Void visitMethodInvocation(MethodInvocationTree node, Void unused) {
                        String select = node.getMethodSelect().toString();
                        if (select.equals("ServiceLoader.load") || select.equals("java.util.ServiceLoader.load")
                                || (staticLoad && select.equals("load"))) {
                            var literals = node.getArguments().stream()
                                    .filter(argument -> argument instanceof MemberSelectTree member
                                            && member.getIdentifier().contentEquals("class"))
                                    .map(argument -> ((MemberSelectTree) argument).getExpression().toString()).toList();
                            sites.add(new LoadSite(unit.getLineMap().getLineNumber(positions.getStartPosition(unit, node)),
                                    unit.getLineMap().getLineNumber(positions.getEndPosition(unit, node)),
                                    literals.size() == 1 ? literals.get(0) : null));
                        }
                        return super.visitMethodInvocation(node, unused);
                    }
                }.scan(unit, null);
            }
            if (diagnostics.getDiagnostics().stream().anyMatch(item -> item.getKind() == Diagnostic.Kind.ERROR)) {
                throw new AssertionError("Cannot parse ServiceLoader source " + source + diagnostics.getDiagnostics());
            }
            return List.copyOf(sites);
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot read ServiceLoader source " + source, failure);
        }
    }

    private static List<Path> sourceRoots() {
        List<Path> roots = new ArrayList<>();
        Path project = TestProjectPaths.findProjectRoot();
        try (var modules = Files.list(project)) {
            for (Path module : modules.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith("pangrova-")).toList()) {
                addRoots(module, roots);
                try (var children = Files.list(module)) {
                    for (Path child : children.filter(Files::isDirectory)
                            .filter(path -> path.getFileName().toString().startsWith("pangrova-")).toList()) {
                        addRoots(child, roots);
                    }
                }
            }
            return List.copyOf(roots);
        } catch (IOException failure) {
            throw new UncheckedIOException("Cannot discover ServiceLoader source owners", failure);
        }
    }

    private static void addRoots(Path module, List<Path> roots) {
        if (!Files.isRegularFile(module.resolve("pom.xml"))) return;
        for (String kind : List.of("main", "test")) {
            Path source = module.resolve("src/" + kind + "/java");
            if (Files.isDirectory(source)) roots.add(source);
        }
    }

    private record LoadSite(long firstLine, long lastLine, String contract) {}
}
