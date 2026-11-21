import com.sun.source.tree.*;
import com.sun.source.util.*;
import javax.tools.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/** Parses candidate production sources without resolving project dependencies. */
class DeadStubCheck {
    private static final Pattern ISSUE = Pattern.compile(
            "https://github\\.com/[^/\\s]+/[^/\\s]+/issues/[\\w-]+|https://issues\\.[^\\s]+|\\bJIRA-[0-9]+\\b");

    public static void main(String[] args) throws Exception {
        var files = new ArrayList<Path>();
        for (String name : new String(System.in.readAllBytes(), StandardCharsets.UTF_8).split("\u0000")) {
            if (name.isEmpty() || name.matches("(?:.*/)?(?:target|generated-sources|deps-download)/.*")) continue;
            Path file = Path.of(name);
            if (Files.notExists(file)) continue; // tracked source deleted in the working tree
            String source = Files.readString(file);
            if (source.contains("UnsupportedOperationException") || source.contains("warn")) files.add(file);
        }
        var compiler = ToolProvider.getSystemJavaCompiler();
        var diagnostics = new DiagnosticCollector<JavaFileObject>();
        int violations = 0;
        try (var manager = compiler.getStandardFileManager(diagnostics, Locale.ROOT, StandardCharsets.UTF_8)) {
            var task = (JavacTask) compiler.getTask(null, manager, diagnostics, List.of("-proc:none"), null,
                    manager.getJavaFileObjectsFromPaths(files));
            if (!files.isEmpty()) {
                var positions = Trees.instance(task).getSourcePositions();
                for (var unit : task.parse()) {
                    var scan = new Scan(unit, positions);
                    scan.scan(unit, null);
                    violations += scan.count;
                }
            }
        }
        for (var diagnostic : diagnostics.getDiagnostics()) {
            if (diagnostic.getKind() == Diagnostic.Kind.ERROR) throw new IllegalStateException(diagnostic.toString());
        }
        System.out.println(violations == 0 ? "No dead stub patterns found." : "Found " + violations + " dead stub(s).");
        if (violations != 0) System.exit(1);
    }

    private static final class Scan extends TreePathScanner<Void, Void> {
        private final CompilationUnitTree unit;
        private final SourcePositions positions;
        private final List<String> lines;
        private int count;

        Scan(CompilationUnitTree unit, SourcePositions positions) throws java.io.IOException {
            this.unit = unit;
            this.positions = positions;
            lines = unit.getSourceFile().getCharContent(true).toString().lines().toList();
        }

        @Override public Void visitMethod(MethodTree method, Void unused) {
            if (method.getBody() != null && !method.getBody().getStatements().isEmpty()
                    && method.getBody().getStatements().stream().allMatch(Scan::stubStatement)) {
                var owner = (ClassTree) getCurrentPath().getParentPath().getLeaf();
                boolean tracked = owner.getModifiers().getAnnotations().stream()
                        .anyMatch(a -> a.getAnnotationType().toString().matches("(?:org\\.pangrova\\.common\\.annotation\\.)?ExperimentalImplementation")
                                && a.getArguments().stream().anyMatch(Scan::trackingIssue));
                int start = line(positions.getStartPosition(unit, method));
                int end = line(positions.getEndPosition(unit, method) - 1);
                boolean issueComment = lines.subList(Math.max(0, start - 2), end).stream()
                        .map(String::stripLeading)
                        .filter(s -> s.startsWith("//") || s.startsWith("/*") || s.startsWith("*"))
                        .anyMatch(s -> ISSUE.matcher(s).find());
                if (!tracked && !issueComment) {
                    count++;
                    System.out.printf("%s:%d %s%n", unit.getSourceFile().getName(), start, method.getName());
                }
            }
            return super.visitMethod(method, unused);
        }

        private int line(long position) {
            return (int) unit.getLineMap().getLineNumber(position);
        }

        private static boolean trackingIssue(ExpressionTree expression) {
            return expression instanceof AssignmentTree assignment
                    && assignment.getVariable().toString().equals("trackingIssue")
                    && assignment.getExpression() instanceof LiteralTree literal
                    && literal.getValue() instanceof String id && !id.isBlank();
        }

        private static boolean stubStatement(StatementTree statement) {
            if (statement instanceof ThrowTree t && t.getExpression() instanceof NewClassTree n
                    && n.getIdentifier().toString().matches("(java\\.lang\\.)?UnsupportedOperationException")) {
                return n.getArguments().isEmpty() || (n.getArguments().size() == 1
                        && n.getArguments().get(0) instanceof LiteralTree literal && "".equals(literal.getValue()));
            }
            return statement instanceof ExpressionStatementTree e && e.getExpression() instanceof MethodInvocationTree call
                    && call.getMethodSelect() instanceof MemberSelectTree member && member.getIdentifier().contentEquals("warn")
                    && !call.getArguments().isEmpty() && call.getArguments().get(0) instanceof LiteralTree literal
                    && literal.getValue() instanceof String message && message.toLowerCase(Locale.ROOT).startsWith("not implemented");
        }
    }
}
