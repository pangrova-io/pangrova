package org.pangrova.testing.testkit.architecture;

import com.sun.source.tree.ClassTree;
import com.sun.source.util.JavacTask;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Enumerates named top-level and member declarations from Git or working-tree Java sources. */
public final class JavaTypeInventory {
    private static final int BATCH_SIZE = 256;

    private JavaTypeInventory() {}

    /** Input is a count followed by length-prefixed UTF-8 sources. Output pairs each index with an FQN. */
    public static void main(String[] args) throws IOException {
        var input = new DataInputStream(System.in);
        int count = input.readInt();
        if (count < 0) throw new IOException("Negative source count");
        var compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) throw new IllegalStateException("Inventory needs a JDK");
        try (var manager = compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
            for (int first = 0; first < count; first += BATCH_SIZE) {
                var sources = new ArrayList<JavaFileObject>();
                for (int i = first; i < Math.min(count, first + BATCH_SIZE); i++) {
                    int length = input.readInt();
                    if (length < 0) throw new IOException("Negative source length at index " + i);
                    byte[] bytes = input.readNBytes(length);
                    if (bytes.length != length) throw new EOFException("Truncated source at index " + i);
                    sources.add(source(i, new String(bytes, StandardCharsets.UTF_8)));
                }
                var errors = new DiagnosticCollector<JavaFileObject>();
                var task = (JavacTask) compiler.getTask(null, manager, errors, List.of("-proc:none"), null, sources);
                for (var unit : task.parse()) {
                    if (unit.getPackageName() == null) continue;
                    for (var declaration : unit.getTypeDecls()) {
                        if (declaration instanceof ClassTree type) {
                            String file = unit.getSourceFile().toUri().getPath();
                            emit(file.substring(1, file.length() - ".java".length()), unit.getPackageName().toString(), type);
                        }
                    }
                }
                for (var diagnostic : errors.getDiagnostics()) {
                    if (diagnostic.getKind() == Diagnostic.Kind.ERROR) throw new IOException(diagnostic.toString());
                }
            }
        }
        if (input.read() != -1 || System.out.checkError()) throw new IOException("Invalid inventory stream");
    }

    private static JavaFileObject source(int index, String contents) {
        return new SimpleJavaFileObject(URI.create("string:///" + index + ".java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return contents;
            }
        };
    }

    private static void emit(String index, String enclosing, ClassTree type) {
        String fqn = enclosing + "." + type.getSimpleName();
        System.out.println(index + "\t" + fqn);
        for (var member : type.getMembers()) {
            if (member instanceof ClassTree nested) emit(index, fqn, nested);
        }
    }
}
