package client.jobmanage.util.analyze;

import client.Configuration;
import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.TypeDeclaration;

import javax.tools.*;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.Collections;

/**
 * CodeAnalyzeUtil provides compiling and executing APIs for JAVA code
 */
public class CodeAnalyzeUtil {
    /**
     * Compile the code from String to class file
     * @param code      code
     * @param className class name
     */
    public static void compile(String code, String className) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();   // init compiler
        StandardJavaFileManager standardFileManager = compiler.getStandardFileManager(null, null, null);    // init file manager
        CompiledFileManager fileManager = new CompiledFileManager(standardFileManager);
        StringFileObject file = new StringFileObject(className, code);
        Iterable<? extends javax.tools.JavaFileObject> fileObjects = Collections.singletonList(file);

        JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, null, null, null, fileObjects);

        boolean compileResult = task.call();    // compile the code

        if (!compileResult) {
            throw new RuntimeException("Compilation failed.");
        }
    }

    /**
     * StringFileObject is a file object used to represent JAVA source code in String
     */
    private static class StringFileObject extends SimpleJavaFileObject {
        final String code;

        StringFileObject(String name, String code) {
            super(URI.create("string:///" + name.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.code = code;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return code;
        }
    }

    /**
     * CompiledFileObject is a file object used to represent compiled class file
     */
    private static class CompiledFileObject extends SimpleJavaFileObject {
        private final String className;
        private final String classDir;

        CompiledFileObject(String className, String classDir, Kind kind) {
            super(URI.create("file:///" + classDir + className.replace('.', '/') + kind.extension), kind);
            this.className = className;
            this.classDir = classDir;
        }

        @Override
        public OutputStream openOutputStream() throws IOException {
            File file = new File(classDir, className.replace('.', '/') + ".class");
            File parentFile = file.getParentFile();
            if (!parentFile.exists() && !parentFile.mkdirs()) {
                throw new IOException("Failed to create directory " + parentFile.getAbsolutePath());
            }
            return Files.newOutputStream(file.toPath());
        }
    }

    /**
     * CompiledFileManager is a file manager used to store and manage the compiled class file
     */
    private static class CompiledFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
        CompiledFileManager(StandardJavaFileManager standardJavaFileManager) {
            super(standardJavaFileManager);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, StringFileObject.Kind kind, FileObject sibling) {
            return new CompiledFileObject(className, Configuration.JOB_COMPILE_PATH, kind);
        }
    }

    /**
     * Execute the compiled class file
     * @param className class name to be executed
     */
    private static void execute(String className) throws MalformedURLException, ClassNotFoundException {
        URLClassLoader classLoader = URLClassLoader.newInstance(new URL[]{new File(Configuration.JOB_COMPILE_PATH).toURI().toURL()}); // ClassLoader used to load the compiled class file
        Class<?> cls = Class.forName(className, true, classLoader);
        try {
            Method main = cls.getMethod("main", String[].class);
            String[] args = new String[0];
            main.invoke(null, (Object) args);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(className + " does not have a main method", e);
        }
    }

    /**
     * Extract class name from code
     * @param code code
     * @return class name
     */
    public static String extractClassName(String code) {
        CompilationUnit cu = StaticJavaParser.parse(code);
        for (TypeDeclaration<?> type : cu.getTypes()) {
            if (type.isClassOrInterfaceDeclaration()) {
                if (type.isPublic()) {
                    return type.getNameAsString();
                }
            }
        }
        return null;
    }

    //     For testing purpose
//    public static void main(String[] args) throws Exception {
//        String code = "public class Main { public static void main(String[] args) { System.out.println(\"Hello, World\"); }}";
//        compile(code, extractClassName(code));
//        execute(extractClassName(code));
//    }
}