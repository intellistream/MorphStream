package application.util;

public class JavaUtils {
    public static boolean isJar() {
        String className = JavaUtils.class.getName().replace('.', '/');
        String classJar = JavaUtils.class.getResource("/" + className + ".class").toString();

        return classJar.startsWith("jar:");
    }
}
