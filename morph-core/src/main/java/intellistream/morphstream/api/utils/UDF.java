package intellistream.morphstream.api.utils;

public class UDF {
    private final String className;
    private final String methodName;

    public UDF(String className, String methodName) {
        this.className = className;
        this.methodName = methodName;
    }

    public String getClassName() {
        return className;
    }

    public String getMethodName() {
        return methodName;
    }
}
