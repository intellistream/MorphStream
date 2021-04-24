package util;
import java.lang.instrument.Instrumentation;
class ObjectSizeFetcher {
    private static Instrumentation instrumentation;
    public static void premain(String args, Instrumentation inst) {
        instrumentation = inst;
    }
    public static long getObjectSize(Object o) {
        return instrumentation.getObjectSize(o);
    }
}
