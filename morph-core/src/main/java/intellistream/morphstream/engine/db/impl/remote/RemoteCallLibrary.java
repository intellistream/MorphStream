package intellistream.morphstream.engine.db.impl.remote;

public class RemoteCallLibrary {
    static {
        System.load(System.getProperty("java.library.path") + "/RemoteCallLibrary.so");
    }
    public native long init();
    public native boolean connect();
    public native void write(String tableName, String key, int workerId);
    public native int read(String tableName, String key);
}
