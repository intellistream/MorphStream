package intellistream.morphstream.engine.db.impl.remote;

public class RemoteCallLibrary {
    static {
        System.loadLibrary("RemoteCallLibrary");
    }
    private native long init();
    private native boolean connect();
    private native void write(String tableName, String key, int workerId);
    private native int read(String tableName, String key);
}
