package java.intellistream.chc;

/**
 * Native interface for CHC
 */
public class NativeInterface {
    // update cache instruction (callback) -> update cache
    public static native int __update_cache(int objKey, int value);

    // return the state (READ: value; WRITE: success or not)
    public static native void __return_state(int requestId, int value);
}
