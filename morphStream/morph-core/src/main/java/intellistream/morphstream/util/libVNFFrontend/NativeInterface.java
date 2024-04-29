package intellistream.morphstream.util.libVNFFrontend;


import intellistream.morphstream.api.launcher.MorphStreamEnv;

public class NativeInterface {
    private static final boolean serveRemoteVNF = MorphStreamEnv.get().configuration().getBoolean("serveRemoteVNF");

    // Native method declaration for __init_SFC
    public native String __init_SFC(int argc, String[] argv);

    // Native method declaration for _callBack
    public static native byte[] _execute_sa_udf(long txnReqId, int saIndex, byte[] saData, int length);

    // Load the native library when the class is initialized
    static {
        if (serveRemoteVNF) {
            System.load("/home/kailian/DB4NFV/tmp/lb-kernel-dynamic.so");
        }
    }

    public static void main(String[] args) {
        // The test simulating the conditions.
		NativeInterface lf = new NativeInterface();

        String def = lf.__init_SFC(0, null);
        System.out.println(def);
        // TODO: Dispose definition of apps.

//        System.out.println("VNFs spawned.");
//        lf.__VNFThread(0, null);
//
//        System.out.println("VNFs ends.");
    }
}
