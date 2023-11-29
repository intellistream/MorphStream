package libVNFFrontend;

public class NativeInterface {

    //TODO: This native interface is copied from MorphStream & libVNF integration, modify it to fit OpenNFController's needs:
    /**
     * 1. Receive packets from libVNF (libVNF instance call OpenNFController.sendPktToController(pkt)), sending packet to controller queue. The packet carries state update actions.
     * 2. For each packet, after controller has determined IDs of instances sharing the packet, it calls instances deployed on libVNF to execute the state update actions.
     * */


    // Native method declaration for __init_SFC
    public native String __init_SFC(int argc, String[] argv);

    // Native method declaration for __VNFThread
    public native void __VNFThread(int argc, String[] argv);

    // Native method declaration for _callBack
    public static native int _execute_txn_udf(String txnID, byte[] value, int length);

    // Native method declaration for __handle_done
    public static native int __txn_finished(long txnID); //TODO: This should be txnID (or packet ID)

    // Load the native library when the class is initialized
    static {
        System.load("/home/kailian/MorphStream/libVNF/build/libvnf-kernel-dynamic.so");
    }

    public static void main(String[] args) {
        // The test simulating the conditions.
		NativeInterface lf = new NativeInterface();

        String def = lf.__init_SFC(0, null);
        System.out.println(def);
        // TODO: Dispose definition of apps.

        System.out.println("VNFs spawned.");
        lf.__VNFThread(0, null);

        System.out.println("VNFs ends.");
    }
}