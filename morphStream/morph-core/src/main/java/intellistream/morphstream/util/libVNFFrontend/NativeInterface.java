package intellistream.morphstream.util.libVNFFrontend;
// import intellistream.morphstream.api.input.InputSource;

public class NativeInterface {

    // Delegate EventRequestInsertion.
    // private static InputSource IS = new InputSource();
    // public static void __insert_input_source(String input){
    //     IS.insertInputData(input);
    // }

    // Native method declaration for __init_SFC
    public native String __init_SFC(int argc, String[] argv);

    // Native method declaration for __VNFThread
    public native void __VNFThread(int argc, String[] argv);

    // Native method declaration for _callBack
    public static native byte[] _execute_sa_udf(long txnReqId, int saFlag, byte[] saData, int length);

    // Native method declaration for __handle_done
    public static native int __txn_finished(long txnID); //TODO: This should be txnID (or packet ID)

    // Load the native library when the class is initialized
    static {
        System.load("/home/shuhao/DB4NFV/tmp/SL-kernel-dynamic.so");
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
