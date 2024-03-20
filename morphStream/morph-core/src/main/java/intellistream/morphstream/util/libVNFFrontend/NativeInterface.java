package intellistream.morphstream.util.libVNFFrontend;
// import intellistream.morphstream.api.input.InputSource;

import java.util.HashMap;

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
    public static native int __txn_finished(long txnID);

    // Manager notifies VNF instances for pattern change: (1) pause further txn transmission to manager, (2) update CC strategy to instances
    // then, VNF instances should wait for manager to notify again for state movement completion
    public static native void __pause_txn_processing(HashMap<String, Integer> changedPatterns); //Maps tupleIDs whose pattern changed to their new patterns

    public static native void __get_states(); //TODO: Coordinate with VNF instances for state movement during CC switch
    public static native void __update_states();

    // Manager notifies VNF instances to resume normal txn processing.
    public static native void __resume_txn_processing();

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
