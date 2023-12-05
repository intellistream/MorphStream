package libVNFFrontend;

import java.util.HashMap;

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

    public static native void __process_packet(String instanceID, String packet); //OpenNF controller calls libVNF instance to process the packet
    public static native HashMap<String, String> __get_instance_state(String instanceID); //TODO: OpenNF controller enters state-sharing mode and get latest states from instances

    // Retrieve the latest state for the instance (You need to implement this logic)
    public static native HashMap<String, String> __get_instance_state(String instanceID);

    // Additional methods for notifying packet processing completion and sending state updates
    public static native void __packet_processing_complete(String instanceID);

    // Send state update event to controller
    public static native void __send_state_update(String instanceID, String stateKey, String stateValue);

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