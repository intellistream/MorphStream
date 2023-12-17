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

    public native static String __process_request(int instanceID, String request); //blocking, returns state update actions when instance finish processing the request

    public native static void __set_per_flow_state(int instanceID, String stateUpdate); //inform all other instances to update states

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