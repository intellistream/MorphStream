package cli.libVNFFrontend;

public class Interface {
    // Native method declaration for __init_SFC
    public native String __init_SFC(int argc, String[] argv);

    // Native method declaration for __VNFThread
    public native void __VNFThread(int argc, String[] argv);

    // Native method declaration for _callBack
    public native int _callBack(long saReqId, byte[] value, int length);

    // Native method declaration for __handle_done
    public native int __handle_done(long saReqId);

    // Load the native library when the class is initialized
    static {
        System.load("/home/kailian/MorphStream/libVNF/build/libvnf-kernel-dynamic.so");
    }

    public static void main(String[] args) {
        // The test simulating the conditions.
		Interface lf = new Interface();

        String def = lf.__init_SFC(0, null);
        System.out.println(def);
        // TODO: Dispose definition of apps.

        System.out.println("VNFs spawned.");
        lf.__VNFThread(0, null);

        System.out.println("VNFs ends.");
    }
}