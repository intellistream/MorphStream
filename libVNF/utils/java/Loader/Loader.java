package utils.java.Loader;
import utils.java.StateAccess.StateAccess;

public class Loader extends Thread {
	private String[] args;
	private StateAccess sa;
	static {
		System.loadLibrary("myVNF");
	}

	public Loader(String[] VNFRunningArgs){
		args = VNFRunningArgs;
	}

	public static void main(String[] args) {
		Loader appLoader = new Loader(null);
		appLoader.run();
	}

	// Working Thread to execute VNF thread.
	public native int WorkingThread(String[] args);

	// Announcing Stata Access callbacks.
	public native int TxnUDF(StateAccess sa);
	public native int PostTxnUDF(StateAccess sa);

	@Override
    public void run() {
		System.out.println("[Spawning a new VNF thread..]");
		this.WorkingThread(this.args);
    }	
}
