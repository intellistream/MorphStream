package intellistream.morphstream.api.input.simVNF;

import java.util.HashMap;

public class SimVNF {
    public static HashMap<Integer, VNFSenderThread> vnfSenderThreadMap = new HashMap<>();
    public static HashMap<Integer, VNFReceiverThread> vnfReceiverThreadMap = new HashMap<>();
    private int parallelism = 4;
    private int ccStrategy = 0;
    private int stateStartID = 0;
    private int stateGap = 2500;
    private String csvFilePath = "data.csv";
//    int instanceID, int ccStrategy, int stateStartID, int stateEndID, String csvFilePath

    public SimVNF() {
        for (int i = 0; i < parallelism; i++) {
            VNFSenderThread vnfSenderThread = new VNFSenderThread(i, ccStrategy, stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, csvFilePath);
            vnfSenderThreadMap.put(i, vnfSenderThread);
            VNFReceiverThread vnfReceiverThread = new VNFReceiverThread(i);
            vnfReceiverThreadMap.put(i, vnfReceiverThread);
        }
    }

    public void startSimVNF() {
        for (int i = 0; i < parallelism; i++) {
            vnfSenderThreadMap.get(i).run();
            vnfReceiverThreadMap.get(i).run();
        }
    }

}
