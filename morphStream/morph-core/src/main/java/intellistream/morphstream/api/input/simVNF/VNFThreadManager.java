package intellistream.morphstream.api.input.simVNF;

import java.util.HashMap;

public class VNFThreadManager {
    private int parallelism = 4;
    private int ccStrategy = 2;
    private int stateStartID = 0;
    private int stateGap = 2500;
    private int stateRange = 10000;
    private String pattern = "loneOperative";
//    private String pattern = "sharedReaders";
//    private String pattern = "sharedWriters";
//    private String pattern = "mutualInteractive";
    private static HashMap<Integer, VNFSenderThread> senderMap = new HashMap<>();
    private static HashMap<Integer, Thread> senderThreadMap = new HashMap<>();
    private static HashMap<Integer, VNFReceiverThread> receiverMap = new HashMap<>();
    private static HashMap<Integer, Thread> receiverThreadMap = new HashMap<>();

    public VNFThreadManager() {
        for (int i = 0; i < parallelism; i++) {
            String csvFilePath = String.format("morphStream/scripts/nfvWorkload/pattern_files/%s/instance_%d.csv", pattern, i);
            VNFSenderThread sender = new VNFSenderThread(i, ccStrategy,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange, csvFilePath);
            Thread senderThread = new Thread(sender);
            senderMap.put(i, sender);
            senderThreadMap.put(i, senderThread);

            VNFReceiverThread receiver = new VNFReceiverThread(i);
            Thread receiverThread = new Thread(receiver);
            receiverMap.put(i, receiver);
            receiverThreadMap.put(i, receiverThread);
        }
    }

    public VNFSenderThread getSender(int id) {
        return senderMap.get(id);
    }

    public static VNFReceiverThread getReceiver(int id) {
        return receiverMap.get(id);
    }

    public void startSimVNF() {
        for (int i = 0; i < parallelism; i++) {
            senderThreadMap.get(i).start();
            receiverThreadMap.get(i).start();
        }
    }

}
