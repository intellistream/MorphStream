package intellistream.morphstream.api.input.simVNF;

import java.util.HashMap;

public class VNFManager {
    private int ccStrategy = 2;
    private int stateStartID = 0;
    private int parallelism;
    private int stateRange;
    private int requestCounter;
    private String patternString = "loneOperative";
//    private String pattern = "sharedReaders";
//    private String pattern = "sharedWriters";
//    private String pattern = "mutualInteractive";
    private static HashMap<Integer, VNFSenderThread> senderMap = new HashMap<>();
    private static HashMap<Integer, Thread> senderThreadMap = new HashMap<>();
    private static HashMap<Integer, VNFReceiverThread> receiverMap = new HashMap<>();
    private static HashMap<Integer, Thread> receiverThreadMap = new HashMap<>();

    public VNFManager(int requestCounter, int parallelism, int stateRange, int ccStrategy, int pattern) {
        this.requestCounter = requestCounter;
        this.parallelism = parallelism;
        this.stateRange = stateRange;
        this.ccStrategy = stateStartID;
        this.patternString = patternTranslator(pattern);

        for (int i = 0; i < parallelism; i++) {
            String csvFilePath = String.format("morphStream/scripts/nfvWorkload/pattern_files/%s/instance_%d.csv", patternString, i);
            int stateGap = stateRange / parallelism;
            VNFSenderThread sender = new VNFSenderThread(i, ccStrategy,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange, csvFilePath);
            Thread senderThread = new Thread(sender);
            senderMap.put(i, sender);
            senderThreadMap.put(i, senderThread);

            VNFReceiverThread receiver = new VNFReceiverThread(i, requestCounter/parallelism);
            Thread receiverThread = new Thread(receiver);
            receiverMap.put(i, receiver);
            receiverThreadMap.put(i, receiverThread);
        }
    }

    public static VNFSenderThread getSender(int id) {
        return senderMap.get(id);
    }
    public static HashMap<Integer, VNFSenderThread> getSenderMap() {
        return senderMap;
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

    private String patternTranslator(int pattern) {
        switch (pattern) {
            case 0:
                return "loneOperative";
            case 1:
                return "sharedReaders";
            case 2:
                return "sharedWriters";
            case 3:
                return "mutualInteractive";
            default:
                return "invalid";
        }
    }

}
