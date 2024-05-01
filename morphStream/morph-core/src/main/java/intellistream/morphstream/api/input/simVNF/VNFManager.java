package intellistream.morphstream.api.input.simVNF;

import intellistream.morphstream.api.launcher.MorphStreamEnv;

import java.util.HashMap;
import java.util.concurrent.CyclicBarrier;

public class VNFManager {
    private int stateStartID = 0;
    private int parallelism;
    private int stateRange;
    private int totalRequests;
    private String patternString;
    private static HashMap<Integer, VNFSenderThread> senderMap = new HashMap<>();
    private static HashMap<Integer, Thread> senderThreadMap = new HashMap<>();
    private static HashMap<Integer, VNFReceiverThread> receiverMap = new HashMap<>();
    private static HashMap<Integer, Thread> receiverThreadMap = new HashMap<>();
    private int totalRequestCounter = 0;
    private long overallStartTime = Long.MAX_VALUE;
    private long overallEndTime = Long.MIN_VALUE;
    private static final boolean serveRemoteVNF = (MorphStreamEnv.get().configuration().getInt("serveRemoteVNF") != 0);

    public VNFManager(int totalRequests, int parallelism, int stateRange, int ccStrategy, int pattern) {
        this.totalRequests = totalRequests;
        this.parallelism = parallelism;
        this.stateRange = stateRange;
        this.patternString = patternTranslator(pattern);

        CyclicBarrier senderBarrier = new CyclicBarrier(parallelism);
        CyclicBarrier receiverBarrier = new CyclicBarrier(parallelism);
        String rootPath = MorphStreamEnv.get().configuration().getString("nfvWorkloadPath");

        for (int i = 0; i < parallelism; i++) {
            String csvFilePath = String.format(rootPath + "/%s/instance_%d.csv", patternString, i);
            int stateGap = stateRange / parallelism;
            VNFSenderThread sender = new VNFSenderThread(i, ccStrategy,
                    stateStartID + i * stateGap, stateStartID + (i + 1) * stateGap, stateRange, csvFilePath, senderBarrier);
            Thread senderThread = new Thread(sender);
            senderMap.put(i, sender);
            senderThreadMap.put(i, senderThread);

            VNFReceiverThread receiver = new VNFReceiverThread(i, totalRequests /parallelism, receiverBarrier);
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

    public void startVNFInstances() {
        for (int i = 0; i < parallelism; i++) {
            senderThreadMap.get(i).start();
            receiverThreadMap.get(i).start();
        }
    }

    public double joinVNFInstances() {
        for (int i = 0; i < parallelism; i++) {
            try {
                senderThreadMap.get(i).join();
                receiverThreadMap.get(i).join();
                totalRequestCounter += receiverMap.get(i).getActualRequestCount();
                overallStartTime = Math.min(overallStartTime, receiverMap.get(i).getStartTime());
                overallEndTime = Math.max(overallEndTime, receiverMap.get(i).getEndTime());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        long overallDuration = overallEndTime - overallStartTime;
        double overallThroughput = totalRequestCounter / (overallDuration / 1E9);
        return overallThroughput;
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
