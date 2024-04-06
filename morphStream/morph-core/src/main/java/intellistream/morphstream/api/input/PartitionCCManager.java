package intellistream.morphstream.api.input;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class PartitionCCManager {
    private static HashMap<Integer, Integer> partitionOwnership; //Maps each state partition to its current owner VNF instance. The key labels partition start index.
    private static final int PARTITION_PORT = 12001;

    public PartitionCCManager() {
        partitionOwnership = new HashMap<>(); //TODO: Hardcoded
    }

    public void initialize() throws IOException {
        LinkedBlockingQueue<byte[]> messageQueue = new LinkedBlockingQueue<>();
        Thread listenerThread = new Thread(new SocketListener(messageQueue, PARTITION_PORT));
        Thread processorThread = new Thread(new PartitionInputProcessor(messageQueue, partitionOwnership));

        listenerThread.start();
        processorThread.start();
    }
}
