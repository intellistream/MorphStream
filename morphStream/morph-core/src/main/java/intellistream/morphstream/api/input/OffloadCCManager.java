package intellistream.morphstream.api.input;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;


public class OffloadCCManager {
    private static final int writeThreadPoolSize = 4; //TODO: Hardcoded
    private static final int OFFLOAD_PORT = 12003;

    public void initialize() throws IOException {
        LinkedBlockingQueue<byte[]> messageQueue = new LinkedBlockingQueue<>();
        Thread listenerThread = new Thread(new SocketListener(messageQueue, OFFLOAD_PORT));
        Thread processorThread = new Thread(new OffloadInputProcessor(messageQueue, writeThreadPoolSize));

        listenerThread.start();
        processorThread.start();
    }

}
