package intellistream.morphstream.api.input;

import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CacheCCManager {
    private final int CACHE_PORT = 12002;

    public void initialize() throws IOException {

        LinkedBlockingQueue<byte[]> messageQueue = new LinkedBlockingQueue<>();
        Thread listenerThread = new Thread(new SocketListener(messageQueue, CACHE_PORT));
        Thread processorThread = new Thread(new CacheInputProcessor(messageQueue));

        listenerThread.start();
        processorThread.start();
    }

}
