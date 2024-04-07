package intellistream.morphstream.api.input;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;


public class PatternMonitor {
    /**
     * Below 4 hashmaps store per-window pattern data for each state-tuple
     * */

    private static LinkedBlockingQueue<byte[]> txnMetaDataQueue;
    private static final int punctuation_interval = 100;

    private final int MONITOR_PORT = 12000;

    public void initialize() throws IOException {

        txnMetaDataQueue = new LinkedBlockingQueue<>();
        Thread listenerThread = new Thread(new SocketListener(txnMetaDataQueue, MONITOR_PORT));
        Thread processorThread = new Thread(new MonitorThread(txnMetaDataQueue, punctuation_interval));

        listenerThread.start();
        processorThread.start();
    }

}
