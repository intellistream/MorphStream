package intellistream.morphstream.common.io.Rdma;

import com.ibm.disni.util.NativeAffinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaThread.class);
    private RdmaChannel rdmaChannel;
    private final int cpuVector;
    private final Thread thread = new Thread(this,"RdmaChannel CQ processing thread");
    private final AtomicBoolean runThread = new AtomicBoolean(false);
    RdmaThread(RdmaChannel rdmaChannel, int cpuVector) {
        this.rdmaChannel = rdmaChannel;
        this.cpuVector = cpuVector;
        thread.setDaemon(true);
    }
    synchronized void start() {
        runThread.set(true);
        thread.start();
    }
    synchronized void stop() throws InterruptedException {
        if (runThread.getAndSet(false)) { thread.join(); }
    }
    @Override
    public void run() {
        long affinity = 1L << cpuVector;
        NativeAffinity.setAffinity(affinity);

        boolean isStillProcessing = false;
        while (runThread.get() || isStillProcessing) {
            try {
                isStillProcessing = rdmaChannel.processCompletions();
            } catch (IOException e) {
                LOG.error("Exception in RdmaThread, aborting: " + e);
                runThread.getAndSet(false);
                isStillProcessing = false;
            }
        }
    }
}
