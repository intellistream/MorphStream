package intellistream.morphstream.common.io.Rdma;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class RdmaThread implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaThread.class);
    private RdmaChannel rdmaChannel;
    private final Thread thread = new Thread(this,"RdmaChannel CQ processing thread");
    private final AtomicBoolean runThread = new AtomicBoolean(false);
    @Override
    public void run() {

    }
}
