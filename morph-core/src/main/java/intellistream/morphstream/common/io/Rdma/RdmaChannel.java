package intellistream.morphstream.common.io.Rdma;

import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

/*
Use single QP between two nodes. (Executors and Managers)
 */
public class RdmaChannel {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaChannel.class);
    private static final AtomicInteger idGenerator = new AtomicInteger(0);
    private final int id = idGenerator.getAndIncrement();
}
