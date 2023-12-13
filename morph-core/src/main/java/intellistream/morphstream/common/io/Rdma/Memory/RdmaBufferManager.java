package intellistream.morphstream.common.io.Rdma.Memory;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.ExecutorsServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class RdmaBufferManager {
    private static final Logger logger = LoggerFactory.getLogger(RdmaBufferManager.class);
    private IbvPd pd;
    private static final int MIN_BLOCK_SIZE = 16 * 1024;
    private final int minimumAllocationSize;
    private long maxCacheSize;
    private CircularRdmaBuffer circularRdmaBuffer;//Receive Events for worker and Receive Results for driver
    private ConcurrentHashMap<Integer, CircularRdmaBuffer> resultBufferMap = new ConcurrentHashMap<>();//workerId -> CircularRdmaBuffer
    private final ConcurrentHashMap<Integer, AllocatorStack> allocStackMap = new ConcurrentHashMap<>();//length -> bufferStack
    private static final ExecutorService executorService = ExecutorsServiceContext.getInstance();
    public RdmaBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        this.pd = pd;
        this.minimumAllocationSize = Math.min(1024, MIN_BLOCK_SIZE);
        this.maxCacheSize = conf.maxBufferAllocationSize();
    }
    public void perAllocateCircularRdmaBuffer(int length, int totalThreads) throws Exception {
        if (circularRdmaBuffer == null) {
            circularRdmaBuffer = new CircularRdmaBuffer(getPd(), length, totalThreads);
        }
    }
    public void perAllocateResultBuffer(int totalWorkers, int length, int totalThreads) throws Exception {
        for (int i = 0; i < totalWorkers; i++) {
            if (resultBufferMap.get(i) == null) {
                resultBufferMap.put(i, new CircularRdmaBuffer(getPd(), length, totalThreads));
            }
        }
    }

    public RdmaBuffer getDirect(int length) throws Exception {
        return new RdmaBuffer(getPd(), length);
    }
    public RdmaBuffer get(int length) throws Exception {
        // Round up length to the nearest power of two, or the minimum block size
        if (length < minimumAllocationSize) {
            length = minimumAllocationSize;
        }
        return getOrCreateAllocatorStack(length).get();
    }
    private AllocatorStack getOrCreateAllocatorStack(int length) {
        AllocatorStack allocatorStack = allocStackMap.get(length);
        if (allocatorStack == null) {
            allocStackMap.putIfAbsent(length, new AllocatorStack(getPd(), length));
            allocatorStack = allocStackMap.get(length);
        }
        return allocatorStack;
    }
    public void preAllocate(int buffSize, int buffCount) throws IOException {
        long totalSize = ((long) buffCount) * buffSize;
        // Disni uses int for length, so we can only allocate and register up to 2GB in a single call
        if (totalSize > (Integer.MAX_VALUE - 1)) {
            int numAllocs = (int)(totalSize / Integer.MAX_VALUE) + 1;
            for (int i = 0; i < numAllocs; i++) {
                getOrCreateAllocatorStack(buffSize).preallocate(buffCount / numAllocs);
            }
        } else {
            getOrCreateAllocatorStack(buffSize).preallocate(buffCount);
        }
    }
    public void put(RdmaBuffer buf) {

    }
    public IbvPd getPd() { return this.pd; }
    public CircularRdmaBuffer getCircularRdmaBuffer() {
        return circularRdmaBuffer;
    }
    public ConcurrentHashMap<Integer, CircularRdmaBuffer> getResultBufferMap() {
        return resultBufferMap;
    }
    public CircularRdmaBuffer getResultBuffer(int workerId) {
        return resultBufferMap.get(workerId);
    }
    public void stop() {
        logger.info("Rdma buffers allocation statistics:");
        for (Integer size : allocStackMap.keySet()) {
            AllocatorStack allocatorStack = allocStackMap.remove(size);
            if (allocatorStack != null) {
                logger.info( "Pre allocated {}, allocated {} buffers of size {} KB",
                        allocatorStack.getTotalPreAllocs(), allocatorStack.getTotalAlloc(), (size / 1024));
                allocatorStack.close();
            }
        }

        executorService.shutdown();
    }
}
