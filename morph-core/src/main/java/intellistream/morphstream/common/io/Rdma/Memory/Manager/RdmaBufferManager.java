package intellistream.morphstream.common.io.Rdma.Memory.Manager;

import com.ibm.disni.verbs.IbvPd;
import intellistream.morphstream.common.io.Rdma.Conf.RdmaChannelConf;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.AllocatorStack;
import intellistream.morphstream.common.io.Rdma.Memory.Buffer.RdmaBuffer;
import intellistream.morphstream.common.io.Rdma.RdmaUtils.ExecutorsServiceContext;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public abstract class RdmaBufferManager {
    protected static final Logger LOG = LoggerFactory.getLogger(RdmaBufferManager.class);
    @Getter
    protected IbvPd pd;
    protected static final int MIN_BLOCK_SIZE = 16 * 1024;
    protected final int minimumAllocationSize;
    protected long maxCacheSize;
    @Getter
    protected final ConcurrentHashMap<Integer, AllocatorStack> allocStackMap = new ConcurrentHashMap<>();//length -> bufferStack
    protected static final ExecutorService executorService = ExecutorsServiceContext.getInstance();
    public RdmaBufferManager(IbvPd pd, RdmaChannelConf conf) throws IOException {
        this.pd = pd;
        this.minimumAllocationSize = Math.min(10, MIN_BLOCK_SIZE);
        this.maxCacheSize = conf.maxBufferAllocationSize();
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
    public void put(RdmaBuffer rdmaBuffer) {

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
    public void stop() {
        LOG.info("Rdma buffers allocation statistics:");
        for (Integer size : allocStackMap.keySet()) {
            AllocatorStack allocatorStack = allocStackMap.remove(size);
            if (allocatorStack != null) {
                LOG.info( "Pre allocated {}, allocated {} buffers of size {} KB",
                        allocatorStack.getTotalPreAllocs(), allocatorStack.getTotalAlloc(), (size / 1024));
                allocatorStack.close();
            }
        }

        executorService.shutdown();
    }
}
