package intellistream.morphstream.common.io.Rdma.Memory.Buffer;

import com.ibm.disni.verbs.IbvPd;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class AllocatorStack {
    private final AtomicInteger totalAlloc = new AtomicInteger(0);
    private final AtomicInteger preAllocs = new AtomicInteger(0);
    private final ConcurrentLinkedDeque<RdmaBuffer> stack = new ConcurrentLinkedDeque<>();
    private final int length;
    private long lastAccess;
    private IbvPd pd;
    private final AtomicLong idleBuffersSize = new AtomicLong(0);
    public AllocatorStack(IbvPd ibvPd, int length) {
        this.pd = ibvPd;
        this.length = length;
    }
    public int getTotalAlloc() {
        return totalAlloc.get();
    }
    public int getTotalPreAllocs() {
        return preAllocs.get();
    }
    public void preallocate(int numBuffers) throws IOException {
        RdmaBuffer[] preAllocatedBuffers = RdmaBuffer.preAllocate(getPd(), length, numBuffers);
        for (int i = 0; i < numBuffers; i++) {
            put(preAllocatedBuffers[i]);
            preAllocs.getAndIncrement();
        }
    }
    public RdmaBuffer get() throws Exception {
        lastAccess = System.nanoTime();
        RdmaBuffer rdmaBuffer = stack.pollFirst();
        if (rdmaBuffer == null) {
            totalAlloc.getAndIncrement();
            return new RdmaBuffer(getPd(), length);
        } else {
            idleBuffersSize.addAndGet(-length);
            return rdmaBuffer;
        }
    }
    public void put(RdmaBuffer rdmaBuffer) {
        rdmaBuffer.clean();
        lastAccess = System.nanoTime();
        stack.addLast(rdmaBuffer);
        idleBuffersSize.addAndGet(length);
    }
    public void close() {
        while (!stack.isEmpty()) {
            RdmaBuffer rdmaBuffer = stack.poll();
            if (rdmaBuffer != null) {
                rdmaBuffer.free();
            }
        }
    }
    private IbvPd getPd() throws IOException {
        return pd;
    }
}
