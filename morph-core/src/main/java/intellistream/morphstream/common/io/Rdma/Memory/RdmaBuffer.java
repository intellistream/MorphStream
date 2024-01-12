package intellistream.morphstream.common.io.Rdma.Memory;

import com.ibm.disni.verbs.IbvMr;
import com.ibm.disni.verbs.IbvPd;
import com.ibm.disni.verbs.SVCRegMr;
import intellistream.morphstream.common.io.Rdma.Msg.RegionToken;
import intellistream.morphstream.common.io.Unsafe.Platform;
import intellistream.morphstream.common.io.Unsafe.memory.MemoryBlock;
import intellistream.morphstream.common.io.Unsafe.memory.UnsafeMemoryAllocator;
import lombok.Getter;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The registered Memory Region includes information such as
 * the length,
 * starting address,
 * lkey,
 * and rkey of the Memory Region.
 */
public class RdmaBuffer {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(RdmaBuffer.class);
    private IbvMr ibvMr;//Memory Region
    @Getter
    private final long address;
    @Getter
    private final int length;
    private final MemoryBlock block;
    private AtomicInteger refCount;
    static final UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();
    public static final Constructor<?> directBufferConstructor;
    static {
        try {
            Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
            directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
            directBufferConstructor.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException("java.nio.DirectByteBuffer class not found");
        }
    }
    RdmaBuffer(IbvPd ibvPd, int length) throws Exception{
        block = unsafeAlloc.allocate((long)length);
        address = block.getBaseOffset();
        this.length = length;
        refCount = new AtomicInteger(1);
        clean();
        ibvMr = register(ibvPd, address, length);
    }
    private RdmaBuffer(IbvMr ibvMr, AtomicInteger refCount, long address, int length,
                       MemoryBlock block) {
        this.ibvMr = ibvMr;
        this.refCount = refCount;
        this.address = address;
        this.length = length;
        this.block = block;
    }
    private static IbvMr register(IbvPd ibvPd, long address, int length) throws IOException {
        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;

        SVCRegMr sMr = ibvPd.regMr(address, length, access).execute();
        IbvMr ibvMr = sMr.getMr();
        sMr.free();
        return ibvMr;
    }
    private void unregister() {
        if (ibvMr != null) {
            try {
                ibvMr.deregMr().execute().free();
            } catch (IOException e) {
                LOG.warn("Deregister MR failed");
            }
            ibvMr = null;
        }
    }
    public void clean() {
        Platform.setMemory(address, (byte)0, length);
    }
    void free() {
        if (refCount.decrementAndGet() == 0) {
            unregister();
            unsafeAlloc.free(block);
        }
    }

    public int getLkey() {return ibvMr.getLkey();}
    public ByteBuffer getByteBuffer(long address, int length) throws IOException {
        try {
            return (ByteBuffer) directBufferConstructor.newInstance(address, length);
        } catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
        }
    }
    public ByteBuffer getByteBuffer() throws IOException {
        try {
            return (ByteBuffer) directBufferConstructor.newInstance(getAddress(), getLength());
        } catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer exception: " + e.toString());
        }
    }
    /**
     * Pre allocates @numBlocks buffers of size @length under single MR.
     * @param ibvPd
     * @param length
     * @param numBlocks
     * @return
     * @throws IOException
     */
    public static RdmaBuffer[] preAllocate(IbvPd ibvPd, int length, int numBlocks)
            throws IOException {
        MemoryBlock block = unsafeAlloc.allocate(length * numBlocks);
        long baseAddress = block.getBaseOffset();
        IbvMr ibvMr = register(ibvPd, baseAddress, length * numBlocks);
        RdmaBuffer[] result = new RdmaBuffer[numBlocks];
        AtomicInteger refCount = new AtomicInteger(numBlocks);
        for (int i = 0; i < numBlocks; i++) {
            result[i] = new RdmaBuffer(ibvMr, refCount, baseAddress + i * length, length, block);
        }
        return result;
    }
    public RegionToken createRegionToken(){
        return new RegionToken(length, address, ibvMr.getLkey(), ibvMr.getRkey());
    }
}
