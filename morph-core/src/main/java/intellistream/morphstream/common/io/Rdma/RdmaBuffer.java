package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.common.io.Unsafe.memory.UnsafeMemoryAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(RdmaBuffer.class);
    static final UnsafeMemoryAllocator unsafeAlloc = new UnsafeMemoryAllocator();

}
