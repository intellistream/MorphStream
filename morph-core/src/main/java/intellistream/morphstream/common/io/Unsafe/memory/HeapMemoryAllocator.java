package intellistream.morphstream.common.io.Unsafe.memory;


import intellistream.morphstream.common.io.Unsafe.Platform;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

/**
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {
    private final Map<Long, LinkedList<WeakReference<long[]>>> bufferPoolsBySize = new HashMap<>();

    private static final int POOLING_THRESHOLD_BYTES = 1024 * 1024;

    /**
     * Returns true if allocations of the given size should go through the pooling mechanism and
     * false otherwise.
     */
    private boolean shouldPool(long size) {
        // Very small allocations are less likely to benefit from pooling.
        return size >= POOLING_THRESHOLD_BYTES;
    }

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        int numWords = (int) ((size + 7) / 8);
        long alignedSize = numWords * 8L;
        assert (alignedSize >= size);
        if (shouldPool(alignedSize)) {
            synchronized (this) {
                final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
                if (pool != null) {
                    while (!pool.isEmpty()) {
                        final WeakReference<long[]> arrayReference = pool.pop();
                        final long[] array = arrayReference.get();
                        if (array != null) {
                            assert (array.length * 8L >= size);
                            MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
                            if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
                                memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
                            }
                            return memory;
                        }
                    }
                    bufferPoolsBySize.remove(alignedSize);
                }
            }
        }
        long[] array = new long[numWords];
        MemoryBlock memory = new MemoryBlock(array, Platform.LONG_ARRAY_OFFSET, size);
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
        }
        return memory;
    }

    @Override
    public void free(MemoryBlock memory) {
        assert (memory.obj != null) :
                "baseObject was null; are you trying to use the on-heap allocator to free off-heap memory?";
        assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
                "page has already been freed";
        assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
                || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
                "TMM-allocated pages must first be freed via TMM.freePage(), not directly in allocator " +
                        "free()";

        final long size = memory.size();
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
        }

        // Mark the page as freed (so we can detect double-frees).
        memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;

        // As an additional layer of defense against use-after-free bugs, we mutate the
        // MemoryBlock to null out its reference to the long[] array.
        long[] array = (long[]) memory.obj;
        memory.setObjAndOffset(null, 0);

        long alignedSize = ((size + 7) / 8) * 8;
        if (shouldPool(alignedSize)) {
            synchronized (this) {
                LinkedList<WeakReference<long[]>> pool =
                        bufferPoolsBySize.computeIfAbsent(alignedSize, k -> new LinkedList<>());
                pool.add(new WeakReference<>(array));
            }
        }
    }
}