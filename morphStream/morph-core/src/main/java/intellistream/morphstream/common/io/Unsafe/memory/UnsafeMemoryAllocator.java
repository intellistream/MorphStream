package intellistream.morphstream.common.io.Unsafe.memory;


import intellistream.morphstream.common.io.Unsafe.Platform;

/**
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

    @Override
    public MemoryBlock allocate(long size) throws OutOfMemoryError {
        long address = Platform.allocateMemory(size);
        MemoryBlock memory = new MemoryBlock(null, address, size);
        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_CLEAN_VALUE);
        }
        return memory;
    }

    @Override
    public void free(MemoryBlock memory) {
        assert (memory.obj == null) :
                "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
        assert (memory.pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) :
                "page has already been freed";
        assert ((memory.pageNumber == MemoryBlock.NO_PAGE_NUMBER)
                || (memory.pageNumber == MemoryBlock.FREED_IN_TMM_PAGE_NUMBER)) :
                "TMM-allocated pages must be freed via TMM.freePage(), not directly in allocator free()";

        if (MemoryAllocator.MEMORY_DEBUG_FILL_ENABLED) {
            memory.fill(MemoryAllocator.MEMORY_DEBUG_FILL_FREED_VALUE);
        }
        Platform.freeMemory(memory.offset);
        // As an additional layer of defense against use-after-free bugs, we mutate the
        // MemoryBlock to reset its pointer.
        memory.offset = 0;
        // Mark the page as freed (so we can detect double-frees).
        memory.pageNumber = MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER;
    }
}
