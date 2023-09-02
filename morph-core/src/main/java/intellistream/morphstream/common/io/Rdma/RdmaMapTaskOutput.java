package intellistream.morphstream.common.io.Rdma;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RdmaMapTaskOutput {
    public static final int ENTRY_SIZE = 8 + 4 + 4;
    // Only address and key. For map output we don't need a length
    public static final int MAP_ENTRY_SIZE = 8 + 4;
    private final int startPartitionId;
    private final int lastPartitionId;
    private final int numPartitions;
    private final int size;

    private final RdmaBufferManager bufferManager;
    private final RdmaBuffer rdmaBuffer;
    private final ByteBuffer byteBuffer;
    public RdmaMapTaskOutput(int startPartitionId, int lastPartitionId) throws IOException {
        this.startPartitionId = startPartitionId;
        this.lastPartitionId = lastPartitionId;
        this.numPartitions = lastPartitionId - startPartitionId + 1;
        this.size = numPartitions * ENTRY_SIZE;

        if (startPartitionId > lastPartitionId) {
            throw new IllegalArgumentException("Reduce partition range must be positive");
        }

//        SparkEnv sparkEnv = SparkEnv.get();
//        this.bufferManager = (RdmaShuffleManager) sparkEnv.shuffleManager();
        //TODO: add SparkEnv
        this.bufferManager = null;
        this.rdmaBuffer = bufferManager.get(size);
        this.byteBuffer = rdmaBuffer.getByteBuffer();
    }
    public int getNumPartitions() {
        return numPartitions;
    }

    public int getSize() {
        return size;
    }

    public RdmaBuffer getRdmaBuffer() {
        return rdmaBuffer;
    }
    public RdmaBlockLocation getRdmaBlockLocation(int requestedId) {
        if (requestedId < startPartitionId || requestedId > lastPartitionId) {
            throw new IndexOutOfBoundsException("PartitionId " + requestedId + " is out of range (" +
                    startPartitionId + "-" + lastPartitionId + ")");
        }

        int offset = (requestedId - startPartitionId) * ENTRY_SIZE;
        return new RdmaBlockLocation(
                byteBuffer.getLong(offset),
                byteBuffer.getInt(offset + 8),
                byteBuffer.getInt(offset + 8 + 4)
        );
    }
    public ByteBuffer getByteBuffer(int firstRequestedId, int lastRequestedId) {
        if (firstRequestedId < startPartitionId ||
                lastRequestedId > lastPartitionId ||
                firstRequestedId > lastRequestedId) {
            throw new IndexOutOfBoundsException("StartPartitionId " + firstRequestedId +
                    ", LastPartitionId " + lastRequestedId + " are out of range (" + startPartitionId +
                    "-" + lastPartitionId + ")");
        }

        int startPosition = (firstRequestedId - startPartitionId) * ENTRY_SIZE;
        int endPosition = startPosition + ((lastRequestedId - firstRequestedId + 1) * ENTRY_SIZE);
        ByteBuffer duplicateBuffer = byteBuffer.duplicate();
        duplicateBuffer.position(startPosition).limit(endPosition);
        return duplicateBuffer;
    }

    private void putInternal(int partitionId, long address, int length, int mKey) {
        int offset = (partitionId - startPartitionId) * ENTRY_SIZE;
        byteBuffer.putLong(offset, address);
        byteBuffer.putInt(offset + 8, length);
        byteBuffer.putInt(offset + 8 + 4, mKey);
    }
    public void put(int requestedId, long address, int length, int mKey) {
        if (requestedId < startPartitionId || requestedId > lastPartitionId) {
            throw new IndexOutOfBoundsException("PartitionId " + requestedId + " is out of range (" +
                    startPartitionId + "-" + lastPartitionId + ")");
        }

        putInternal(requestedId, address, length, mKey);
    }
}
