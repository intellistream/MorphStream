package intellistream.morphstream.common.io.Rdma.Shuffle.RW.Write;

import intellistream.morphstream.common.io.Rdma.ByteBufferBackedInputStream;
import intellistream.morphstream.common.io.Rdma.RdmaMappedFile;
import intellistream.morphstream.common.io.Rdma.RdmaShuffleManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaWrapperShuffleData {
    private final int shuffleId;
    private final int numPartitions;
    private final RdmaShuffleManager rdmaShuffleManager;
    private final ConcurrentHashMap<Integer, RdmaMappedFile> rdmaMappedFileByMapId = new ConcurrentHashMap<>();//MapId -> RdmaMappedFile
    public RdmaWrapperShuffleData(int shuffleId, int numPartitions, RdmaShuffleManager rdmaShuffleManager) {
        this.shuffleId = shuffleId;
        this.numPartitions = numPartitions;
        this.rdmaShuffleManager = rdmaShuffleManager;
    }
    public List<InputStream> getInputStreams(int partitionId) throws IOException {
        List<InputStream> inputStreams = new ArrayList<>();

        for (Map.Entry<Integer, RdmaMappedFile> entry : rdmaMappedFileByMapId.entrySet()) {
            ByteBuffer byteBuffer = entry.getValue().getByteBufferForPartition(partitionId);
            if (byteBuffer != null) {
                inputStreams.add(new ByteBufferBackedInputStream(byteBuffer));
            }
        }
        return inputStreams;
    }
    public void dispose() throws Exception {
        for (RdmaMappedFile rdmaMappedFile : rdmaMappedFileByMapId.values()) {
            rdmaMappedFile.dispose();
        }
    }
    public RdmaMappedFile getRdmaMappedFileForMapId(int mapId) {
        return rdmaMappedFileByMapId.get(mapId);
    }
    public void removeDataByMap(int mapId) throws IOException, InvocationTargetException, IllegalAccessException {
        RdmaMappedFile removed = rdmaMappedFileByMapId.remove(mapId);
        if (removed != null) {
            removed.dispose();
        }
    }
    public void writeIndexFileAndCommit(int mapId, long[] lengths, File dataTmp) throws IOException, InvocationTargetException, IllegalAccessException {
        File dataFile = rdmaShuffleManager.shuffleBlockResolver.getDataFile(shuffleId, mapId);
        synchronized (this) {
            if (dataFile.exists()) {
                if (!dataFile.delete()) {
                    throw new IOException("fail to delete file " + dataFile);
                }
            }
            if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
                throw new IOException("fail to rename file from " + dataTmp + " to " + dataFile);
            }

            RdmaMappedFile rdmaFile = new RdmaMappedFile(dataFile, (int) rdmaShuffleManager.conf.shuffleReadBlockSize, lengths, rdmaShuffleManager.getRdmaBufferManager());
            RdmaMappedFile rdmaMappedFile = rdmaMappedFileByMapId.put(mapId, rdmaFile);
            if (rdmaMappedFile != null)
                rdmaMappedFile.dispose();
        }
    }
}
