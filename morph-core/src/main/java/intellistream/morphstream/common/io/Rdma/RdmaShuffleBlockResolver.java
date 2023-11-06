package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.common.io.Rdma.Shuffle.RW.Write.RdmaWrapperShuffleData;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaShuffleBlockResolver {
    private final ConcurrentHashMap<Integer, RdmaWrapperShuffleData> rdmaShuffleDataMap = new ConcurrentHashMap<>();

    public File getDataFile(int shuffleId, int mapId) {
        return null;
    }
    public List<InputStream> getLocalRdmaPartition(int shuffleId, int partitionId) throws IOException {
       return rdmaShuffleDataMap.get(shuffleId).getInputStreams(partitionId);
    }
}
