package intellistream.morphstream.common.io.Rdma;

import intellistream.morphstream.common.io.Rdma.Shuffle.RW.RdmaWrapperShuffleData;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

public class RdmaShuffleBlockResolver {
    private final ConcurrentHashMap<Integer, RdmaWrapperShuffleData> rdmaShuffleDataMap = new ConcurrentHashMap<>();

    public File getDataFile(int shuffleId, int mapId) {
        return null;
    }
}
