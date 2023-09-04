package intellistream.morphstream.common.io.Rdma.Shuffle.Handle;

import intellistream.morphstream.common.io.Rdma.Shuffle.ShuffleDependency;

public class RdmaBaseShuffleHandle extends ShuffleHandle {
    public final long driverTableAddress;
    public final int driverTableLength;
    public final int driverTableRKey;
    public final int numMaps;
    public final ShuffleDependency shuffleDependency;
    public RdmaBaseShuffleHandle(int shuffleId, long driverTableAddress, int driverTableLength, int driverTableRKey, int numMaps, ShuffleDependency shuffleDependency) {
        super(shuffleId);
        this.driverTableAddress = driverTableAddress;
        this.driverTableLength = driverTableLength;
        this.driverTableRKey = driverTableRKey;
        this.numMaps = numMaps;
        this.shuffleDependency = shuffleDependency;
    }
}
