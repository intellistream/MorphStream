package intellistream.morphstream.common.io.Rdma.Shuffle.RW;

import intellistream.morphstream.common.io.Rdma.Shuffle.Handle.RdmaBaseShuffleHandle;

public class RdmaShuffleReader implements ShuffleReader {
    RdmaBaseShuffleHandle handle;
    int startPartition;
    int endPartition;

}
