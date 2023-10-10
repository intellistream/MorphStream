package intellistream.morphstream.common.io.Rdma.Shuffle.Handle;

import java.io.Serializable;

public abstract class ShuffleHandle implements Serializable {
    public int shuffleId;
    public ShuffleHandle(int shuffleId) {
        this.shuffleId = shuffleId;
    }
}
