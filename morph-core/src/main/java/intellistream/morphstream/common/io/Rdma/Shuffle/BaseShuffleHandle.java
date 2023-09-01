package intellistream.morphstream.common.io.Rdma.Shuffle;

public class BaseShuffleHandle extends ShuffleHandle {
    public ShuffleDependency shuffleDependency;
    public BaseShuffleHandle(int shuffleId, ShuffleDependency shuffleDependency) {
        super(shuffleId);
        this.shuffleDependency = shuffleDependency;
    }
}
