package intellistream.morphstream.common.io.Rdma.RdmaUtils.Block;

public abstract class BlockId {
    String name;//A globally unique identifier for this Block. Can be used for ser/de.
    public abstract boolean isEventBlock();

}
