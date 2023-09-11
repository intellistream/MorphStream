package intellistream.morphstream.common.io.Rdma.RdmaUtils.Block;

public class EventBlockId extends BlockId {
    private int punctuationId;

    public EventBlockId(int punctuationId) {
        this.punctuationId = punctuationId;
    }

    @Override
    public boolean isEventBlock() {
        return true;
    }
}
