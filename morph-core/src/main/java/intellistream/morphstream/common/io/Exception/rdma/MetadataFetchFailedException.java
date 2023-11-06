package intellistream.morphstream.common.io.Exception.rdma;

public class MetadataFetchFailedException extends RuntimeException{
    public int shuffleId;
    public int partitionId;
    public MetadataFetchFailedException(int shuffleId, int partitionId, String msg) {
        super(msg);
        this.partitionId = partitionId;
        this.shuffleId = shuffleId;
    }
}
