package intellistream.morphstream.common.io.Rdma.RdmaUtils.Block;

public class BlockManagerId {
    private String executorId;
    private String host;
    private int port;
    private static final int MAX_CACHE_SIZE = 10000;
    private BlockManagerId() {
        // For deserialization only
    }

    public BlockManagerId(String executorId, String host, int port) {
        this.executorId = executorId;
        this.host = host;
        this.port = port;
    }
    public String getExecutorId() {
        return executorId;
    }
    public String getHost() {
        return host;
    }
    public int getPort() {
        return port;
    }
    public boolean isDriver() {
        //return executorId.equals(SparkContext.DRIVER_IDENTIFIER);
        return executorId.equals("");
    }

}

