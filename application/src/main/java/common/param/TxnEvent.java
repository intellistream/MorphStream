package common.param;

public class TxnEvent {
    protected final long bid;//as msg id.
    protected final int pid;
    protected final long[] bid_array;
    protected final int number_of_partitions;
    public double[] index_time = new double[1];
    public double[] enqueue_time = new double[1];
    public int[] success;
    //embeded state.
    protected long timestamp;//emit timestamp

    public TxnEvent(long bid, int partition_id, String bid_array, int number_of_partitions) {
        this.bid = bid;
        this.pid = partition_id;
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new long[bid_arrays.length];
        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Long.parseLong(bid_arrays[i].trim());
        }
        this.number_of_partitions = number_of_partitions;
        success = new int[1];

    }

    public TxnEvent(long bid, int partition_id, long[] bid_array, int number_of_partitions) {
        this.bid = bid;
        this.pid = partition_id;
        this.bid_array = bid_array;
        this.number_of_partitions = number_of_partitions;
        success = new int[1];

    }

    public long getBid() {
        return bid;
    }

    public int getPid() {
        return pid;
    }

    public int num_p() {
        return number_of_partitions;
    }

    public long[] getBid_array() {
        return bid_array;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
