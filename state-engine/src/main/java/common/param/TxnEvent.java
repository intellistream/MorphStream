package common.param;

import java.util.Arrays;

public class TxnEvent implements Comparable<TxnEvent> {
    protected final double bid;//as msg id.
    protected final int pid;
    protected double[] bid_array;
    protected final int number_of_partitions;
    public double[] enqueue_time = new double[1];
    public int[] success;
    //embeded state.
    protected long timestamp;//emit timestamp

    public int[] partition_indexs;

    public TxnEvent(double bid, int partition_id, String bid_array, String partition_index, int number_of_partitions) {
        this.bid = bid;
        this.pid = partition_id;
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new double[bid_arrays.length];
        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Double.parseDouble(bid_arrays[i].trim());
        }
        String[] partition_indexs = partition_index.substring(1, partition_index.length() - 1).split(",");
        this.partition_indexs = new int[partition_indexs.length];
        for (int i = 0; i < partition_indexs.length; i++) {
            this.partition_indexs[i] = Integer.parseInt(partition_indexs[i].trim());
        }
        this.number_of_partitions = number_of_partitions;
        success = new int[1];
    }

    public TxnEvent(double bid, int partition_id, String bid_array, int number_of_partitions) {
        this.bid = bid;
        this.pid = partition_id;
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new double[bid_arrays.length];
        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Double.parseDouble(bid_arrays[i].trim());
        }
        this.number_of_partitions = number_of_partitions;
        success = new int[1];
    }

    public TxnEvent(double bid, int partition_id, double[] bid_array, int number_of_partitions) {
        this.bid = bid;
        this.pid = partition_id;
        this.bid_array = bid_array;
        this.number_of_partitions = number_of_partitions;
        success = new int[1];
    }

    public TxnEvent(double bid) {
        this.bid = bid;
        this.pid = 0;
        number_of_partitions = 1;
        success = new int[1];
    }

    public double getBid() {
        return bid;
    }

    public int getPid() {
        return pid;
    }

    public int num_p() {
        return number_of_partitions;
    }

    public void setBid_array(String bid_array, String partition_index) {
        String[] bid_arrays = bid_array.substring(1, bid_array.length() - 1).split(",");
        this.bid_array = new double[bid_arrays.length];
        for (int i = 0; i < bid_arrays.length; i++) {
            this.bid_array[i] = Double.parseDouble(bid_arrays[i].trim());
        }
        String[] partition_indexs = partition_index.substring(1, partition_index.length() - 1).split(",");
        this.partition_indexs = new int[partition_indexs.length];
        for (int i = 0; i < partition_indexs.length; i++) {
            this.partition_indexs[i] = Integer.parseInt(partition_indexs[i].trim());
        }
    }

    public double[] getBid_array() {
        return bid_array;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public TxnEvent cloneEvent() {
        return new TxnEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions);
    }

    /**
     *
     * @param event
     * @return
     */
    @Override
    public int compareTo(TxnEvent event) {
        return Double.compare(this.bid, event.bid);
    }
}
