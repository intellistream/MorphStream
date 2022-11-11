package common.param.ed.es;

import common.param.TxnEvent;

import java.util.Arrays;

public class ESEvent extends TxnEvent {
    private final long clusterID;

    public ESEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, long clusterID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.clusterID = clusterID;
    }

    public long getClusterID() {
        return this.clusterID;
    }

    public ESEvent cloneEvent() {
        return new ESEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, clusterID);
    }
}
