package common.param.ed.es;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class ESEvent extends TxnEvent {
    private final String clusterID;
    public SchemaRecordRef cluster_record = new SchemaRecordRef();
    public boolean isEvent;
    public String[] wordList;
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;

    public ESEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions, String clusterID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.clusterID = clusterID;
    }

    public double getMyBid() {
        return myBid;
    }
    public int getMyPid() {
        return myPid;
    }
    public String getMyBidArray() {
        return my_bid_array;
    }
    public String getMyPartitionIndex() {
        return my_partition_index;
    }
    public int getMyNumberOfPartitions() {
        return my_number_of_partitions;
    }

    public String getClusterID() {
        return this.clusterID;
    }

    public ESEvent cloneEvent() {
        return new ESEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, clusterID);
    }
}
