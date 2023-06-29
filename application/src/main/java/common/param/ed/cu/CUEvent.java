package common.param.ed.cu;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import static common.CONTROL.tweetWindowSize;

import java.util.Arrays;

public class CUEvent extends TxnEvent {
    private final String tweetID;
    private final String clusterID;
    private final int currWindow;
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;
    public SchemaRecordRef clusterRecord = new SchemaRecordRef();

    public CUEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String tweetID, String clusterID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.tweetID = tweetID;
        this.clusterID = clusterID;
        this.currWindow = computeCurrWindow(bid);
    }

    private int computeCurrWindow(double bid) {
        return (int) bid / tweetWindowSize + 1;
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
    public String getTweetID() {
        return this.tweetID;
    }
    public String getClusterID() {
        return this.clusterID;
    }
    public int getCurrWindow() {return this.currWindow;}

    public CUEvent cloneEvent() {
        return new CUEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                tweetID, clusterID);
    }
}
