package common.param.ed.cu;

import common.param.TxnEvent;

import java.util.Arrays;

import static common.CONTROL.tweetWindowSize;

public class CUEvent extends TxnEvent {
    private final String tweetID;
    private final boolean isBurst;
    private final int currWindow;
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;

    public CUEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String tweetID, boolean isBurst) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.tweetID = tweetID;
        this.isBurst = isBurst;
        this.currWindow = computeCurrWindow(bid);
    }

    private int computeCurrWindow(double bid) {
        return (int) bid / tweetWindowSize;
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

    public int getCurrWindow() {
        return this.currWindow;
    }

    public boolean isBurst() {
        return isBurst;
    }

    public CUEvent cloneEvent() {
        return new CUEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                tweetID, isBurst);
    }
}
