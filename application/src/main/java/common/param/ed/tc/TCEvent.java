package common.param.ed.tc;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

import static common.CONTROL.tweetWindowSize;
import static common.CONTROL.wordWindowSize;

public class TCEvent extends TxnEvent {
    private final String wordID;
    private final int windowSize;
    private final int currWindow;
    private final SchemaRecordRef word_record = new SchemaRecordRef();
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;
    public String[] tweetIDList;
    public boolean isBurst;

    public TCEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String wordID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.wordID = wordID;
        this.windowSize = wordWindowSize;
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

    public String getWordID() {
        return this.wordID;
    }

    public int getWindowSize() {
        return this.windowSize;
    }

    public int getWindowCount() {
        return this.currWindow;
    }

    public SchemaRecordRef getWord_record() {
        return this.word_record;
    }

    public TCEvent cloneEvent() {
        return new TCEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                wordID);
    }
}
