package common.param.ed.wu;

import common.param.TxnEvent;
import static common.CONTROL.wordWindowSize;
import static common.CONTROL.tweetWindowSize;

import java.util.Arrays;

public class WUEvent extends TxnEvent {
    private final String word;
    private final String tweetID;
    private final int currWindow;
    private final int myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;

    public WUEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String word, String tweetID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.word = word;
        this.tweetID = tweetID;
        this.currWindow = computeCurrWindow(bid);
    }

    private int computeCurrWindow(int bid) {
        return bid / tweetWindowSize;
    }

    public int getMyBid() {
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

    public String getWord() {
        return this.word;
    }

    public String getTweetID() {
        return this.tweetID;
    }
    public int getCurrWindow() {return this.currWindow;}

    public WUEvent cloneEvent() {
        return new WUEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, word, tweetID);
    }
}
