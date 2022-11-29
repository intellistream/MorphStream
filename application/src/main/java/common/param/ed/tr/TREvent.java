package common.param.ed.tr;

import common.param.TxnEvent;

import java.util.Arrays;

public class TREvent extends TxnEvent {

    private final String tweetID;
    private final String[] words;
    private final int myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;

    public TREvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String tweetID, String[] words) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.tweetID = tweetID;
        this.words = words;
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

    public String getTweetID() {
        return tweetID;
    }

    public String[] getWords() {
        return this.words;
    }

    public TREvent cloneEvent() {
        return new TREvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, tweetID, words);
    }

    @Override
    public String toString() {
        return "TREvent (" + bid + ") {"
                + "tweetID=" + tweetID
                + ", words=" + Arrays.toString(words)
                + '}';
    }
}
