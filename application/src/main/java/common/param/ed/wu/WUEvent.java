package common.param.ed.wu;

import common.param.TxnEvent;

import java.util.Arrays;

public class WUEvent extends TxnEvent {
    private final String word;
    private final long tweetID;

    public WUEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String word, long tweetID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.word = word;
        this.tweetID = tweetID;
    }

    public String getWord() {
        return this.word;
    }

    public long getTweetID() {
        return this.tweetID;
    }

    public WUEvent cloneEvent() {
        return new WUEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, word, tweetID);
    }
}
