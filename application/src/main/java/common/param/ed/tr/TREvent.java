package common.param.ed.tr;

import common.param.TxnEvent;
import common.param.ed.tc.TCEvent;

import java.util.Arrays;

public class TREvent extends TxnEvent {
    private final long tweetID;
    private final String[] words;

    public TREvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, long tweetID, String[] words) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.tweetID = tweetID;
        this.words = words;
    }

    public long getTweetID() {
        return this.tweetID;
    }

    public String[] getWords() {
        return this.words;
    }

    public TREvent cloneEvent() {
        return new TREvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, tweetID, words);
    }
}
