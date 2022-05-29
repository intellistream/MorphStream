package common.param.ed.wu;

import common.param.TxnEvent;

public class WUEvent extends TxnEvent {
    private final int tweetID;
    private final String word;

    public WUEvent(int tweetID, String word,
                   int partition_id, long[] bid_array, long bid, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.tweetID = tweetID;
        this.word = word;
    }
}
