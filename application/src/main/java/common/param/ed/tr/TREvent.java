package common.param.ed.tr;

import common.param.TxnEvent;

public class TREvent extends TxnEvent {
    private final String tweetValue;

    public TREvent(String tweetValue,
                   int partition_id, long[] bid_array, long bid, int number_of_partitions) {
        super(bid, partition_id, bid_array, number_of_partitions);
        this.tweetValue = tweetValue;
    }
}
