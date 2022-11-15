package common.param.ed.cu;

import common.param.TxnEvent;

import java.util.Arrays;

public class CUEvent extends TxnEvent {
    private final long tweetID;
    private final boolean isBurst;

    public CUEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, long tweetID, boolean isBurst) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.tweetID = tweetID;
        this.isBurst = isBurst;
    }

    public long getTweetID() {
        return this.tweetID;
    }

    public boolean isBurst() {
        return isBurst;
    }

    public CUEvent cloneEvent() {
        return new CUEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, tweetID, isBurst);
    }
}
