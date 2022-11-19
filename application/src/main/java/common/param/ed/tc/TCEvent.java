package common.param.ed.tc;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class TCEvent extends TxnEvent {
    private final String wordID;
    private final int windowSize;
    private final int windowCount;
    private final SchemaRecordRef word_record = new SchemaRecordRef();
    public String[] tweetIDList;
    public boolean isBurst;

    public TCEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String wordID, int windowSize, int windowCount) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.wordID = wordID;
        this.windowSize = windowSize;
        this.windowCount = windowCount;
    }

    public String getWordID() {return this.wordID;}

    public int getWindowSize() {return this.windowSize;}

    public int getWindowCount() {return this.windowCount;}

    public SchemaRecordRef getWord_record() {return this.word_record;}

    public TCEvent cloneEvent() {
        return new TCEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                wordID, windowSize, windowCount);
    }
}
