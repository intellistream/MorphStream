package common.param.ed.tc;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class TCEvent extends TxnEvent {
    private final String wordValue;
    private int windowSize;
    private int windowCount;
    public double tfIdf;
    private final SchemaRecordRef word_record = new SchemaRecordRef();
    public String[] tweetIDList;
    public boolean isBurst;

    public TCEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String wordValue) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.wordValue = wordValue;
    }

    public String getWord() {return this.wordValue;}

    public int getWindowSize() {return this.windowSize;}

    public int getWindowCount() {return this.windowCount;}

    public SchemaRecordRef getWord_record() {return this.word_record;}

    public TCEvent cloneEvent() {
        return new TCEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, wordValue);
    }
}
