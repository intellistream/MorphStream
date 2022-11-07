package common.param.ed.tc;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class TCEvent extends TxnEvent {
    private final String wordId;
    public volatile SchemaRecordRef frequencyRecord = new SchemaRecordRef(); //number of occurrence in the current window
    public volatile SchemaRecordRef windowSizeRecord = new SchemaRecordRef(); //current window size
    public volatile SchemaRecordRef countOccurWindowRecord = new SchemaRecordRef(); //number of windows that the word has appeared
    public volatile SchemaRecordRef windowCountRecord = new SchemaRecordRef(); //total number of windows so far
    public int frequency;
    public int windowSize;
    public int countOccurWindow;
    public int windowCount;
    public double tfIdf;

    public TCEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String wordId) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.wordId = wordId;
    }

    public String getWordId() {return this.wordId;}

    public TCEvent cloneEvent() {
        return new TCEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, wordId);
    }
}
