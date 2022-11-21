package common.param.ed.tc;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class TCEvent extends TxnEvent {
    private final String wordID;
    private final int windowSize;
    private final int currWindow;
    private final SchemaRecordRef word_record = new SchemaRecordRef();
    public String[] tweetIDList;
    public boolean isBurst;

    public TCEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String wordID, int windowSize) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.wordID = wordID;
        this.windowSize = windowSize;
        this.currWindow = computeCurrWindow(bid);
    }

    private int computeCurrWindow(int bid) {
        int windowSize = 50; //TODO: This is the tweetWindowSize
        return bid / windowSize;
    }

    public String getWordID() {return this.wordID;}

    public int getWindowSize() {return this.windowSize;}

    public int getWindowCount() {return this.currWindow;}

    public SchemaRecordRef getWord_record() {return this.word_record;}

    public TCEvent cloneEvent() {
        return new TCEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                wordID, windowSize);
    }
}
