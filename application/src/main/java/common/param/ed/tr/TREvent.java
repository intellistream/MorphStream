package common.param.ed.tr;

import common.param.TxnEvent;
import storage.SchemaRecord;
import storage.SchemaRecordRef;

import java.util.Arrays;
import java.util.HashSet;

public class TREvent extends TxnEvent {
    private final String tweetID; //TODO: If we let table assign ID when inserting tweet, this tweetID is not need anymore
    private final String[] words;
    public SchemaRecordRef tweetRecordRef;//The Read result from INSERT, read the tweetID that is assigned by table
    public String tweetIDResult;

    public TREvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String tweetID, String[] words) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.tweetID = tweetID;
        this.words = words;
        this.tweetRecordRef = new SchemaRecordRef();
    }

    public String getTweetID() {
        return this.tweetID;
    }

    public String[] getWords() {
        return this.words;
    }

    public TREvent cloneEvent() {
        return new TREvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, tweetID, words);
    }
}
