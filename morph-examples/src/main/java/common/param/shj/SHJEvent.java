package common.param.shj;

import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;

import java.util.Arrays;


/**
 * Support Multi workset since 1 SEP 2018.
 */
public class SHJEvent extends TxnEvent {

    private final String key;
    private final String streamID;
    private final String amount;
    private final String[] lookupKeys;
    private final SchemaRecordRef[] lookupIndexRecords;
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;
    private final String[] turnoverRatePair = new String[2];
    public volatile SchemaRecordRef srcIndexRecordRef = new SchemaRecordRef();


    public SHJEvent(long bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                    String key, String streamID, String amount, String[] lookupKeys) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.key = key;
        this.streamID = streamID;
        this.amount = amount;
        this.lookupKeys = lookupKeys;
        this.turnoverRatePair[0] = amount;
        lookupIndexRecords = new SchemaRecordRef[lookupKeys.length];
        for (int i = 0; i < lookupKeys.length; i++) {
            lookupIndexRecords[i] = new SchemaRecordRef();
        }
    }

    public String getStreamID() {
        return streamID;
    }

    public String getKey() {
        return key;
    }

    public String getAmount() {
        return amount;
    }

    public String[] getLookupKeys() {
        return lookupKeys;
    }

    public SchemaRecordRef[] getLookupIndexRecords() {
        return lookupIndexRecords;
    }

    public String[] getTurnoverRatePair() {
        return turnoverRatePair;
    }

    public void setTurnoverRatePair(String matchingTupleAddr) {
        turnoverRatePair[1] = matchingTupleAddr;
    }

    public double getMyBid() {
        return myBid;
    }

    public int getMyPid() {
        return myPid;
    }

    public String getMyBidArray() {
        return my_bid_array;
    }

    public String getMyPartitionIndex() {
        return my_partition_index;
    }

    public int getMyNumberOfPartitions() {
        return my_number_of_partitions;
    }


    public SHJEvent cloneEvent() {
        return new SHJEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, key, streamID, amount, lookupKeys);
    }
}