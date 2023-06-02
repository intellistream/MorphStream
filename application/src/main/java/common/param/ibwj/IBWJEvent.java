package common.param.ibwj;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;


/**
 * Support Multi workset since 1 SEP 2018.
 */
public class IBWJEvent extends TxnEvent {
    private final String streamID;
    private final String key;
    private final String address;
    public volatile SchemaRecordRef indexRecordRef = new SchemaRecordRef();
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;
    private String[] addressTuple = new String[2];


    public IBWJEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String key, String streamID, String address) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.key = key;
        this.streamID = streamID;
        this.address = address;
        this.addressTuple[0] = address;
    }

    public String getStreamID() {
        return streamID;
    }

    public String getKey() {
        return key;
    }

    public String getAddress() {
        return address;
    }

    public SchemaRecordRef getRecord_ref() {
        return indexRecordRef;
    }

    public void setAddressTuple(String matchingTupleAddr) {
        addressTuple[1] = matchingTupleAddr;
    }
    public String[] getAddressTuple() {
        return addressTuple;
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


    public IBWJEvent cloneEvent() {
        return new IBWJEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, key, streamID, address);
    }
}