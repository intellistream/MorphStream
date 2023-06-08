package common.param.lb;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;


/**
 * Support Multi workset since 1 SEP 2018.
 */
public class LBEvent extends TxnEvent {
    private final String connID;
    private final String srcAddr;
    private final String srcPort;
    public volatile SchemaRecordRef serverRecord = new SchemaRecordRef();
    public boolean isNewConn = false;
    public String serverID;
    private final double myBid;
    private final int myPid;
    private final String my_bid_array;
    private final String my_partition_index;
    private final int my_number_of_partitions;

    public LBEvent(double bid, int pid, String bid_array, String partition_index, int number_of_partitions,
                   String connID, String srcAddr, String srcPort) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.myBid = bid;
        this.myPid = pid;
        this.my_bid_array = bid_array;
        this.my_partition_index = partition_index;
        this.my_number_of_partitions = number_of_partitions;
        this.connID = connID;
        this.srcAddr = srcAddr;
        this.srcPort = srcPort;
    }

    public String getConnID() {
        return connID;
    }
    public String getSrcAddr() {
        return srcAddr;
    }
    public String getSrcPort() {
        return srcPort;
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


    public LBEvent cloneEvent() {
        return new LBEvent(bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions,
                connID, srcAddr, srcPort);
    }
}