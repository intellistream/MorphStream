package common.param.ed.es;

import common.param.TxnEvent;
import storage.SchemaRecordRef;

import java.util.Arrays;

public class ESEvent extends TxnEvent {
    private final String clusterID;
    private final SchemaRecordRef cluster_record = new SchemaRecordRef();
    public boolean isEvent;
    public String[] wordList;

    public ESEvent(int bid, int pid, String bid_array, String partition_index, int number_of_partitions, String clusterID) {
        super(bid, pid, bid_array, partition_index, number_of_partitions);
        this.clusterID = clusterID;
    }

    public String getClusterID() {
        return this.clusterID;
    }

    public SchemaRecordRef getClusterRecord() {
        return cluster_record;
    }

    public ESEvent cloneEvent() {
        return new ESEvent((int) bid, pid, Arrays.toString(bid_array), Arrays.toString(partition_indexs), number_of_partitions, clusterID);
    }
}
