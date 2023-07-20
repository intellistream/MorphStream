package intellistream.morphstream.examples.tsp.tollprocessing.events;

import intellistream.morphstream.examples.tsp.tollprocessing.util.datatype.PositionReport;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;

/**
 * Currently only consider position events.
 */
public class TPTxnEvent extends TxnEvent {
    private final int tthread;
    private final long bid;
    private final PositionReport posreport;//input_event associated common.meta data.
    public int count;
    public double lav;
    public SchemaRecordRef speed_value;
    public SchemaRecordRef count_value;
    private long timestamp;

    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param tthread
     * @param bid
     */
    public TPTxnEvent(PositionReport posreport, int tthread, long bid) {
        super(bid);
        this.posreport = posreport;
        this.tthread = tthread;
//      vsreport = vehicleSpeedTuple;
        this.bid = bid;
        speed_value = new SchemaRecordRef();
        count_value = new SchemaRecordRef();
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public PositionReport getPOSReport() {
        return posreport;
    }

    public int getPid() {
        return posreport.getSegment() % tthread;//which partition does this input_event belongs to.
    }

    public long getBid() {
        return bid;
    }

    @Override
    public TPTxnEvent cloneEvent() {
        return new TPTxnEvent(this.posreport, tthread, bid);
    }
}