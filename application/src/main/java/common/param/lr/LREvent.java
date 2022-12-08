package common.param.lr;

import common.datatype.PositionReport;
import common.param.TxnEvent;
import storage.SchemaRecordRef;

/**
 * Currently only consider position events.
 */
public class LREvent extends TxnEvent {
    private final int tthread;
    private final double bid;
    private final PositionReport posreport;//input_event associated common.meta data.
    public int count;
    public double lav;
    public SchemaRecordRef speed_value;
    public SchemaRecordRef count_value;
    private long timestamp;
//  private final AvgVehicleSpeedTuple vsreport;//intermediate input.

    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param tthread
     * @param bid
     */
    public LREvent(PositionReport posreport, int tthread, double bid) {
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

    public double getBid() {
        return bid;
    }

    @Override
    public LREvent cloneEvent() {
        return new LREvent(this.posreport,tthread,bid);
    }
}