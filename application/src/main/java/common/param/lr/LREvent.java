package common.param.lr;
import common.datatype.PositionReport;
import storage.SchemaRecordRef;
/**
 * Currently only consider position events.
 */
public class LREvent {
    private final int tthread;
    private final long bid;
    public int count;
    public double lav;
    public SchemaRecordRef speed_value;
    public SchemaRecordRef count_value;
    private long timestamp;
    private PositionReport posreport;//input_event associated common.meta data.
//    private final AvgVehicleSpeedTuple vsreport;//intermediate input.
    /**
     * creating a new LREvent.
     *
     * @param posreport
     * @param tthread
     * @param bid
     */
    public LREvent(PositionReport posreport, int tthread, long bid) {
        this.posreport = posreport;
        this.tthread = tthread;
//        vsreport = vehicleSpeedTuple;
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
    public long[] getBid_array() {
        return new long[0];
    }
}