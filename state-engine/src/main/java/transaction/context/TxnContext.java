package transaction.context;

import storage.SchemaRecordRef;

public class TxnContext {
    public final int thread_Id;
    private final int fid;
    private final double bid;
    private final String thisOpId;
    private final boolean is_read_only_;
    private final boolean is_dependent_;
    private final boolean is_adhoc_;
    public double[] partition_bid;
    //    public long lock_ratio;
    public long index_time;
    public long ts_allocation;
    public boolean is_retry_;
    public boolean success;
    public int pid;
    private double[] index_time_;
    private int txn_type_;
    private SchemaRecordRef record_ref;

    public TxnContext(int thread_Id, int fid, double bid) {
        this.thread_Id = thread_Id;
        this.thisOpId = null;
        this.fid = fid;
        this.bid = bid;
        is_adhoc_ = false;
        is_read_only_ = false;
        is_dependent_ = false;
        is_retry_ = false;
        record_ref = null;
    }

    /**
     * Multi-keys transaction
     *
     * @param thisOpId
     * @param fid
     * @param bid
     */
    public TxnContext(String thisOpId, int fid, double bid) {
        this.thisOpId = thisOpId;
        this.fid = fid;
        this.bid = bid;
        is_adhoc_ = false;
        is_read_only_ = false;
        is_dependent_ = false;
        is_retry_ = false;
        thread_Id = -1;
        record_ref = null;
    }

    /**
     * Single key transaction.
     *
     * @param thisOpId
     * @param fid
     * @param bid
     * @param record_ref
     */
    public TxnContext(String thisOpId, int fid, double bid, SchemaRecordRef record_ref) {
        this(thisOpId, fid, bid);
    }

    public TxnContext(int thread_Id, int fid, double bid, SchemaRecordRef record_ref) {
        this(thread_Id, fid, bid);
        this.record_ref = record_ref;
    }

    public TxnContext(String thisOpId, int fid, double bid, double[] index_time) {
        this(thisOpId, fid, bid);
        index_time_ = index_time;
    }

    public TxnContext(int thread_Id, int fid, double bid, double[] index_time) {
        this(thread_Id, fid, bid);
        index_time_ = index_time;
    }

    public TxnContext(int thread_id, int fid, double bid, int pid) {
        this(thread_id, fid, bid);
        this.pid = pid;
    }

    public TxnContext(int thread_id, int fid, double[] bid, int pid) {
        this(thread_id, fid, bid[0]);
        this.partition_bid = bid;
        this.pid = pid;
    }

    public String getThisOpId() {
        return thisOpId;
    }

    public int getFID() {
        return fid;
    }

    public double getBID() {
        return bid;
    }
}
