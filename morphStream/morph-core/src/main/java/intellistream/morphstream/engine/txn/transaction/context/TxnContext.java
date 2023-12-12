package intellistream.morphstream.engine.txn.transaction.context;

import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;

public class TxnContext {
    public final int thread_Id;
    private final int fid;
    private final long bid;
    private final long txnReqID; //Used under NFV context, created by VNF instance to track txn request
    private final String thisOpId;
    private final boolean is_read_only_;
    private final boolean is_dependent_;
    private final boolean is_adhoc_;
    public long[] partition_bid;
    //    public long lock_ratio;
    public long index_time;
    public long ts_allocation;
    public boolean is_retry_;
    public boolean success;
    public int pid;
    private double[] index_time_;
    private int txn_type_;
    private SchemaRecordRef record_ref;

    public TxnContext(int thread_Id, int fid, long bid, long txnReqID) {
        this.thread_Id = thread_Id;
        this.thisOpId = null;
        this.fid = fid;
        this.bid = bid;
        this.txnReqID = txnReqID;
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
    public TxnContext(String thisOpId, int fid, long bid, long txnReqID) {
        this.thisOpId = thisOpId;
        this.fid = fid;
        this.bid = bid;
        this.txnReqID = txnReqID;
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
    public TxnContext(String thisOpId, int fid, long bid, long txnReqID, SchemaRecordRef record_ref) {
        this(thisOpId, fid, bid, txnReqID);
    }

    public TxnContext(int thread_Id, int fid, long bid, long txnReqID, SchemaRecordRef record_ref) {
        this(thread_Id, fid, bid, txnReqID);
        this.record_ref = record_ref;
    }

    public TxnContext(String thisOpId, int fid, long bid, long txnReqID, double[] index_time) {
        this(thisOpId, fid, bid, txnReqID);
        index_time_ = index_time;
    }

    public TxnContext(int thread_Id, int fid, long bid, long txnReqID, double[] index_time) {
        this(thread_Id, fid, bid, txnReqID);
        index_time_ = index_time;
    }

    public TxnContext(int thread_id, int fid, long bid, long txnReqID, int pid) {
        this(thread_id, fid, bid, txnReqID);
        this.pid = pid;
    }

    public TxnContext(int thread_id, int fid, long[] bid, long txnReqID, int pid) {
        this(thread_id, fid, bid[0], txnReqID);
        this.partition_bid = bid;
        this.pid = pid;
    }

    public String getThisOpId() {
        return thisOpId;
    }

    public int getFID() {
        return fid;
    }

    public long getBID() {
        return bid;
    }
    public long getTxnReqID() {
        return txnReqID;
    }
}
