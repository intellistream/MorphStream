package components.operators.api;

import components.operators.base.MapBolt;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import profiler.Metrics;
import transaction.TxnManager;
import transaction.context.TxnContext;

import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;

public abstract class TransactionalBolt extends MapBolt implements Checkpointable {
    protected static final Logger LOG = LoggerFactory.getLogger(TransactionalBolt.class);
    private static final long serialVersionUID = -3899457584889441657L;
    public TxnManager transactionManager;
    public int _combo_bid_size = 1;
    protected int thread_Id;
    protected int tthread;
    protected int total_events;
    protected int NUM_ACCESSES;
    protected int COMPUTE_COMPLEXITY;
    protected int POST_COMPUTE_COMPLEXITY;
    protected long timestamp;
    protected double _bid;
    protected Object input_event;
    int sum = 0;

    public TransactionalBolt(Logger log, int fid) {
        super(log);
        this.fid = fid;
    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, double _bid, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(_bid, _bid);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, double[] bid_array, double _bid, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid], _bid);
//            LOG.info(_pid + " : " + bid_array[_pid]);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_LOCK_Reentrance(TxnManager txnManager, double[] bid_array, int[] partition_indexs, double _bid, int thread_Id) {
        for (int _pid : partition_indexs) {
//            LOG.info(thread_Id + " try lock: " + _pid);
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid], _bid);
//            LOG.info(thread_Id + " get lock: " + _pid);
        }
    }

    public static void LA_RESETALL(TxnManager txnManager, int tthread) {
        txnManager.getOrderLock(tthread).reset();
    }

    public static void LA_UNLOCKALL(TxnManager txnManager, int tthread) {
        for (int k = 0; k < tthread; k++) {
            txnManager.getOrderLock(k).advance();
        }
    }

    public static void LA_UNLOCK(int _pid, int num_P, TxnManager txnManager, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).advance();
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_UNLOCK_Reentrance(TxnManager txnManager, int[] partition_indexs, int thread_Id) {
        for (int _pid : partition_indexs) {
            txnManager.getOrderLock(_pid).advance();
//            LOG.info(thread_Id + " release lock: " + _pid);

        }
    }

    protected abstract void TXN_PROCESS(double _bid) throws DatabaseException, InterruptedException;

    protected void nocc_execute(Tuple in) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id);//start measure prepare and total.
        PRE_EXECUTE(in);
        MeasureTools.END_PREPARE_TIME_MEASURE(thread_Id);
        //begin transaction processing.
        MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);//need to amortize.
        TXN_PROCESS(_bid);
        //end transaction processing.
        MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
        POST_PROCESS(_bid, timestamp, combo_bid_size);
    }

    /**
     * This is used for all LAL based schemes including LOCK and MVLK.
     *
     * @param in
     * @throws InterruptedException
     * @throws DatabaseException
     * @throws BrokenBarrierException
     */
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id);
        PRE_EXECUTE(in);
        MeasureTools.END_PREPARE_TIME_MEASURE(thread_Id);
        //begin transaction processing.
        MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);//need to amortize.
        LAL_PROCESS(_bid);
        PostLAL_process(_bid);
        //end transaction processing.
        MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
        POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.
    }

    public void execute() throws BrokenBarrierException, InterruptedException {}

    @Override
    public boolean checkpoint(int counter) {
        return false;
    }

    protected void PRE_EXECUTE(Tuple in) {
        if (enable_latency_measurement)
            timestamp = in.getLong(1);
        else
            timestamp = 0L;//
        _bid = in.getBID();
        input_event = in.getValue(0);
//        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid); //TODO: Removed because txn_context[] is not initialized for all ED bolt threads. Improve this.
        sum = 0;
    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..
        MeasureTools.BEGIN_TOTAL_TIME_MEASURE_TS(thread_Id);
        PRE_EXECUTE(in);
        MeasureTools.END_PREPARE_TIME_MEASURE_ACC(thread_Id);
        PRE_TXN_PROCESS(_bid, timestamp);
    }

    protected void PRE_TXN_PROCESS(double bid, long timestamp) throws DatabaseException, InterruptedException {
    }//only used by TSTREAM.

    protected void PostLAL_process(double bid) throws DatabaseException, InterruptedException {
    }

    protected void LAL_PROCESS(double bid) throws DatabaseException, InterruptedException {
    }

    protected void POST_PROCESS(double bid, long timestamp, int i) throws InterruptedException {
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        total_events = config.getInt("totalEvents", 0);
        NUM_ACCESSES = Metrics.NUM_ACCESSES;
        COMPUTE_COMPLEXITY = Metrics.COMPUTE_COMPLEXITY;
        POST_COMPUTE_COMPLEXITY = Metrics.POST_COMPUTE_COMPLEXITY;
        //LOG.DEBUG("NUM_ACCESSES: " + NUM_ACCESSES + " theta:" + theta);
    }
}