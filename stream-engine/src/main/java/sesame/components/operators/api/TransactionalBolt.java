package sesame.components.operators.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.operators.base.MapBolt;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.execution.runtime.tuple.impl.Tuple;
import state_engine.DatabaseException;
import state_engine.profiler.Metrics;
import state_engine.transaction.TxnManager;
import state_engine.transaction.impl.TxnContext;
import state_engine.utils.SOURCE_CONTROL;

import java.util.concurrent.BrokenBarrierException;

import static application.CONTROL.combo_bid_size;
import static application.CONTROL.enable_latency_measurement;
import static state_engine.profiler.MeasureTools.*;

public abstract class TransactionalBolt<T> extends MapBolt implements Checkpointable {
    protected static final Logger LOG = LoggerFactory.getLogger(TransactionalBolt.class);
    private static final long serialVersionUID = -3899457584889441657L;
    public TxnManager transactionManager;
    public int _combo_bid_size = 1;
    protected int thread_Id;
    protected int tthread;
    protected int NUM_ACCESSES;
    protected int COMPUTE_COMPLEXITY;
    protected int POST_COMPUTE_COMPLEXITY;
    protected long timestamp;
    protected long _bid;
    protected Object input_event;
    int sum = 0;

    public TransactionalBolt(Logger log, int fid) {
        super(log);
        this.fid = fid;

    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, long _bid, int tthread) {
        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(_bid, _bid);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_LOCK(int _pid, int num_P, TxnManager txnManager, long[] bid_array, long _bid, int tthread) {

        for (int k = 0; k < num_P; k++) {
            txnManager.getOrderLock(_pid).blocking_wait(bid_array[_pid], _bid);
            _pid++;
            if (_pid == tthread)
                _pid = 0;
        }
    }

    public static void LA_RESETALL(TxnManager txnManager, int tthread) {
        for (int k = 0; k < tthread; k++) {
            txnManager.getOrderLock(k).reset();
        }
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

    protected abstract void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException;

    protected void nocc_execute(Tuple in) throws DatabaseException, InterruptedException {
        BEGIN_TOTAL_TIME_MEASURE(thread_Id);//start measure prepare and total.

        PRE_EXECUTE(in);

        END_PREPARE_TIME_MEASURE(thread_Id);

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        TXN_PROCESS(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, combo_bid_size);

        END_TOTAL_TIME_MEASURE(thread_Id, combo_bid_size);
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
        BEGIN_TOTAL_TIME_MEASURE(thread_Id);//start measure prepare and total.

        PRE_EXECUTE(in);

        END_PREPARE_TIME_MEASURE(thread_Id);

        //begin transaction processing.
        BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);//need to amortize.

        LAL_PROCESS(_bid);

        PostLAL_process(_bid);

        //end transaction processing.
        END_TRANSACTION_TIME_MEASURE(thread_Id);

        POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.

        END_TOTAL_TIME_MEASURE(thread_Id, 1);//otherwise deadlock.
    }

    @Override
    public void forward_checkpoint_single(int sourceId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint_single(int sourceTask, String streamId, long bid, Marker marker) {

    }

    @Override
    public void forward_checkpoint(int sourceId, long bid, Marker marker) throws InterruptedException {
        this.collector.broadcast_marker(bid, marker);//bolt needs to broadcast_marker
    }

    @Override
    public void forward_checkpoint(int sourceTask, String streamId, long bid, Marker marker) throws InterruptedException {
        this.collector.broadcast_marker(streamId, bid, marker);//bolt needs to broadcast_marker
    }

    @Override
    public void ack_checkpoint(Marker marker) {
        this.collector.broadcast_ack(marker);//bolt needs to broadcast_ack
    }

    @Override
    public void earlier_ack_checkpoint(Marker marker) {

    }

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

        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);

        sum = 0;


    }

    protected void execute_ts_normal(Tuple in) throws DatabaseException, InterruptedException {
        //pre stream processing phase..

        BEGIN_TOTAL_TIME_MEASURE_TS(thread_Id);

        PRE_EXECUTE(in);

        END_PREPARE_TIME_MEASURE_TS(thread_Id);

        PRE_TXN_PROCESS(_bid, timestamp);
    }

    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
    }//only used by TSTREAM.


    protected void PostLAL_process(long bid) throws DatabaseException, InterruptedException {
    }

    protected void LAL_PROCESS(long bid) throws DatabaseException, InterruptedException {
    }

    protected void POST_PROCESS(long bid, long timestamp, int i) throws InterruptedException {

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {

        this.thread_Id = thread_Id;
        tthread = config.getInt("tthread", 0);
        NUM_ACCESSES = Metrics.NUM_ACCESSES;
        COMPUTE_COMPLEXITY = Metrics.COMPUTE_COMPLEXITY;
        POST_COMPUTE_COMPLEXITY = Metrics.POST_COMPUTE_COMPLEXITY;
        //LOG.DEBUG("NUM_ACCESSES: " + NUM_ACCESSES + " theta:" + theta);
        SOURCE_CONTROL.getInstance().config(tthread);
    }

}
