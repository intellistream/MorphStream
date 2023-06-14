package common.bolts.transactional.nongs;

import combo.SINKCombo;
import common.bolts.transactional.sl.GlobalSorter;
import common.param.TxnEvent;
import common.param.mb.MicroEvent;
import common.param.mb.NonMicroEvent;
import components.context.TopologyContext;
import content.common.CommonMetaTypes;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerSStore;
import utils.AppConfig;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static content.common.CommonMetaTypes.AccessType.NON_READ_WRITE_COND_READN;
import static content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static profiler.MeasureTools.*;
import static profiler.MeasureTools.END_WAIT_TIME_MEASURE_ACC;

public class NonGSBolt_sstore extends NonGSBolt{
    private static final Logger LOG = LoggerFactory.getLogger(NonGSBolt_sstore.class);
    private static final long serialVersionUID = -1837448729429847022L;
    ArrayDeque<Tuple> tuples = new ArrayDeque<>();

    public NonGSBolt_sstore(int fid) {
        super(LOG, fid, null);
    }
    public NonGSBolt_sstore(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerSStore(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, this.context.getThisComponent().getNumTasks(), this.context.getStageMap().get(this.fid));
        if (!enable_states_partition) {
            if (enable_log) LOG.info("Please enable `enable_states_partition` for PAT scheme");
            System.exit(-1);
        }
    }
    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        context.getGraph().topology.tableinitilizer.loadDB(thread_Id, context.getGraph().topology.spinlock, this.context.getNUMTasks());
    }
    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int num_events = tuples.size();
            start_evaluate(thread_Id, in.getBID(), num_events);
            // execute txn_
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            for (Tuple tuple : tuples) {
                PRE_EXECUTE(tuple);
                //begin transaction processing.
                LAL_PROCESS(_bid);
                PostLAL_process(_bid);
                //end transaction processing.
                POST_PROCESS(_bid, timestamp, 1);//otherwise deadlock.
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id, num_events);
            tuples.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);//otherwise deadlock.
        } else {
            // sort
            execute_ts_normal(in);
            tuples.add(in);
        }
    }
    public void start_evaluate(int thread_Id, double mark_ID, int num_events) throws InterruptedException, BrokenBarrierException {
        transactionManager.stage.getControl().preStateAccessBarrier(thread_Id);//sync for all threads to come to this line to ensure chains are constructed for the current batch.
        // add bid_array for events
        if (thread_Id == 0) {
            int partitionOffset = config.getInt("NUM_ITEMS") / tthread;
            int[] p_bids = new int[(int) tthread];
            HashMap<Integer, Integer> pids = new HashMap<>();
            for (TxnEvent event : GlobalSorter.sortedEvents) {
                if (event instanceof NonMicroEvent) {
                    parseMicroEvent(partitionOffset, (NonMicroEvent) event, pids);
                    event.setBid_array(Arrays.toString(p_bids), Arrays.toString(pids.keySet().toArray()));
                    pids.replaceAll((k, v) -> p_bids[k]++);
                } else {
                    throw new UnsupportedOperationException();
                }
                pids.clear();
            }
            GlobalSorter.sortedEvents.clear();
        }
        transactionManager.stage.getControl().postStateAccessBarrier(thread_Id);
    }
    private void parseMicroEvent(int partitionOffset, NonMicroEvent event, HashMap<Integer, Integer> pids) {
        if (event.isNon_Deterministic_StateAccess()) {//For non-deterministic state access, we need to add all the partitions.
            for (int i = 0; i < tthread; i++) {
                pids.put(i, 0);
            }
        } else {
            for (int key : event.getKeys()) {
                pids.put(key / partitionOffset, 0);
            }
        }
    }
    @Override
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
//            System.out.println("thread: "+thread_Id+", event_id: "+_bid);
            TxnEvent event = (TxnEvent) input_event;
            GlobalSorter.addEvent(event);
        }
    }
    @Override
    protected void LAL_PROCESS(double _bid) throws DatabaseException {
        txn_context[0] = new TxnContext(thread_Id, this.fid, _bid);
        NonMicroEvent event = (NonMicroEvent) input_event;
        int _pid = event.getPid();
        BEGIN_WAIT_TIME_MEASURE(thread_Id);
        //ensures that locks are added in the input_event sequence order.
        LA_LOCK_Reentrance(transactionManager, event.getBid_array(), event.partition_indexs, _bid, thread_Id);
        BEGIN_LOCK_TIME_MEASURE(thread_Id);
        LAL(event, _bid, _bid);
        END_LOCK_TIME_MEASURE_ACC(thread_Id);
        LA_UNLOCK_Reentrance(transactionManager, event.partition_indexs, thread_Id);
        END_WAIT_TIME_MEASURE_ACC(thread_Id);
    }
    protected void LAL(NonMicroEvent event, double i, double _bid) throws DatabaseException {
        if (event.isAbort()) {
            return;
        } else {
            if (event.isNon_Deterministic_StateAccess()) {
                WRITE_LOCK_ALL(this.context.getGraph().topology.spinlock);
            } else {
                WRITE_LOCK_AHEAD(event, txn_context[(int) (i - _bid)]);
            }
        }
    }
    @Override
    protected void PostLAL_process(double bid) throws DatabaseException, InterruptedException {
        for (double i = _bid; i < _bid + _combo_bid_size; i++) {
            NonMicroEvent event = (NonMicroEvent) input_event;
            if (event.isNon_Deterministic_StateAccess()) {
                non_write_request_noLock(event, txn_context[(int) (i - _bid)]);
            } else {
                write_request_noLock(event, txn_context[(int) (i - _bid)]);
            }
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            WRITE_CORE(event);
            END_ACCESS_TIME_MEASURE_ACC(thread_Id);
            transactionManager.CommitTransaction(txn_context[(int) (i - _bid)]);
            if (event.isNon_Deterministic_StateAccess())
                WRITE_UNLOCK_ALL(this.context.getGraph().topology.spinlock);
        }
    }
    protected void WRITE_LOCK_AHEAD(NonMicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i)
            transactionManager.lock_ahead(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], READ_WRITE);
    }
    protected void WRITE_LOCK_ALL(SpinLock[] spinLocks) throws DatabaseException {
        transactionManager.lock_all(spinLocks);
    }
    protected void WRITE_UNLOCK_ALL(SpinLock[] spinLocks) throws DatabaseException {
        transactionManager.unlock_all(spinLocks);
    }
    protected boolean write_request_noLock(NonMicroEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, READ_WRITE);
    }
    protected boolean non_write_request_noLock(NonMicroEvent event, TxnContext txnContext) throws DatabaseException {
        return !process_request_noLock(event, txnContext, NON_READ_WRITE_COND_READN);
    }

    private boolean process_request_noLock(NonMicroEvent event, TxnContext txnContext, CommonMetaTypes.AccessType accessType) throws DatabaseException {
        for (int i = 0; i < event.TOTAL_NUM_ACCESS; ++i) {
            boolean rt = transactionManager.SelectKeyRecord_noLock(txnContext, "MicroTable",
                    String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], accessType);
            if (rt) {
                assert event.getRecord_refs()[i].getRecord() != null;
            } else {
                return true;
            }
        }
        return false;
    }

    protected void WRITE_CORE(NonMicroEvent event) {
        long sum = 0;
        DataBox TargetValue_value = event.getRecord_refs()[0].getRecord().getValues().get(1);
        int NUM_ACCESS = event.TOTAL_NUM_ACCESS / event.Txn_Length;
        for (int j = 0; j < event.Txn_Length; ++j) {
            AppConfig.randomDelay();
            for (int i = 0; i < NUM_ACCESS; ++i) {
                int offset = j * NUM_ACCESS + i;
                SchemaRecordRef recordRef = event.getRecord_refs()[offset];
                SchemaRecord record = recordRef.getRecord();
                DataBox Value_value = record.getValues().get(1);
                final long Value = Value_value.getLong();
                sum += Value;
            }
        }
        sum /= event.TOTAL_NUM_ACCESS;
        TargetValue_value.setLong(sum);
    }

    @Override
    protected void POST_PROCESS(double bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            NonMicroEvent event = (NonMicroEvent) input_event;
            (event).setTimestamp(timestamp);
            WRITE_POST(event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }
    protected void WRITE_POST(NonMicroEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));//(double bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }
}
