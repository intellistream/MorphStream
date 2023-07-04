package common.bolts.transactional.nongs;

import combo.SINKCombo;
import common.param.mb.NonMicroEvent;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.SUM;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class NonGSBolt_ts extends NonGSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(NonGSBolt_ts.class);
    private static final long serialVersionUID = 1802551870447129582L;
    Collection<NonMicroEvent> nonMicroEvents;
    public NonGSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        nonMicroEvents = new ArrayDeque<>();
    }
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            NonMicroEvent event = (NonMicroEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            RANGE_WRITE_CONSTRUCT(event, txnContext);
        }
    }
    private void RANGE_WRITE_CONSTRUCT(NonMicroEvent event, TxnContext txnContext) throws DatabaseException {
        SUM sum;
        if (event.isAbort()) {
            sum = new SUM(-1);
        } else {
            sum = new SUM();
        }
        transactionManager.BeginTransaction(txnContext);
        // multiple operations will be decomposed
        for (int i = 0; i < event.Txn_Length; i++) {
            int NUM_ACCESS = event.TOTAL_NUM_ACCESS / event.Txn_Length;
            String[] condition_table = new String[NUM_ACCESS];
            String[] condition_source = new String[NUM_ACCESS];
            for (int j = 0; j < NUM_ACCESS; j++) {
                int offset = i * NUM_ACCESS + j;
                condition_source[j] = String.valueOf(event.getKeys()[offset]);
                condition_table[j] = "MicroTable";
            }
            int writeKeyIdx = i * NUM_ACCESS;
            if (event.isNon_Deterministic_StateAccess()) {
                transactionManager.Asy_ModifyRecord_Non_ReadN( txnContext,
                        "MicroTable",
                        String.valueOf(event.getKeys()[writeKeyIdx]), // src key to write ahead
                        event.getRecord_refs()[writeKeyIdx],//to be fill up.
                        sum,
                        condition_table, condition_source,//condition source, condition id.
                        event.success);
            } else {
                transactionManager.Asy_ModifyRecord_ReadN(
                        txnContext,
                        "MicroTable",
                        String.valueOf(event.getKeys()[writeKeyIdx]), // src key to write ahead
                        event.getRecord_refs()[writeKeyIdx],//to be fill up.
                        sum,
                        condition_table, condition_source,//condition source, condition id.
                        event.success);          //asynchronously return.
            }
        }
        transactionManager.CommitTransaction(txnContext);
        nonMicroEvents.add(event);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int num_events = nonMicroEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    READ_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    READ_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                //all tuples in the holder is finished.
                nonMicroEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }

    private void READ_POST() throws InterruptedException {
        for (NonMicroEvent event:nonMicroEvents) {
            READ_POST(event);
        }
    }

    private void READ_REQUEST_CORE() {
        for (NonMicroEvent event:nonMicroEvents) {
            READ_CORE(event);
        }
    }
}