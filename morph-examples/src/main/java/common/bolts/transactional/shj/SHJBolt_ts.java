package common.bolts.transactional.shj;

import combo.SINKCombo;
import common.param.shj.SHJEvent;
import engine.txn.db.DatabaseException;
import engine.stream.execution.ExecutionGraph;
import engine.stream.execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.profiler.MeasureTools;
import engine.txn.transaction.context.TxnContext;
import engine.txn.transaction.function.Join;
import engine.txn.transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;
import static engine.txn.profiler.MeasureTools.*;
import static engine.txn.profiler.Metrics.NUM_ITEMS;

public class SHJBolt_ts extends SHJBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SHJBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    Collection<SHJEvent> joinEvents;

    public SHJBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public SHJBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            SHJEvent event = (SHJEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            SHJ_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    private void SHJ_REQUEST_CONSTRUCT(SHJEvent event, TxnContext txnContext) throws DatabaseException {

        String updateIndexTable = ""; //index to update
        String lookupIndexTable = ""; //index to lookup
        if (Objects.equals(event.getStreamID(), "r")) {
            updateIndexTable = "index_r_table";
            lookupIndexTable = "index_s_table";
        } else if (Objects.equals(event.getStreamID(), "s")) {
            updateIndexTable = "index_s_table";
            lookupIndexTable = "index_r_table";
        }

        Join join = new Join(Long.parseLong(event.getAmount()));
        String[] condition_table = new String[event.getLookupKeys().length];
        String[] condition_source = new String[event.getLookupKeys().length];
        for (int offset = 0; offset < event.getLookupKeys().length; offset++) {
            condition_table[offset] = lookupIndexTable;
            condition_source[offset] = String.valueOf(event.getLookupKeys()[offset]);
        }

//        LOG.info("Constructing TC request: " + event.getMyBid());
        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                updateIndexTable, // source_table to write to
                event.getKey(),  // source_key to write to
                event.srcIndexRecordRef, // record to be filled up from READ
                join, // overwrite empty index with new index
                condition_table, condition_source, //condition_source_table, condition_source_key
                null,
                event.success
        );

        transactionManager.CommitTransaction(txnContext);
        joinEvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        joinEvents = new ArrayDeque<>();
    }

    void SHJ_REQUEST_CORE() throws InterruptedException {
        for (SHJEvent event : joinEvents) {
            SHJ_CORE(event);
        }
    }

    void SHJ_REQUEST_POST() throws InterruptedException {
        for (SHJEvent event : joinEvents) {
            SHJ_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int num_events = joinEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    SHJ_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    SHJ_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                //all tuples in the holder is finished.
                joinEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
