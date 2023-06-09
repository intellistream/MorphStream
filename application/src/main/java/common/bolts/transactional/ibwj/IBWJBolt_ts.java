package common.bolts.transactional.ibwj;

import combo.SINKCombo;
import common.param.ibwj.IBWJEvent;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.Insert;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class IBWJBolt_ts extends IBWJBolt {
    private static final Logger LOG = LoggerFactory.getLogger(IBWJBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    Collection<IBWJEvent> joinEvents;

    public IBWJBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public IBWJBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            IBWJEvent event = (IBWJEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            IBWJ_REQUEST_CONSTRUCT(event, txnContext);
        }

    }

    private void IBWJ_REQUEST_CONSTRUCT(IBWJEvent event, TxnContext txnContext) throws DatabaseException {

        String updateIndexTable = ""; //index to update
        String lookupIndexTable = ""; //index to lookup
        if (Objects.equals(event.getStreamID(), "r")) {
            updateIndexTable = "index_r_table";
            lookupIndexTable = "index_s_table";
        } else if (Objects.equals(event.getStreamID(), "s")) {
            updateIndexTable = "index_s_table";
            lookupIndexTable = "index_r_table";
        }

        Insert insert = new Insert(event.getAddress());
        String[] condition_table = new String[5];
        String[] condition_source = new String[5];
        for (int offset = 0; offset < 5; offset++) {
            condition_table[offset] = lookupIndexTable;
            condition_source[offset] = String.valueOf(event.getLookupKeys()[offset]);
        }

//        LOG.info("Constructing TC request: " + event.getMyBid());
        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                updateIndexTable, // source_table to write to
                event.getKey(),  // source_key to write to
                event.srcIndexRecordRef, // record to be filled up from READ
                insert, // overwrite empty index with new index
                condition_table, condition_source, //condition_source_table, condition_source_key
                null,
                event.success,
                "ibwj"
        );

        transactionManager.CommitTransaction(txnContext);
        joinEvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        joinEvents = new ArrayDeque<>();
    }

    void IBWJ_REQUEST_CORE() throws InterruptedException {
        for (IBWJEvent event : joinEvents) {
            IBWJ_CORE(event);
        }
    }

    void IBWJ_REQUEST_POST() throws InterruptedException {
        for (IBWJEvent event : joinEvents) {
            IBWJ_POST(event);
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
                    IBWJ_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    IBWJ_REQUEST_POST();
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
