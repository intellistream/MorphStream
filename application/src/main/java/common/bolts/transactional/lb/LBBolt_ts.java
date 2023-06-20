package common.bolts.transactional.lb;

import combo.SINKCombo;
import common.param.lb.LBEvent;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Tuple;
import execution.runtime.tuple.impl.msgs.GeneralMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.Constants.DEFAULT_STREAM_ID;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class LBBolt_ts extends LBBolt {
    private static final Logger LOG = LoggerFactory.getLogger(LBBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    Collection<LBEvent> lbEvents;
//    private final HashMap<String, String> conn_server_map = new HashMap<>(); //TODO: Store conn-server mapping

    public LBBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public LBBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        lbEvents = new ArrayDeque<>();
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            LBEvent event = (LBEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            LB_REQUEST_CONSTRUCT((LBEvent) event, txnContext);
        }

    }

    private void LB_REQUEST_CONSTRUCT(LBEvent event, TxnContext txnContext) throws DatabaseException {

        if (event.isNewConn()) {
            transactionManager.BeginTransaction(txnContext);
            int NUM_ACCESS = event.TOTAL_NUM_ACCESS;
            String[] condition_table = new String[NUM_ACCESS];
            String[] condition_source = new String[NUM_ACCESS];
            for (int j = 0; j < NUM_ACCESS; j++) {
                condition_source[j] = String.valueOf(event.getKeys()[j]);
                condition_table[j] = "server_table";
            }
            transactionManager.Asy_ModifyRecord_ReadN(
                    txnContext,
                    "server_table",
                    String.valueOf(event.getKeys()[0]), // src key to write ahead
                    event.getRecord_refs()[0],//to be fill up.
                    null, //no function
                    condition_table, condition_source,//condition source, condition id.
                    event.success, "lb");//asynchronously return.
            transactionManager.CommitTransaction(txnContext);
        }

        lbEvents.add(event);
    }

    void LB_REQUEST_CORE() {
        for (LBEvent event : lbEvents) {
            LB_TS_CORE(event);
        }
    }

    void LB_REQUEST_POST() throws InterruptedException {
        for (LBEvent event : lbEvents) {
            if (!enable_app_combo) {
                collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
            } else {
                if (enable_latency_measurement) {
                    sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
                }
            }
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int num_events = lbEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    LB_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    LB_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                //all tuples in the holder is finished.
                lbEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
