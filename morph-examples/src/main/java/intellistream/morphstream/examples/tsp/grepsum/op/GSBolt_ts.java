package intellistream.morphstream.examples.tsp.grepsum.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.grepsum.events.GSTxnEvent;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.SUM;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.combo_bid_size;
import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;

public class GSBolt_ts extends GSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final double write_useful_time = 556;//write-compute time pre-measured.
    Collection<GSTxnEvent> GSTxnEvents;
    int i = 0;
    private int writeEvents;

    public GSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public GSBolt_ts(int fid) {
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
            GSTxnEvent event = (GSTxnEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            boolean flag = event.ABORT_EVENT();
//            if (flag) {//read
//                read_construct(event, txnContext);
//            } else {
//                write_construct(event, txnContext);
//            }
            RANGE_WRITE_CONSRUCT(event, txnContext);
        }
    }

    private void RANGE_WRITE_CONSRUCT(GSTxnEvent event, TxnContext txnContext) throws DatabaseException {
        SUM sum;
        if (event.ABORT_EVENT()) {
            sum = new SUM(-1);
        } else
            sum = new SUM();

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
            transactionManager.Asy_WriteRecord_Cond(
                    txnContext,
                    "MicroTable",
                    String.valueOf(event.getKeys()[writeKeyIdx]), // src key to write ahead
                    //to be fill up.
                    condition_table, condition_source,
                    null//condition source, condition id.
            );          //asynchronously return.
        }
        transactionManager.CommitTransaction(txnContext);
        GSTxnEvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        GSTxnEvents = new ArrayDeque<>();
    }

    void READ_REQUEST_CORE() throws InterruptedException {
//        while (!EventsHolder.isEmpty() && !Thread.interrupted()) {
//            MicroEvent input_event = EventsHolder.remove();
//            if (!READ_REQUEST_CORE(input_event))
//                EventsHolder.offer(input_event);
//            else {
//                BEGIN_POST_TIME_MEASURE(thread_Id);
//                BUYING_REQUEST_POST(input_event);
//                END_POST_TIME_MEASURE_ACC(thread_Id);
//            }
//        }
        for (GSTxnEvent event : GSTxnEvents) {
            READ_CORE(event);
        }
    }

    void READ_POST() throws InterruptedException {
        for (GSTxnEvent event : GSTxnEvents) {
            READ_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int num_events = GSTxnEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    i++;
                    READ_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    READ_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);
                //all tuples in the holder is finished.
                GSTxnEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
