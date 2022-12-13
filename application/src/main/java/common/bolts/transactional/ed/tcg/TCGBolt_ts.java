package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
import common.param.ed.tc.TCEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;

public class TCGBolt_ts extends TCGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TCEvent> tcEvents;
    HashMap<String, Boolean> tweetMap = new HashMap<>();
    private int counter = 0;

    //To be used in Combo
    public TCGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public TCGBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        tcEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {}

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TCEvent event = (TCEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                TC_GATE_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    //Updates the mapping of tweetID and isBurst
    protected void TC_GATE_REQUEST_CONSTRUCT(TCEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        tcEvents.add(event);

        for (String tweetID : event.tweetIDList) {
            if (!Boolean.TRUE.equals(tweetMap.get(tweetID))) { //Do not update when the value is "not-null and true"
                tweetMap.put(tweetID, event.isBurst);
            }
        }
    }

    private void TC_GATE_REQUEST_CORE() {}

    // Emit output information to TCGBolt
    private void TC_GATE_REQUEST_POST() throws InterruptedException {
        Iterator<TCEvent> tcEventIterator = tcEvents.iterator();

        for (Map.Entry<String, Boolean> entry : tweetMap.entrySet()) {
            TCEvent event = tcEventIterator.next();

            CUEvent outEvent = new CUEvent(event.getBid(), event.getMyPid(), event.getMyBidArray(), event.getMyPartitionIndex(),
                    event.getMyNumberOfPartitions(), entry.getKey(), entry.getValue());

            TC_GATE_REQUEST_POST(event.getBid(), outEvent);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        counter++;

        if (counter % wordWindowSize == 0) { //wait for TC to finish processing all words in the window
            int num_events = tcEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TC_GATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TC_GATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                tcEvents.clear();
                tweetMap.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
