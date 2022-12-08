package common.bolts.transactional.ed.trg;

import combo.SINKCombo;
import common.param.ed.wu.WUEvent;
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
import static common.CONTROL.wordWindowSize;
import static common.CONTROL.tweetWindowSize;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;


public class TRGBolt_ts extends TRGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TRGBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<WUEvent> wuEvents;
    private int counter = 0;

    //To be used in Combo
    public TRGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public TRGBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        wuEvents = new ArrayDeque<>();
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
            WUEvent event = (WUEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                TR_GATE_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TR_GATE_REQUEST_CONSTRUCT(WUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        LOG.info("Constructing TRG request: " + event.getBid());
        wuEvents.add(event);
    }

    private void TR_GATE_REQUEST_CORE() throws InterruptedException {}

    // Emit all events to collector, then insert one punctuation signal
    private void TR_GATE_REQUEST_POST() throws InterruptedException {
        for (WUEvent event : wuEvents) {
            TR_GATE_REQUEST_POST(event);
            LOG.info("Posting event: " + event.getBid());
        }
//        insertMaker(wuEvents.getLast());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        counter++;

        if (counter % wordWindowSize == 0) { //punctuation_interval = tweetWindowSize
            int num_events = wuEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TR_GATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TR_GATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                wuEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }
}
