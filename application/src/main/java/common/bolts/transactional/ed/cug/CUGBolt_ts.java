package common.bolts.transactional.ed.cug;

import combo.SINKCombo;
import common.param.ed.cu.CUEvent;
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

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_latency_measurement;
import static common.CONTROL.tweetWindowSize;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;
import static common.bolts.transactional.ed.PunctuationAligner.*;

public class CUGBolt_ts extends CUGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CUGBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<CUEvent> cuEvents;
    private int starting_bid = 0;
    private int counter = 0;

    //To be used in Combo
    public CUGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public CUGBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        cuEvents = new ArrayDeque<>();
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
            CUEvent event = (CUEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                CU_GATE_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void CU_GATE_REQUEST_CONSTRUCT(CUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        cuEvents.add(event);
    }

    private void CU_GATE_REQUEST_CORE() throws InterruptedException {}

    // Emit all events to collector
    private void CU_GATE_REQUEST_POST() throws InterruptedException, DatabaseException {
        CUEvent firstEvent = cuEvents.poll();
        assert firstEvent != null;
        CU_GATE_REQUEST_POST(firstEvent.getMyBid());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        counter++;

        if (counter % tweetWindowSize == 0) { //punctuation_interval = tweetWindowSize
            int num_events = cuEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    CU_GATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    CU_GATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                cuEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }

}
