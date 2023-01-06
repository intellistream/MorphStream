package common.bolts.transactional.ed.tcg;

import combo.SINKCombo;
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
import utils.lib.ConcurrentHashMap;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

import static common.CONTROL.*;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;

public class TCGBolt_ts extends TCGBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCGBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TCEvent> tcEvents;
    static ConcurrentHashMap<String, Boolean> tweetBurstMap; //Maps tweetID -> isBurst
    static ConcurrentHashMap<String, TCEvent> tweetEventMap; //Maps tweetID -> One related TCEvent
    private double windowBoundary;
    private int tweetIDFloor;
    ArrayDeque<Tuple> outWindowEvents;

    //To be used in Combo
    public TCGBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public TCGBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        tcEvents = new ArrayDeque<>();
        tweetBurstMap = new ConcurrentHashMap<>();
        tweetEventMap = new ConcurrentHashMap<>();
        windowBoundary = tweetWindowSize;
        tweetIDFloor = 0;
        outWindowEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {}

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
            TC_GATE_REQUEST_CONSTRUCT(event, txnContext);

            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TC_GATE_REQUEST_CONSTRUCT(TCEvent event, TxnContext txnContext) {
        tcEvents.add(event);
        tweetEventMap.putIfAbsent(event.getTweetID(), event);

        if (event.tweetIDList != null) {
            for (String tweetID : event.tweetIDList) {
                if (Integer.parseInt(tweetID) < tweetIDFloor || Integer.parseInt(tweetID) >= windowBoundary) { //Only consider tweets in the current window
                    continue;
                }
                if (!Boolean.TRUE.equals(tweetBurstMap.get(tweetID))) { //Do not update when the value is "not-null and true"
                    tweetBurstMap.put(tweetID, event.isBurst);
                }
                assert tweetBurstMap.size() <= tweetWindowSize;
            }
        } else {
            LOG.info("No tweetIDList found for event " + event.getBid());
            throw new NoSuchElementException();
        }
    }

    private void TC_GATE_REQUEST_CORE() {}

    private void TC_GATE_REQUEST_POST() throws InterruptedException {

        int tweet_partition_interval = (int) Math.ceil(tweetWindowSize / (double) tthread);
        int tweet_left_bound = thread_Id * tweet_partition_interval + tweetIDFloor;
        int tweet_right_bound;
        if (thread_Id == tthread - 1) {//last executor need to handle left-over
            tweet_right_bound = tweetWindowSize + tweetIDFloor;
        } else {
            tweet_right_bound = (thread_Id + 1) * tweet_partition_interval + tweetIDFloor;
        }

        for (int i=tweet_left_bound; i<tweet_right_bound; i++) {
            String tweetID = String.valueOf(i);
            TCEvent event = tweetEventMap.get(tweetID);
            Boolean isBurst = tweetBurstMap.get(tweetID);

            tweetEventMap.remove(tweetID);
            tweetBurstMap.remove(tweetID); //TODO: Improve this

            if (isBurst == null || event == null) {
                throw new NoSuchElementException();
            }

            TC_GATE_REQUEST_POST(event, isBurst);
        }

    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        double bid = in.getBID();
//        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) {
//            LOG.info("Thread " + this.thread_Id + " detects out-window event: " + in.getBID());
            outWindowEvents.add(in);

        } else {
            execute_ts_normal(in);
        }

        if (outWindowEvents.size() == tthread) { //no more current-window-events in all receive_queues
            LOG.info("Thread " + this.thread_Id + " has reached punctuation: " + windowBoundary);
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
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);

            //normal-process the previous out-of-window events
            while (!outWindowEvents.isEmpty()) {
                Tuple outWindowTuple = outWindowEvents.poll();

                if (outWindowTuple.getBID() >= total_events) {//if the out-of-window events are stopping signals, directly pass to downstream
                    TC_GATE_REQUEST_POST((TCEvent) outWindowTuple.getValue(0), false);
                } else {
                    execute_ts_normal(outWindowTuple); //continue with normal-processing
                }
            }

            tweetIDFloor += tweetWindowSize;
            windowBoundary += tweetWindowSize;
//            LOG.info("Thread " + this.thread_Id + " increment window boundary to: " + windowBoundary);

        }

    }

}
