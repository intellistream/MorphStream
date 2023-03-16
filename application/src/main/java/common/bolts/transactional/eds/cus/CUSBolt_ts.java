package common.bolts.transactional.eds.cus;

import combo.SINKCombo;
import common.param.eds.cus.CUSEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentSkipListSet;

import static common.CONTROL.*;
import static common.CONTROL.tweetWindowSize;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class CUSBolt_ts extends CUSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(CUSBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<CUSEvent> cuEvents;
    private double windowBoundary;
    ArrayDeque<Tuple> outWindowEvents;

    //To be used in Combo
    public CUSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    //To be used in ED Topology
    public CUSBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        cuEvents = new ArrayDeque<>();
        windowBoundary = tweetWindowSize;
        outWindowEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {}

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            CUSEvent event = (CUSEvent) input_event;
            if (enable_latency_measurement) {
                (event).setTimestamp(timestamp);
            }
            CLUSTER_UPDATE_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    public static ConcurrentSkipListSet<Integer> cuTweets = new ConcurrentSkipListSet<>();

    protected void CLUSTER_UPDATE_REQUEST_CONSTRUCT(CUSEvent event, TxnContext txnContext) throws DatabaseException {

        cuTweets.add(Integer.parseInt(event.getTweetID()));

        String[] tweetTable = new String[]{"tweet_table"}; //condition source table
        String[] tweetKey = new String[]{event.getTweetID()}; //condition source key - tweet to be merged into cluster
        Condition condition1 = new Condition(event.getCurrWindow()); //arg1: currentWindow

//        LOG.info("Constructing CU request: " + event.getMyBid());
        transactionManager.BeginTransaction(txnContext);

        String clusterID = event.getClusterID();
        if (clusterID == null) {
            LOG.info("Null cluster ID detected");
            throw new NoSuchElementException();
        }

        // Update cluster: merge input tweet into existing cluster, or initialize new cluster
        transactionManager.Asy_ModifyRecord(
                txnContext,
                "cluster_table", // source_table
                event.getClusterID(),  // source_key
                null, // no function required
                tweetTable, tweetKey, //condition_source_table, condition_source_key
                condition1,
                event.success,
                "eds_cus"
        );

        transactionManager.CommitTransaction(txnContext);
        cuEvents.add(event);
    }

    private void CLUSTER_UPDATE_REQUEST_CORE() {}

    private void CLUSTER_UPDATE_REQUEST_POST() throws InterruptedException {
        for (CUSEvent event : cuEvents) {
            CLUSTER_UPDATE_REQUEST_POST(event);
        }
    }

//    public static AtomicInteger cuEventCount = new AtomicInteger(0);

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
//        cuEventCount.incrementAndGet();

        double bid = in.getBID();
//        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) {
//            LOG.info("Thread " + this.thread_Id + " detects out-window event: " + in.getBID());
            outWindowEvents.add(in);
        } else {
            execute_ts_normal(in);
        }

        if (outWindowEvents.size() == tthread) { //no more current-window-events in all receive_queues
//            LOG.info("Thread " + this.thread_Id + " has reached punctuation: " + windowBoundary);
            int num_events = cuEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    CLUSTER_UPDATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    CLUSTER_UPDATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                cuEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);

            //normal-process the previous out-of-window events
            while (!outWindowEvents.isEmpty()) {
                Tuple outWindowTuple = outWindowEvents.poll();
                if (outWindowTuple.getBID() >= total_events) {//if the out-of-window events are stopping signals, directly pass to downstream
                    CLUSTER_UPDATE_REQUEST_POST((CUSEvent) outWindowTuple.getValue(0));
                    if (outWindowEvents.isEmpty()) { //stop itself when all stopping signals are posted
                        this.context.stop_running();
                    }

//                    LOG.info("CU unique tweet count: " + cuTweets);
//                    LOG.info("OP TR updated tweet set: " + OPScheduler.updatedTweets);

                } else { //otherwise, continue with normal-processing
                    execute_ts_normal(outWindowTuple);
                }
            }

            windowBoundary += tweetWindowSize;

        }

    }
}
