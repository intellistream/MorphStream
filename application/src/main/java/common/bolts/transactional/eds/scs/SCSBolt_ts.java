package common.bolts.transactional.eds.scs;

import combo.SINKCombo;
import common.param.eds.scs.SCSEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecordRef;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Similarity;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.CONTROL.tweetWindowSize;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class SCSBolt_ts extends SCSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SCSBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<SCSEvent> scEvents;
    ArrayDeque<Tuple> outWindowEvents;
    private double windowBoundary;
    private double tweetIDFloor;
    private double recordUpdateTime;

    //To be used in Combo
    public SCSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public SCSBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        scEvents = new ArrayDeque<>();
        outWindowEvents = new ArrayDeque<>();
        windowBoundary = tweetWindowSize;
        tweetIDFloor = 0;
        recordUpdateTime = 0;
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
            SCSEvent event = (SCSEvent) input_event;
            if (enable_latency_measurement) {
                (event).setTimestamp(timestamp);
            }
            SIMILARITY_CALCULATE_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    protected void SIMILARITY_CALCULATE_REQUEST_CONSTRUCT(SCSEvent event, TxnContext txnContext) throws DatabaseException {
        String[] clusterTable = new String[]{"cluster_table"}; //condition source table to iterate
        String[] clusterKey = new String[]{String.valueOf(0)}; //condition source key, set to zero
        Similarity function = new Similarity();
//        Condition condition1 = new Condition(event.getCurrWindow(), event.isBurst()); //arg1: currentWindow, boolArg1: isBurst
        Condition condition1 = new Condition(event.getCurrWindow(), true); //TODO: Set to always true for testing

        boolean doUpdate = false;
        if (recordUpdateTime <= tweetIDFloor) {
            doUpdate = true;
            recordUpdateTime = (int) event.getBid();
            LOG.info("Thread " + thread_Id + " do record update: " + (int) event.getBid() + " with tweetIDFloor " + tweetIDFloor);
        }

        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Iteration_Read(txnContext,
                "tweet_table", // source_table
                event.getTweetID(),  // source_key
                event.tweetRecord, // record to read from
                function, // determine the most similar cluster
                clusterTable, clusterKey, //condition_source_table, condition_source_key
                condition1,
                event.success,
                "eds_scs",
                doUpdate
        );

        transactionManager.CommitTransaction(txnContext);
        scEvents.add(event);
    }

    private void SIMILARITY_CALCULATE_REQUEST_CORE() {

        for (SCSEvent event : scEvents) {
            SchemaRecordRef ref = event.tweetRecord;
            if (ref.isEmpty()) {
                LOG.info("Thead " + thread_Id + " reads empty tweet record");
                throw new NoSuchElementException();
            }
            event.targetClusterID = ref.getRecord().getValues().get(2).toString();
        }
//        LOG.info("TxnManager counter: " + TxnManagerDedicatedAsy.scTMCounter.get());
//        LOG.info("OPSchedulerContext Push counter: " + OPSchedulerContext.opsContextPushCounter.get());
//        LOG.info("OP SC Ref counter: " + OPScheduler.opSCRefCounter.get());

    }

    private void SIMILARITY_CALCULATE_REQUEST_POST() throws InterruptedException {
        for (SCSEvent event : scEvents) {
            SIMILARITY_CALCULATE_REQUEST_POST(event);
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
//            LOG.info("Thread " + this.thread_Id + " has reached punctuation: " + windowBoundary);
            int num_events = scEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    SIMILARITY_CALCULATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    SIMILARITY_CALCULATE_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                scEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);

            //normal-process the previous out-of-window events
            while (!outWindowEvents.isEmpty()) {
                Tuple outWindowTuple = outWindowEvents.poll();
                if (outWindowTuple.getBID() >= total_events) {//if the out-of-window events are stopping signals, directly pass to downstream
                    SIMILARITY_CALCULATE_REQUEST_POST((SCSEvent) outWindowTuple.getValue(0));
                    if (outWindowEvents.isEmpty()) { //stop itself when all stopping signals are posted
                        this.context.stop_running();
                    }

                } else { //otherwise, continue with normal-processing
                    execute_ts_normal(outWindowTuple);
                }
            }

            tweetIDFloor += tweetWindowSize;
            windowBoundary += tweetWindowSize;

        }

    }
}