package common.bolts.transactional.ed.tc;

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
import storage.SchemaRecordRef;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.TFIDF;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class TCBolt_ts extends TCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TCBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TCEvent> tcEvents;
    private double windowBoundary;
    ArrayDeque<Tuple> outWindowEvents;

    public TCBolt_ts(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCBolt_ts(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        tcEvents = new ArrayDeque<>();
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

        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TCEvent event = (TCEvent) input_event;

//            LOG.info("Added event: " + event.getMyBid() + " with _bid " + _bid);
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                TREND_CALCULATE_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TREND_CALCULATE_REQUEST_CONSTRUCT(TCEvent event, TxnContext txnContext) throws DatabaseException {
        //it simply constructs the operations and return.

        String[] wordTable = new String[]{"word_table"}; //condition source table
        String[] wordID = new String[]{event.getWordID()}; //condition source key
        TFIDF tfIdf = new TFIDF();
        Condition condition = new Condition(event.getWindowSize(), event.getWindowCount()); //arg1: windowSize, arg2: windowCount

//        LOG.info("Constructing TC request: " + event.getMyBid());
        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "word_table", // source_table
                event.getWordID(),  // source_key
                event.wordRecordRef, // record to be filled up from READ
                tfIdf, // append new tweetID to word's tweetList
                wordTable, wordID, //condition_source_table, condition_source_key
                condition,
                event.success,
                "ed_tc"
        );

        transactionManager.CommitTransaction(txnContext);
        tcEvents.add(event);
    }

    private void TREND_CALCULATE_REQUEST_CORE() {
        for (TCEvent event : tcEvents) {
            SchemaRecordRef ref = event.wordRecordRef;
            if (ref.isEmpty()) {
                LOG.info("Thead " + thread_Id + " reads empty word record: " + event.word);
                throw new NullPointerException();
            }
            event.tweetIDList = ref.getRecord().getValues().get(2).getStringList().toArray(new String[0]);
            event.isBurst = ref.getRecord().getValues().get(7).getBool();
        }
    }

    private void TREND_CALCULATE_REQUEST_POST() throws InterruptedException {
        for (TCEvent event : tcEvents) {
            TREND_CALCULATE_REQUEST_POST(event);
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
            int num_events = tcEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TREND_CALCULATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TREND_CALCULATE_REQUEST_POST();
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
                    TREND_CALCULATE_REQUEST_POST((TCEvent) outWindowTuple.getValue(0));
                    if (outWindowEvents.isEmpty()) { //stop itself when all stopping signals are posted
                        this.context.stop_running();
                    }

                } else { //otherwise, continue with normal-processing
                    execute_ts_normal(outWindowTuple);
                }
            }

            windowBoundary += tweetWindowSize;
            LOG.info("Thread " + this.thread_Id + " increment window boundary to: " + windowBoundary);

        }

    }

}
