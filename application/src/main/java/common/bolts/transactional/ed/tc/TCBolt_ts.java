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
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;

public class TCBolt_ts extends TCBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TCBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TCEvent> tcEvents;

    public TCBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TCBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    //TODO: Copied from GSWBolt_ts
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        tcEvents = new ArrayDeque<>();
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
    }

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

        LOG.info("Constructing TC request: " + event.getMyBid());

        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "word_table", // source_table
                event.getWordID(),  // source_key
                event.getWord_record(), // record to be filled up from READ
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
            SchemaRecordRef ref = event.getWord_record();
            if (ref.isEmpty()) {
                continue; //not yet processed.
            }
            event.isBurst = ref.getRecord().getValues().get(6).getBool();
            event.tweetIDList = ref.getRecord().getValues().get(1).getStringList().toArray(new String[0]);
        }
    }

    private void TREND_CALCULATE_REQUEST_POST() throws InterruptedException {
        for (TCEvent event : tcEvents) {
            TREND_CALCULATE_REQUEST_POST(event);
        }
    }

    private boolean doPunctuation() {
        return tcEvents.size() == wordWindowSize / tthread;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (doPunctuation()) {
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
        } else {
            execute_ts_normal(in);
        }
    }
}
