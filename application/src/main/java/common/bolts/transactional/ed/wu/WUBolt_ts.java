package common.bolts.transactional.ed.wu;

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
import transaction.function.Append;
import transaction.function.Condition;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class WUBolt_ts extends WUBolt{
    private static final Logger LOG = LoggerFactory.getLogger(WUBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<WUEvent> wuEvents;

    //To be used in Combo
    public WUBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public WUBolt_ts(int fid) {
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
                WORD_UPDATE_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void WORD_UPDATE_REQUEST_CONSTRUCT(WUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        String[] wordTable = new String[]{"word_table"}; //condition source table
        String[] wordID = new String[]{event.getWordID()}; //condition source key
        Append append = new Append(event.getTweetID());
        Condition condition = new Condition(event.getCurrWindow(), event.getWord()); //arg1, stringArg1

        String sourceTable = "word_table";
        String sourceKey = event.getWordID();

        LOG.info("Constructing WU request: " + event.getMyBid());

        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord(txnContext,
                sourceTable, // source_table
                sourceKey,  // source_key
                append, // append new tweetID to word's tweetList
                wordTable, wordID, //condition_source_table, condition_source_key
                condition,
                event.success,
                "ed_wu"
                );

        transactionManager.CommitTransaction(txnContext);

        wuEvents.add(event);
    }

    private void WORD_UPDATE_REQUEST_CORE() throws InterruptedException {}

    private void WORD_UPDATE_REQUEST_POST() throws InterruptedException {
        for (WUEvent event : wuEvents) {
            WORD_UPDATE_REQUEST_POST(event);
        }
    }

    private boolean doPunctuation() {
        return wuEvents.size() == wordWindowSize / tthread;
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (doPunctuation()) {
            int num_events = wuEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    WORD_UPDATE_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    WORD_UPDATE_REQUEST_POST();
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
