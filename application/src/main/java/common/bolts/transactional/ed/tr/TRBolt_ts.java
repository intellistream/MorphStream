package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.param.ed.tr.TREvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;

import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.Insert;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;


public class TRBolt_ts extends TRBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<TREvent> trEvents;
    private double windowBoundary;

    //To be used in Combo
    public TRBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    //To be used in ED Topology
    public TRBolt_ts(int fid) {
        super(LOG, fid, null);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) throws DatabaseException {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"), this.context.getStageMap().get(this.fid));
        trEvents = new ArrayDeque<>();
        windowBoundary = tweetWindowSize;
    }

    @Override
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) throws DatabaseException {
        loadDB(transactionManager.getSchedulerContext(),
                context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
        // Aqif: For TStream taskId increases by 1 and executorId is always 0.
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(double _bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TREvent event = (TREvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                TWEET_REGISTRANT_REQUEST_CONSTRUCT(event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TWEET_REGISTRANT_REQUEST_CONSTRUCT(TREvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        String[] tweetTable = new String[]{"tweet_table"}; //condition source table
        String[] tweetID = new String[]{event.getTweetID()}; //condition source key
        String sourceTable = "tweet_table";
        String sourceKey = event.getTweetID();
        Insert function = new Insert(event.getWords());

        transactionManager.BeginTransaction(txnContext);

        //Insert new tweet record by writing to corresponding invalid record
        transactionManager.Asy_ModifyRecord(txnContext,
                sourceTable, // source_table
                sourceKey,  // source_key
                function, // words in tweet
                tweetTable, tweetID, //condition_source_table, condition_source_key
                null, // no condition required
                event.success,
                "ed_tr"
        );

        transactionManager.CommitTransaction(txnContext);

        trEvents.add(event);
    }

    private void TWEET_REGISTRANT_REQUEST_CORE() throws InterruptedException {}

    private void TWEET_REGISTRANT_REQUEST_POST() throws InterruptedException {
        for (TREvent event : trEvents) {
            TWEET_REGISTRANT_REQUEST_POST(event);
//            LOG.info("Thread " + this.thread_Id + " is posting TR event: " + event.getBid());
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        double bid = in.getBID();
//        LOG.info("Thread " + this.thread_Id + " has event " + bid);

        if (bid >= windowBoundary) {// Input event is the last event in the current window
//            LOG.info("Thread " + this.thread_Id + " has reached punc before event: " + in.getBID());

            int num_events = trEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TWEET_REGISTRANT_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TWEET_REGISTRANT_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                trEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);

            windowBoundary += tweetWindowSize;
//            LOG.info("Thread " + this.thread_Id + " increment window boundary to: " + windowBoundary);

            // Upon receiving stopping signal, pass it to downstream
            if (bid >= total_events) {
                EMIT_STOP_SIGNAL((TREvent) in.getValue(0));
                // Stop itself
                this.context.stop_running();
            }

        }

        execute_ts_normal(in);

    }

    @Override
    public void execute() throws BrokenBarrierException, InterruptedException {
//        if (punctuation.get()) {
//            trBarrier.await();
            transactionManager.start_evaluate(thread_Id, -1, -1);//start lazy evaluation in transaction manager.
//            punctuation.set(false);
//            LOG.info("Thread " + this.thread_Id + " has event " + in.getBID());
//        }
    }

}
