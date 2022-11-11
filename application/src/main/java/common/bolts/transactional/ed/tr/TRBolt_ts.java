package common.bolts.transactional.ed.tr;

import combo.SINKCombo;
import common.bolts.transactional.sl.TransactionResult;
import common.param.ed.tr.TREvent;
import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;

import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.DataBox;
import storage.datatype.HashSetDataBox;
import storage.datatype.IntDataBox;
import storage.datatype.StringDataBox;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.CONTROL.enable_log;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;


public class TRBolt_ts extends TRBolt{
    private static final Logger LOG = LoggerFactory.getLogger(TRBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<TREvent> trEvents;

    //To be used in Combo
    public TRBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    //To be used in ED Topology
    public TRBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    //TODO: Copied from GSWBolt_ts
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        trEvents = new ArrayDeque<>();
    }

    //TODO: Copied from SLBolt_ts, check where this method is used, modify or remove accordingly
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(transactionManager.getSchedulerContext(),
                context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
        // Aqif: For TStream taskId increases by 1 and executorId is always 0.
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TREvent event = (TREvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            TWEET_REGISTRANT_CONSTRUCT(event, txnContext);
        }
    }

    //TODO: Implement HashMapDataBox datatype to replace HashSet
    private SchemaRecord TweetRecord(String tweetID, HashSet wordMap, int computeTime) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(tweetID));           //Primary key
        values.add(new HashSetDataBox(wordMap));
        values.add(new IntDataBox(computeTime));
        return new SchemaRecord(values);
    }

    //TODO: Complete the tweet registration request construct
    protected void TWEET_REGISTRANT_CONSTRUCT(TREvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        HashSet<String> wordHashSet = new HashSet<>(Arrays.asList(event.getWords())); //Convert String[] to HashSet
        SchemaRecord tweetRecord = TweetRecord(event.getTweetID(), wordHashSet, -1); //Set default computeTime to -1
        LinkedList<Long> gap = new LinkedList<>();

        transactionManager.BeginTransaction(txnContext);

        //TODO: Check the Insert operation in TxnManagerDedicatedAsy
        // What is the gap argument in this method?
        transactionManager.InsertRecordRead(txnContext, "tweet_table", tweetRecord, event.tweetRecordRef, gap);

        transactionManager.CommitTransaction(txnContext);

        trEvents.add(event);
    }

    //TODO
    // This method should read the INSERT result as new tweet's tweetID
    private void TWEET_REGISTRANT_REQUEST_CORE() throws InterruptedException {
        for (TREvent event : trEvents) {

            SchemaRecordRef tweetRecordRef = event.tweetRecordRef;

            //INSERT failed
            if (tweetRecordRef == null) {
                if (enable_log) LOG.debug(event.getBid() + " | " + Arrays.toString(event.getWords()) + "INSERT failed");
            }

            //INSERT success, pass read result to event. It will be referenced in REQUEST_POST
            if (tweetRecordRef != null) {
                event.tweetIDResult = tweetRecordRef.getRecord().getValues().get(1).getString().trim(); //Add read result to event.result
            }
        }
    }

    private void TWEET_REGISTRANT_REQUEST_POST() throws InterruptedException {
        for (TREvent event : trEvents) {
            TWEET_REGISTRANT_REQUEST_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
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
        } else {
            execute_ts_normal(in);
        }
    }

}
