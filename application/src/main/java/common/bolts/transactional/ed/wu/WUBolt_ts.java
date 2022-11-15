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
import storage.SchemaRecord;
import storage.SchemaRecordRef;
import storage.datatype.*;
import transaction.context.TxnContext;
import transaction.function.Append;
import transaction.function.Condition;
import transaction.function.INC;
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

    //TODO: Copied from GSWBolt_ts
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        wuEvents = new ArrayDeque<>();
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
            WUEvent event = (WUEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            WORD_UPDATE_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    private SchemaRecord WordRecord(String wordValue, HashSet tweetList, int countOccurWindow, double tfIdf, int lastOccurWindow, int frequency) {
        List<DataBox> values = new ArrayList<>();
        values.add(new StringDataBox(wordValue));       //Primary key
        values.add(new HashSetDataBox(tweetList));
        values.add(new IntDataBox(countOccurWindow));
        values.add(new DoubleDataBox(tfIdf));
        values.add(new IntDataBox(lastOccurWindow));
        values.add(new IntDataBox(frequency));
        return new SchemaRecord(values);
    }

    protected void WORD_UPDATE_REQUEST_CONSTRUCT(WUEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {


        transactionManager.BeginTransaction(txnContext);

        //TODO: Define TxnOperation for WU

        String[] wordTable = new String[]{"word_table"}; //condition source table
        String[] wordID = new String[]{event.getWord()}; //condition source key
        Append append = new Append(event.getTweetID());
        Condition condition = new Condition(event.getCurrWindow(), event.getWord()); //pass the current window info to scheduler

        transactionManager.Asy_ModifyRecord(txnContext,
                "word_table", // source_table
                event.getWord(),  // source_key
                append, // append new tweetID to word's tweetList
                wordTable, wordID, //condition_source_table, condition_source_key
                condition,
                event.success
                );

        transactionManager.CommitTransaction(txnContext);

        wuEvents.add(event);
    }

    private void WORD_UPDATE_REQUEST_CORE() throws InterruptedException {

        //TODO: Define CORE for WU

        for (WUEvent event : wuEvents) {
//
//            SchemaRecordRef tweetRecordRef = event.tweetRecordRef;
//
//            //INSERT failed
//            if (tweetRecordRef == null) {
//                if (enable_log) LOG.debug(event.getBid() + " | " + Arrays.toString(event.getWords()) + "INSERT failed");
//            }
//
//            //INSERT success, pass read result to event. It will be referenced in REQUEST_POST
//            if (tweetRecordRef != null) {
//                event.tweetIDResult = tweetRecordRef.getRecord().getValues().get(1).getString().trim(); //Add read result to event.result
//            }
        }
    }

    private void WORD_UPDATE_REQUEST_POST() throws InterruptedException {
        for (WUEvent event : wuEvents) {
            WORD_UPDATE_REQUEST_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
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
