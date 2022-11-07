package common.bolts.transactional.ed.tc;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import common.param.ed.tc.TCEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.CONTROL.enable_log;
import static profiler.MeasureTools.*;

public class TCBolt_ts extends TCBolt{
    private static final Logger LOG= LoggerFactory.getLogger(TCBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TCEvent> tcEvents;
//    Collection<TCEvent> tcEvents;

    public TCBolt_ts(int fid, SINKCombo sink){
        super(LOG,fid,sink);
    }
    public TCBolt_ts(int fid){
        super(LOG,fid,null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        int numberOfStates = config.getInt("NUM_ITEMS");
        //TODO: Correct the last argument config.getString("scheduler", "ED")), register in config
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                numberOfStates, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "ED"));
        tcEvents = new ArrayDeque<>();
    }

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
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TCEvent event = (TCEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event != null) {
                TC_REQUEST_CONSTRUCT((TCEvent) event, txnContext);
            } else {
                throw new UnknownError();
            }
            MeasureTools.END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);
        }
    }

    protected void TC_REQUEST_CONSTRUCT(TCEvent event, TxnContext txnContext) throws DatabaseException {
        //it simply constructs the operations and return.
        transactionManager.BeginTransaction(txnContext);
        //define txn operation here

        transactionManager.CommitTransaction(txnContext);
        tcEvents.add(event);
    }

    private void TC_REQUEST_CORE() throws InterruptedException {
        for (TCEvent event : tcEvents) {
            //TODO: Put windowSize and windowCount outside of wordTable
            SchemaRecord frequencyRecord = event.frequencyRecord.getRecord(); //Retrieve the READ result from Asy_WindowReadRecords(), and pass to event.result.
            SchemaRecord windowSizeRecord = event.windowSizeRecord.getRecord(); //TODO: We may not need to READ here, just update tf-idf is enough, use Asy_ModifyRecord().
            SchemaRecord countOccurWindowRecord = event.countOccurWindowRecord.getRecord(); //TODO: Define the tf-idf calculation as a function and pass to Request, not inside POST().
            SchemaRecord windowCountRecord = event.windowCountRecord.getRecord();

            if (frequencyRecord == null || windowSizeRecord == null || countOccurWindowRecord == null || windowCountRecord == null) {
                if (enable_log) LOG.debug(event.getBid() + " | " + event.getWordId());
            }

            //Pass read results to event, they will be used to compute tf-idf in TC_REQUEST_POST().
            event.frequency = Integer.parseInt(frequencyRecord.getValue().getString().trim());
            event.windowSize = Integer.parseInt(windowSizeRecord.getValue().getString().trim());
            event.countOccurWindow = Integer.parseInt(countOccurWindowRecord.getValue().getString().trim());
            event.windowCount = Integer.parseInt(windowCountRecord.getValue().getString().trim());
        }
    }

    private void TC_REQUEST_POST() throws InterruptedException {
        for (TCEvent event : tcEvents) {
            TC_REQUEST_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int num_events = tcEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    TC_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    TC_REQUEST_POST();
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
