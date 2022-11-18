package common.bolts.transactional.ed.es;

import combo.SINKCombo;
import common.bolts.transactional.ed.tr.TRBolt_ts;
import common.param.ed.es.ESEvent;
import common.param.ed.tr.TREvent;
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
import transaction.context.TxnContext;
import transaction.function.Division;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class ESBolt_ts extends ESBolt{
    private static final Logger LOG = LoggerFactory.getLogger(ESBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    //write-compute time pre-measured.
    ArrayDeque<ESEvent> esEvents;

    //To be used in Combo
    public ESBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    //To be used in ED Topology
    public ESBolt_ts(int fid) {
        super(LOG, fid, null);
    }

    //TODO: Copied from GSWBolt_ts
    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        esEvents = new ArrayDeque<>();
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
            ESEvent event = (ESEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            EVENT_SELECT_REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    protected void EVENT_SELECT_REQUEST_CONSTRUCT(ESEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {

        String[] conditionSourceTable = new String[]{"word_table"}; //condition source table
        String[] conditionSourceKey = new String[]{event.getClusterID()}; //condition source key
        Division function = new Division();

        transactionManager.BeginTransaction(txnContext);

        transactionManager.Asy_ModifyRecord_Read(txnContext,
                "cluster_table", // source_table
                event.getClusterID(),  // source_key
                event.getClusterRecord(), // record to be filled up from READ
                function,
                conditionSourceTable, conditionSourceKey,
                null, // no condition required
                event.success,
                "ed_es"
        );

        transactionManager.CommitTransaction(txnContext);

        esEvents.add(event);
    }

    private void EVENT_SELECT_REQUEST_CORE() {
        for (ESEvent event : esEvents) {
            SchemaRecordRef ref = event.getClusterRecord();
            if (ref.isEmpty()) {
                continue; //not yet processed.
            }
            event.wordList = ref.getRecord().getValues().get(1).getStringList().toArray(new String[0]);
            event.isEvent = ref.getRecord().getValues().get(5).getBool();
        }
    }

    private void EVENT_SELECT_REQUEST_POST() throws InterruptedException {
        for (ESEvent event : esEvents) {
            EVENT_SELECT_REQUEST_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {

        if (in.isMarker()) {
            int num_events = esEvents.size();
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            {
                MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
                {
                    transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
                    EVENT_SELECT_REQUEST_CORE();
                }
                MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
                BEGIN_POST_TIME_MEASURE(thread_Id);
                {
                    EVENT_SELECT_REQUEST_POST();
                }
                END_POST_TIME_MEASURE_ACC(thread_Id);

                //all tuples in the holder is finished.
                esEvents.clear();
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, num_events);
        } else {
            execute_ts_normal(in);
        }
    }

}
