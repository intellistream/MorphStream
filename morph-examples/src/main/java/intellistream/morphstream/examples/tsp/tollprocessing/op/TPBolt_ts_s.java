package intellistream.morphstream.examples.tsp.tollprocessing.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.tollprocessing.events.TPTxnEvent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.engine.txn.profiler.Metrics.NUM_ITEMS;

public class TPBolt_ts_s extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts_s.class);
    ArrayDeque<TPTxnEvent> TPTxnEvents;

    public TPBolt_ts_s(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }

    public TPBolt_ts_s(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BF"));
        TPTxnEvents = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            int readSize = TPTxnEvents.size();
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            {
                transactionManager.start_evaluate(thread_Id, in.getBID(), readSize);
                REQUEST_REQUEST_CORE();
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
            MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
            {
                REQUEST_POST();
            }
            MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
            //all tuples in the holder is finished.
            TPTxnEvents.clear();
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize);
        } else {
            execute_ts_normal(in);
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + _combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TPTxnEvent event = (TPTxnEvent) input_event;
            if (enable_latency_measurement) {
                (event).setTimestamp(timestamp);
            }
            REQUEST_CONSTRUCT(event, txnContext);
        }
    }

    protected void REQUEST_CONSTRUCT(TPTxnEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.BeginTransaction(txnContext);
//        transactionManager.Asy_ModifyRecord_Read(txnContext
//                , "segment_speed"
//                , String.valueOf(event.getPOSReport().getSegment())
//                , event.speed_value//holder to be filled up.
//                , new AVG(event.getPOSReport().getSpeed())
//        );          //asynchronously return.
        transactionManager.Asy_WriteRecord(txnContext
                , "segment_speed", String.valueOf(event.getPOSReport().getSegment()), null
        );
        transactionManager.Asy_WriteRecord(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment()), null
        );          //asynchronously return.
        transactionManager.CommitTransaction(txnContext);
        TPTxnEvents.add(event);
    }

    protected void REQUEST_REQUEST_CORE() {
        for (TPTxnEvent event : TPTxnEvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(TPTxnEvent event) {
        if (event.success[0] != 0) {
            event.count = event.count_value.getRecord().getValue().getInt();
            event.lav = event.speed_value.getRecord().getValue().getDouble();
        }
    }

    protected void REQUEST_POST() throws InterruptedException {
        for (TPTxnEvent event : TPTxnEvents) {
            REQUEST_POST(event);
        }
    }
}
