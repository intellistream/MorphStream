package intellistream.morphstream.examples.tsp.tollprocessing.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.tollprocessing.events.TPTxnEvent;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.AVG;
import intellistream.morphstream.engine.txn.transaction.function.CNT;
import intellistream.morphstream.engine.txn.transaction.impl.ordered.TxnManagerTStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static intellistream.morphstream.common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static intellistream.morphstream.configuration.CONTROL.combo_bid_size;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.*;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_ts extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<TPTxnEvent> TPTxnEvents = new ArrayDeque<>();

    public TPBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);

    }

    public TPBolt_ts(int fid) {
        super(LOG, fid, null);

    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID()
                , context.getGraph());
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException {
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TPTxnEvent event = (TPTxnEvent) input_event;
            (event).setTimestamp(timestamp);
            REQUEST_CONSTRUCT(event, txnContext);
        }

    }

    protected void REQUEST_CONSTRUCT(TPTxnEvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_speed"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.speed_value//holder to be filled up.
                , new AVG(event.getPOSReport().getSpeed())
        );          //asynchronously return.
        transactionManager.Asy_ModifyRecord_Read(txnContext
                , "segment_cnt"
                , String.valueOf(event.getPOSReport().getSegment())
                , event.count_value//holder to be filled up.
                , new CNT(event.getPOSReport().getVid())
        );          //asynchronously return.
        TPTxnEvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id,
                NUM_SEGMENTS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int readSize = TPTxnEvents.size();
            transactionManager.start_evaluate(thread_Id, this.fid, readSize);//start lazy evaluation in transaction manager.
            REQUEST_REQUEST_CORE();
            REQUEST_POST();
            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize);
            TPTxnEvents.clear();//clear stored events.
        } else {
            execute_ts_normal(in);
        }
    }

    protected void REQUEST_POST() throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (TPTxnEvent event : TPTxnEvents) {
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }

    protected void REQUEST_REQUEST_CORE() {
        for (TPTxnEvent event : TPTxnEvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(TPTxnEvent event) {
        event.count = event.count_value.getRecord().getValue().getInt();
        event.lav = event.speed_value.getRecord().getValue().getDouble();
    }
}