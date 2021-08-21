package common.bolts.transactional.tp;

import common.param.lr.LREvent;
import combo.SINKCombo;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.context.TxnContext;
import transaction.function.AVG;
import transaction.function.CNT;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.combo_bid_size;
import static common.CONTROL.enable_app_combo;
import static common.constants.TPConstants.Constant.NUM_SEGMENTS;
import static profiler.MeasureTools.*;

/**
 * Combine Read-Write for TStream.
 */
public class TPBolt_ts extends TPBolt {
    private static final Logger LOG = LoggerFactory.getLogger(TPBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    ArrayDeque<LREvent> LREvents = new ArrayDeque<>();

    public TPBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public TPBolt_ts(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
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
            LREvent event = (LREvent) input_event;
            (event).setTimestamp(timestamp);
            REQUEST_CONSTRUCT(event, txnContext);
        }

    }

    protected void REQUEST_CONSTRUCT(LREvent event, TxnContext txnContext) throws DatabaseException {
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
        LREvents.add(event);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_SEGMENTS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int readSize = LREvents.size();
            transactionManager.start_evaluate(thread_Id, this.fid, readSize);//start lazy evaluation in transaction manager.
            REQUEST_REQUEST_CORE();
            REQUEST_POST();
            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {
            }
            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize);
            LREvents.clear();//clear stored events.
        } else {
            execute_ts_normal(in);
        }
    }

    protected void REQUEST_POST() throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (LREvent event : LREvents) {
            REQUEST_POST(event);
        }
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }

    protected void REQUEST_REQUEST_CORE() {
        for (LREvent event : LREvents) {
            TXN_REQUEST_CORE_TS(event);
        }
    }

    private void TXN_REQUEST_CORE_TS(LREvent event) {
        event.count = event.count_value.getRecord().getValue().getInt();
        event.lav = event.speed_value.getRecord().getValue().getDouble();
    }
}
