package common.bolts.transactional.gs;

import common.param.mb.MicroEvent;
import common.sink.SINKCombo;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Tuple;
import faulttolerance.impl.ValueState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transaction.dedicated.ordered.TxnManagerTStream;
import transaction.impl.TxnContext;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static profiler.MeasureTools.*;
import static profiler.Metrics.NUM_ITEMS;

public class GSBolt_ts extends GSBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GSBolt_ts.class);
    private static final long serialVersionUID = -5968750340131744744L;
    private final double write_useful_time = 556;//write-compute time pre-measured.
    Collection<MicroEvent> EventsHolder;
    private int writeEvents;

    public GSBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public GSBolt_ts(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    /**
     * THIS IS ONLY USED BY TSTREAM.
     * IT CONSTRUCTS and POSTPONES TXNS.
     */
    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            MicroEvent event = (MicroEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            boolean flag = event.READ_EVENT();
            if (flag) {//read
                read_construct(event, txnContext);
            } else {
                write_construct(event, txnContext);
            }
        }
        END_PRE_TXN_TIME_MEASURE(thread_Id);
    }

    void read_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES; i++) {
            //it simply constructs the operations and return.
            transactionManager.Asy_ReadRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getRecord_refs()[i], event.enqueue_time);
        }
        if (enable_speculative) {//TODO: future work.
            //earlier emit
            //collector.emit(input_event.getBid(), 1, input_event.getTimestamp());//the tuple is finished.
        } else {
            EventsHolder.add(event);//mark the tuple as ``in-complete"
        }
    }

    protected void write_construct(MicroEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES; ++i) {
            //it simply construct the operations and return.
            transactionManager.Asy_WriteRecord(txnContext, "MicroTable", String.valueOf(event.getKeys()[i]), event.getValues()[i], event.enqueue_time);//asynchronously return.
        }
        writeEvents++;
        //post_process for write events immediately.
        BEGIN_POST_TIME_MEASURE(thread_Id);
        WRITE_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks());
        EventsHolder = new ArrayDeque<>();
    }

    void READ_REQUEST_CORE() throws InterruptedException {
//        while (!EventsHolder.isEmpty() && !Thread.interrupted()) {
//            MicroEvent input_event = EventsHolder.remove();
//            if (!READ_REQUEST_CORE(input_event))
//                EventsHolder.offer(input_event);
//            else {
//                BEGIN_POST_TIME_MEASURE(thread_Id);
//                BUYING_REQUEST_POST(input_event);
//                END_POST_TIME_MEASURE_ACC(thread_Id);
//            }
//        }
        for (MicroEvent event : EventsHolder) {
            READ_CORE(event);
        }
    }

    void READ_POST() throws InterruptedException {
        for (MicroEvent event : EventsHolder) {
            READ_POST(event);
        }
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int readSize = EventsHolder.size();
            BEGIN_TXN_TIME_MEASURE(thread_Id);
            BEGIN_TXN_PROCESSING_TIME_MEASURE(thread_Id);
            transactionManager.start_evaluate(thread_Id, in.getBID());//start lazy evaluation in transaction manager.
            END_TXN_PROCESSING_TIME_MEASURE(thread_Id);// overhead_total TP time.
            BEGIN_ACCESS_TIME_MEASURE(thread_Id);
            READ_REQUEST_CORE();
            END_ACCESS_TIME_MEASURE_TS(thread_Id, readSize, write_useful_time, writeEvents);//overhead_total compute time.
            END_TXN_TIME_MEASURE_TS(thread_Id, write_useful_time * writeEvents);//overhead_total txn time.
            READ_POST();
            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {
            }
            //post_process for events left-over.
            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + writeEvents);
            EventsHolder.clear();//all tuples in the EventsHolder are finished.
            if (enable_profile)
                writeEvents = 0;//all tuples in the holder are finished.
        } else {
            execute_ts_normal(in);
        }
    }
}
