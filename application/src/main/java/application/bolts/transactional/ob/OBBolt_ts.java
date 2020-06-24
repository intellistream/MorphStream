package application.bolts.transactional.ob;


import application.param.TxnEvent;
import application.param.ob.AlertEvent;
import application.param.ob.BuyingEvent;
import application.param.ob.ToppingEvent;
import application.sink.SINKCombo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.execution.runtime.tuple.impl.Tuple;
import sesame.faulttolerance.impl.ValueState;
import state_engine.DatabaseException;
import state_engine.transaction.dedicated.ordered.TxnManagerTStream;
import state_engine.transaction.function.Condition;
import state_engine.transaction.function.DEC;
import state_engine.transaction.function.INC;
import state_engine.transaction.impl.TxnContext;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static application.CONTROL.*;
import static application.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static state_engine.profiler.MeasureTools.*;
import static state_engine.profiler.Metrics.NUM_ITEMS;

public class OBBolt_ts extends OBBolt {
    private static final long serialVersionUID = -589295586738474236L;
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_ts.class);
    private final static double write_useful_time = 1556.713743100476;//write-compute time pre-measured.

    private int thisTaskId;
    final ArrayDeque<BuyingEvent> buyingEvents = new ArrayDeque<>();
    private int alertEvents = 0, toppingEvents = 0;


    public OBBolt_ts(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
        state = new ValueState();
    }

    public OBBolt_ts(int fid) {
        super(LOG, fid, null);
        state = new ValueState();
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        this.thisTaskId = thread_Id;
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks());


    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }

    protected void TOPPING_REQUEST_CONSTRUCT(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_ModifyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
        BEGIN_POST_TIME_MEASURE(thread_Id);
        TOPPING_REQUEST_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
        toppingEvents++;
    }

    protected void ALERT_REQUEST_CONSTRUCT(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_WriteRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
        BEGIN_POST_TIME_MEASURE(thread_Id);
        ALERT_REQUEST_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
        alertEvents++;
    }

    private void BUYING_REQUEST_CONSTRUCT(BuyingEvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            //it simply constructs the operations and return.
            //condition on itself.
            transactionManager.Asy_ModifyRecord(//TODO: add atomicity preserving later.
                    txnContext,
                    "goods",
                    String.valueOf(event.getItemId()[i]),
                    new DEC(event.getBidQty(i)),
                    new Condition(event.getBidPrice(i), event.getBidQty(i)),
                    event.success
            );
        }

        buyingEvents.add(event);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {

            int readSize = buyingEvents.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);// overhead_total TP time.

            BEGIN_ACCESS_TIME_MEASURE(thread_Id);

            BUYING_REQUEST_CORE();

            END_ACCESS_TIME_MEASURE_TS(thread_Id, readSize, write_useful_time, alertEvents + toppingEvents);//overhead_total compute time.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id, write_useful_time * toppingEvents);//overhead_total txn time.


//            BEGIN_POST_TIME_MEASURE(thread_Id);
            BUYING_REQUEST_POST();
//            END_POST_TIME_MEASURE_ACC(thread_Id);

            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {

            }

            //post_process for events left-over.

            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + alertEvents + toppingEvents);

            buyingEvents.clear();//all tuples in the EventsHolder are finished.
            if (enable_profile) {//all tuples in the holder are finished.
                alertEvents = 0;
                toppingEvents = 0;
            }

        } else {
            execute_ts_normal(in);
        }
    }


    protected void PRE_TXN_PROCESS(long _bid, long timestamp) throws DatabaseException, InterruptedException {
        BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);

        for (long i = _bid; i < _bid + combo_bid_size; i++) {

            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;

            (event).setTimestamp(timestamp);

            if (event instanceof BuyingEvent) {
                BUYING_REQUEST_CONSTRUCT((BuyingEvent) event, txnContext);
            } else if (event instanceof AlertEvent) {

                ALERT_REQUEST_CONSTRUCT((AlertEvent) event, txnContext);

            } else {
                TOPPING_REQUEST_CONSTRUCT((ToppingEvent) event, txnContext);
            }
        }

        END_PRE_TXN_TIME_MEASURE_ACC(thread_Id);

    }


    private void BUYING_REQUEST_POST() throws InterruptedException {
        for (BuyingEvent event : buyingEvents) {
            BUYING_REQUEST_POST(event);
        }
    }

    private void BUYING_REQUEST_CORE() {
        for (BuyingEvent event : buyingEvents) {
            BUYING_REQUEST_CORE(event);
        }
    }

    /**
     * Evaluation are pushed down..
     *
     * @param event
     */
    @Override
    protected void BUYING_REQUEST_CORE(BuyingEvent event) {
        //measure_end if any item is not able to buy.
        event.biding_result = new BidingResult(event, event.success[0]);
    }
}
