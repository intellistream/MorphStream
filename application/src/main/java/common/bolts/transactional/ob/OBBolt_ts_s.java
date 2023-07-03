package common.bolts.transactional.ob;

import combo.SINKCombo;
import common.param.TxnEvent;
import common.param.ob.AlertEvent;
import common.param.ob.BuyingEvent;
import common.param.ob.ToppingEvent;
import components.context.TopologyContext;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import profiler.MeasureTools;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.DEC;
import transaction.function.INC;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.CONTROL.*;
import static common.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static profiler.MeasureTools.END_POST_TIME_MEASURE_ACC;
import static profiler.Metrics.NUM_ITEMS;

public class OBBolt_ts_s extends OBBolt {
    private static final Logger LOG= LoggerFactory.getLogger(OBBolt_ts_s.class);
    ArrayDeque<BuyingEvent> buyingEvents = new ArrayDeque<>();
    private int alertEvents = 0, toppingEvents = 0;
    public OBBolt_ts_s(int fid, SINKCombo sink) {
        super(LOG, fid, sink);
    }
    public OBBolt_ts_s(int fid) {
        super(LOG, fid, null);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ITEMS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
    }
    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()){
            int readSize= buyingEvents.size();
            int num_events=readSize+alertEvents+toppingEvents;
            /**
             *  MeasureTools.BEGIN_TOTAL_TIME_MEASURE(thread_Id); at {@link #execute_ts_normal(Tuple)}}.
             */
            MeasureTools.BEGIN_TXN_TIME_MEASURE(thread_Id);
            {
                transactionManager.start_evaluate(thread_Id,in.getBID(),num_events);
                BUYING_REQUEST_CORE();
            }
            MeasureTools.END_TXN_TIME_MEASURE(thread_Id);
            MeasureTools.BEGIN_POST_TIME_MEASURE(thread_Id);
            BUYING_REQUEST_POST();
            MeasureTools.END_POST_TIME_MEASURE_ACC(thread_Id);
            buyingEvents.clear();//all tuples in the EventsHolder are finished.
            if (enable_profile) {//all tuples in the holder are finished.
                alertEvents = 0;
                toppingEvents = 0;
            }
            MeasureTools.END_TOTAL_TIME_MEASURE_TS(thread_Id,num_events);
        }else {
            execute_ts_normal(in);
        }
    }

    @Override
    protected void PRE_TXN_PROCESS(long bid, long timestamp) throws DatabaseException, InterruptedException {
        MeasureTools.BEGIN_PRE_TXN_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnContext txnContext = new TxnContext(thread_Id, this.fid, i);
            TxnEvent event = (TxnEvent) input_event;
            if (enable_latency_measurement)
                (event).setTimestamp(timestamp);
            if (event instanceof BuyingEvent) {
                BUYING_REQUEST_CONSTRUCT((BuyingEvent) event, txnContext);
            } else if (event instanceof AlertEvent) {
                ALERT_REQUEST_CONSTRUCT((AlertEvent) event, txnContext);
            } else {
                TOPPING_REQUEST_CONSTRUCT((ToppingEvent) event, txnContext);
            }
        }
    }
    private void BUYING_REQUEST_CONSTRUCT(BuyingEvent event, TxnContext txnContext) throws DatabaseException {
        //it simply construct the operations and return.
        transactionManager.BeginTransaction(txnContext);
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; i++) {
            //it simply constructs the operations and return.
            //condition on itself.
            transactionManager.Asy_ModifyRecord(//TODO: addOperation atomicity preserving later.
                    txnContext,
                    "goods",
                    String.valueOf(event.getItemId()[i]),
                    new DEC(event.getBidQty(i)),
                    new Condition(event.getBidPrice(i), event.getBidQty(i)),
                    event.success
            );
        }
        transactionManager.CommitTransaction(txnContext);
        buyingEvents.add(event);
    }
    protected void ALERT_REQUEST_CONSTRUCT(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.BeginTransaction(txnContext);
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_WriteRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.getAsk_price()[i], 1);//asynchronously return.
        BEGIN_POST_TIME_MEASURE(thread_Id);
        ALERT_REQUEST_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
        transactionManager.CommitTransaction(txnContext);
        alertEvents++;
    }
    protected void TOPPING_REQUEST_CONSTRUCT(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.BeginTransaction(txnContext);
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_ModifyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), new INC(event.getItemTopUp()[i]), 2);//asynchronously return.
        BEGIN_POST_TIME_MEASURE(thread_Id);
        TOPPING_REQUEST_POST(event);
        END_POST_TIME_MEASURE_ACC(thread_Id);
        transactionManager.CommitTransaction(txnContext);
        toppingEvents++;
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
        event.biding_result = new BidingResult(event, event.success[0] == NUM_ACCESSES_PER_BUY);
    }
    private void BUYING_REQUEST_POST() throws InterruptedException {
        for (BuyingEvent event : buyingEvents) {
            BUYING_REQUEST_POST(event);
        }
    }
}
