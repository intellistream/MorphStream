package application.bolts.transactional.ob;


import application.param.ob.AlertEvent;
import application.param.ob.BuyingEvent;
import application.param.ob.ToppingEvent;
import application.sink.SINKCombo;
import sesame.components.context.TopologyContext;
import sesame.execution.ExecutionGraph;
import sesame.execution.runtime.collector.OutputCollector;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.execution.runtime.tuple.impl.Tuple;
import state_engine.DatabaseException;
import state_engine.transaction.impl.TxnContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static application.CONTROL.enable_app_combo;
import static state_engine.profiler.MeasureTools.*;

public class OBBolt_ts_nopush extends OBBolt_ts {
    private static final Logger LOG = LoggerFactory.getLogger(OBBolt_ts_nopush.class);
    private int thisTaskId;

    private Collection<ToppingEvent> ToppingEventsHolder;

    private Collection<AlertEvent> AlertEventsHolder;

    public OBBolt_ts_nopush(int fid, SINKCombo sink) {
        super(fid,sink);
    }


    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        ToppingEventsHolder = new ArrayDeque<>();
        AlertEventsHolder = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getThisTaskId(), context.getGraph());
    }

    protected void TOPPING_REQUEST_CONSTRUCT(ToppingEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_ReadRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], event.enqueue_time);//asynchronously return.
        ToppingEventsHolder.add(event);
    }

    protected void ALERT_REQUEST_CONSTRUCT(AlertEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        for (int i = 0; i < event.getNum_access(); i++)
            transactionManager.Asy_ReadRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], event.enqueue_time);//asynchronously return.
        AlertEventsHolder.add(event);
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {

            int readSize = buyingEvents.size();
            int alertSize = AlertEventsHolder.size();
            int toppingSize = ToppingEventsHolder.size();

            BEGIN_TRANSACTION_TIME_MEASURE(thread_Id);

            BEGIN_TP_TIME_MEASURE(thread_Id);

            transactionManager.start_evaluate(thread_Id, this.fid);//start lazy evaluation in transaction manager.

            END_TP_TIME_MEASURE(thread_Id);// overhead_total TP time.

            BEGIN_ACCESS_TIME_MEASURE(thread_Id);

            BUYING_REQUEST_CORE();

            ALERT_REQUEST_CORE();

            TOPPING_REQUEST_CORE();

            END_ACCESS_TIME_MEASURE_TS(thread_Id, readSize, 0, alertSize + toppingSize);//overhead_total compute time.

            END_TRANSACTION_TIME_MEASURE_TS(thread_Id, 0);//overhead_total txn time.

            BUYING_REQUEST_POST();

            ALERT_REQUEST_POST();

            TOPPING_REQUEST_POST();
            if (!enable_app_combo) {
                final Marker marker = in.getMarker();
                this.collector.ack(in, marker);//tell spout it has finished transaction processing.
            } else {

            }

            //post_process for events left-over.

            END_TOTAL_TIME_MEASURE_TS(thread_Id, readSize + alertSize + toppingSize);

            buyingEvents.clear();//all tuples in the EventsHolder are finished.
            ToppingEventsHolder.clear();
            AlertEventsHolder.clear();

        } else {
            execute_ts_normal(in);
        }
    }

    private void TOPPING_REQUEST_POST() throws InterruptedException {
        for (ToppingEvent event : ToppingEventsHolder) {
            TOPPING_REQUEST_POST(event);
        }
    }

    private void ALERT_REQUEST_POST() throws InterruptedException {
        for (AlertEvent event : AlertEventsHolder) {
            ALERT_REQUEST_POST(event);
        }
    }

    private void TOPPING_REQUEST_CORE() {
        for (ToppingEvent event : ToppingEventsHolder) {
            TOPPING_REQUEST_CORE(event);
        }
    }

    private void ALERT_REQUEST_CORE() {
        for (AlertEvent event : AlertEventsHolder) {
            ALERT_REQUEST_CORE(event);
        }
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
