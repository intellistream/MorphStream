package intellistream.morphstream.examples.tsp.onlinebiding.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.onlinebiding.events.AlertTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.BuyingTxnEvent;
import intellistream.morphstream.examples.tsp.onlinebiding.events.ToppingTxnEvent;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.examples.tsp.onlinebiding.util.BidingResult;
import org.slf4j.Logger;

import java.util.List;

import static intellistream.morphstream.common.constants.OnlineBidingSystemConstants.Constant.NUM_ACCESSES_PER_BUY;
import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class OBBolt extends TransactionalBolt {
    SINKCombo sink;

    public OBBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "ob";
    }

    /**
     * Perform some dummy calculation to simulate authentication process..
     *
     * @param bid
     * @param timestamp
     */
    protected void auth(long bid, Long timestamp) {
//        System.out.println(generatedString);
//        stateless_task.random_compute(5);
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void BUYING_REQUEST_LOCKAHEAD(BuyingTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i)
            transactionManager.lock_ahead(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }

    protected void ALERT_REQUEST_LOCKAHEAD(AlertTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i)
            transactionManager.lock_ahead(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }

    protected void TOPPING_REQUEST_LOCKAHEAD(ToppingTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i)
            transactionManager.lock_ahead(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
    }

    protected void BUYING_REQUEST_NOLOCK(BuyingTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            transactionManager.SelectKeyRecord_noLock(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void TOPPING_REQUEST_NOLOCK(ToppingTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord_noLock(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void ALERT_REQUEST_NOLOCK(AlertTxnEvent event, TxnContext txnContext) throws DatabaseException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord_noLock(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void BUYING_REQUEST(BuyingTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            transactionManager.SelectKeyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void TOPPING_REQUEST(ToppingTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void ALERT_REQUEST(AlertTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        for (int i = 0; i < event.getNum_access(); ++i) {
            transactionManager.SelectKeyRecord(txnContext, "goods", String.valueOf(event.getItemId()[i]), event.record_refs[i], READ_WRITE);
            assert event.record_refs[i].getRecord() != null;
        }
    }

    protected void BUYING_REQUEST_CORE(BuyingTxnEvent event) {
        //measure_end if any item is not able to buy.
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            long bidPrice = event.getBidPrice(i);
            long qty = event.getBidQty(i);
            List<DataBox> values = event.record_refs[i].getRecord().getValues();
            long askPrice = values.get(1).getLong();
            long left_qty = values.get(2).getLong();
            if (bidPrice < askPrice || qty > left_qty) {
                //bid failed.
                event.biding_result = new BidingResult(event, false);
                return;
            }
        }
        //if allowed to proceed.
        for (int i = 0; i < NUM_ACCESSES_PER_BUY; ++i) {
            long bidPrice = event.getBidPrice(i);
            long qty = event.getBidQty(i);
            List<DataBox> values = event.record_refs[i].getRecord().getValues();
            long askPrice = values.get(1).getLong();
            long left_qty = values.get(2).getLong();
            //bid success
            values.get(2).setLong(left_qty - qty);//new quantity.
        }
        event.biding_result = new BidingResult(event, true);
    }

    protected void TOPPING_REQUEST_CORE(ToppingTxnEvent event) {
        for (int i = 0; i < event.getNum_access(); ++i) {
            List<DataBox> values = event.record_refs[i].getRecord().getValues();
            long newQty = values.get(2).getLong() + event.getItemTopUp()[i];
            values.get(2).setLong(newQty);
        }
        event.topping_result = true;
//        collector.force_emit(input_event.getBid(), true, input_event.getTimestamp());//the tuple is immediately finished.
    }

    protected void ALERT_REQUEST_CORE(AlertTxnEvent event) {
        for (int i = 0; i < event.getNum_access(); ++i) {
            List<DataBox> values = event.record_refs[i].getRecord().getValues();
            long newPrice = event.getAsk_price()[i];
            values.get(1).setLong(newPrice);
        }
        event.alert_result = true;
//        collector.force_emit(input_event.getBid(), true, input_event.getTimestamp());//the tuple is immediately finished.
    }

    protected void BUYING_REQUEST_POST(BuyingTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), event.biding_result, event.getTimestamp());//the tuple is finished finally.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.biding_result, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }

    protected void ALERT_REQUEST_POST(AlertTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), event.alert_result, event.getTimestamp());//the tuple is finished finally.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.alert_result, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }

    protected void TOPPING_REQUEST_POST(ToppingTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), event.topping_result, event.getTimestamp());//the tuple is finished finally.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.topping_result, event.getTimestamp())));//(long bid, int sourceId, TopologyContext context, Message message)
            }
        }
    }

    @Override
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            TxnEvent event = (TxnEvent) input_event;
            (event).setTimestamp(timestamp);
            if (event instanceof BuyingTxnEvent) {
                BUYING_REQUEST_POST((BuyingTxnEvent) event);
            } else if (event instanceof AlertTxnEvent) {
                ALERT_REQUEST_POST((AlertTxnEvent) event);
            } else {
                TOPPING_REQUEST_POST((ToppingTxnEvent) event);
            }
        }
        END_POST_TIME_MEASURE(thread_Id);
    }
}
