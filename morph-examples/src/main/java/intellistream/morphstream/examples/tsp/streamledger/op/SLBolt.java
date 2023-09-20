package intellistream.morphstream.examples.tsp.streamledger.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.streamledger.events.DepositTxnEvent;
import intellistream.morphstream.examples.tsp.streamledger.events.TransactionTxnEvent;
import intellistream.morphstream.engine.stream.components.operators.api.delete.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.util.AppConfig;
import org.slf4j.Logger;

import java.util.List;

import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class SLBolt extends TransactionalBolt {
    SINKCombo sink;

    public SLBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "sl";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void DEPOSITE_REQUEST_NOLOCK(DepositTxnEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.SelectKeyRecord_noLock(txnContext, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);
        assert event.account_value.getRecord() != null && event.asset_value.getRecord() != null;
    }

    protected void TRANSFER_REQUEST_NOLOCK(TransactionTxnEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.SelectKeyRecord_noLock(txnContext, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.SelectKeyRecord_noLock(txnContext, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);
        assert event.src_account_value.getRecord() != null && event.dst_account_value.getRecord() != null && event.src_asset_value.getRecord() != null && event.dst_asset_value.getRecord() != null;
    }

    protected void DEPOSITE_REQUEST(DepositTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.SelectKeyRecord(txnContext, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);
        assert event.account_value.getRecord() != null && event.asset_value.getRecord() != null;
    }

    protected void TRANSFER_REQUEST(TransactionTxnEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        transactionManager.SelectKeyRecord(txnContext, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord(txnContext, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.SelectKeyRecord(txnContext, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.SelectKeyRecord(txnContext, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);
        assert event.src_account_value.getRecord() != null && event.dst_account_value.getRecord() != null && event.src_asset_value.getRecord() != null && event.dst_asset_value.getRecord() != null;
    }

    protected void TRANSFER_LOCK_AHEAD(TransactionTxnEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.lock_ahead(txnContext, "accounts", event.getSourceAccountId(), event.src_account_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "accounts", event.getTargetAccountId(), event.dst_account_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "bookEntries", event.getSourceBookEntryId(), event.src_asset_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "bookEntries", event.getTargetBookEntryId(), event.dst_asset_value, READ_WRITE);
    }

    protected void DEPOSITE_LOCK_AHEAD(DepositTxnEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.lock_ahead(txnContext, "accounts", event.getAccountId(), event.account_value, READ_WRITE);
        transactionManager.lock_ahead(txnContext, "bookEntries", event.getBookEntryId(), event.asset_value, READ_WRITE);
    }

    protected void TRANSFER_REQUEST_CORE(TransactionTxnEvent event) throws InterruptedException {
//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        // measure_end the preconditions
        DataBox sourceAccountBalance_value = event.src_account_value.getRecord().getValues().get(1);
        final long sourceAccountBalance = sourceAccountBalance_value.getLong();
        DataBox sourceAssetValue_value = event.src_asset_value.getRecord().getValues().get(1);
        final long sourceAssetValue = sourceAssetValue_value.getLong();
        DataBox targetAccountBalance_value = event.dst_account_value.getRecord().getValues().get(1);
        final long targetAccountBalance = targetAccountBalance_value.getLong();
        DataBox targetAssetValue_value = event.dst_asset_value.getRecord().getValues().get(1);
        final long targetAssetValue = targetAssetValue_value.getLong();
        if (sourceAccountBalance > event.getMinAccountBalance()
                && sourceAccountBalance > event.getAccountTransfer()
                && sourceAssetValue > event.getBookEntryTransfer()) {
//            long start = System.nanoTime();
//            while (System.nanoTime() - start < 40000) {}
            // compute the new balances
            AppConfig.randomDelay();
            final long newSourceBalance = sourceAccountBalance - event.getAccountTransfer();
            AppConfig.randomDelay();
            final long newTargetBalance = targetAccountBalance + event.getAccountTransfer();
            AppConfig.randomDelay();
            final long newSourceAssets = sourceAssetValue - event.getBookEntryTransfer();
            AppConfig.randomDelay();
            final long newTargetAssets = targetAssetValue + event.getBookEntryTransfer();
            // write back the updated values
            sourceAccountBalance_value.setLong(newSourceBalance);
            targetAccountBalance_value.setLong(newTargetBalance);
            targetAccountBalance_value.setLong(newSourceAssets);
            targetAssetValue_value.setLong(newTargetAssets);
            event.transaction_result = new TransactionResult(event, true, newSourceBalance, newTargetBalance);
        } else {
            event.transaction_result = new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance);
        }
//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    protected void DEPOSITE_REQUEST_CORE(DepositTxnEvent event) {
//        BEGIN_ACCESS_TIME_MEASURE(thread_Id);
        List<DataBox> values = event.account_value.getRecord().getValues();
        AppConfig.randomDelay();
        long newAccountValue = values.get(1).getLong() + event.getAccountTransfer();
        values.get(1).setLong(newAccountValue);
        List<DataBox> asset_values = event.asset_value.getRecord().getValues();
        AppConfig.randomDelay();
        long newAssetValue = values.get(1).getLong() + event.getBookEntryTransfer();
        asset_values.get(1).setLong(newAssetValue);
//        collector.force_emit(input_event.getBid(), null, input_event.getTimestamp());
//        END_ACCESS_TIME_MEASURE_ACC(thread_Id);
    }

    //post stream processing phase..
    protected void POST_PROCESS(long _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (long i = _bid; i < _bid + combo_bid_size; i++) {
            if (input_event instanceof DepositTxnEvent) {
                ((DepositTxnEvent) input_event).setTimestamp(timestamp);
                DEPOSITE_REQUEST_POST((DepositTxnEvent) input_event);
            } else {
                ((TransactionTxnEvent) input_event).setTimestamp(timestamp);
                TRANSFER_REQUEST_POST((TransactionTxnEvent) input_event);
            }
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected void TRANSFER_REQUEST_POST(TransactionTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, event.transaction_result, event.getTimestamp())));
            }
        }
    }

    void DEPOSITE_REQUEST_POST(DepositTxnEvent event) throws InterruptedException {
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, true, event.getTimestamp())));
            }
        }
    }

    protected void LAL_PROCESS(long _bid) throws InterruptedException, DatabaseException {
    }
}
