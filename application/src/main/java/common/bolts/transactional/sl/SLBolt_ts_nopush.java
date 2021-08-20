package common.bolts.transactional.sl;

import common.param.sl.DepositEvent;
import common.param.sl.TransactionEvent;
import combo.SINKCombo;
import components.context.TopologyContext;
import content.T_StreamContent;
import db.DatabaseException;
import execution.ExecutionGraph;
import execution.runtime.collector.OutputCollector;
import execution.runtime.tuple.impl.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.impl.ordered.TxnManagerTStream;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;

import static common.constants.StreamLedgerConstants.Constant.NUM_ACCOUNTS;

public class SLBolt_ts_nopush extends SLBolt_ts {
    private static final Logger LOG = LoggerFactory.getLogger(SLBolt_ts_nopush.class);
    ArrayDeque<DepositEvent> depositeEvents;

    public SLBolt_ts_nopush(int fid, SINKCombo sink) {
        super(fid, sink);
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        transactionManager = new TxnManagerTStream(db.getStorageManager(), this.context.getThisComponentId(), thread_Id, NUM_ACCOUNTS, this.context.getThisComponent().getNumTasks(), config.getString("scheduler", "BL"));
        depositeEvents = new ArrayDeque<>();
    }

    public void loadDB(Map conf, TopologyContext context, OutputCollector collector) {
//        prepareEvents();
        loadDB(context.getThisTaskId() - context.getThisComponent().getExecutorList().get(0).getExecutorID(), context.getGraph());
    }

    @Override
    public void execute(Tuple in) throws InterruptedException, DatabaseException, BrokenBarrierException {
        if (in.isMarker()) {
            int transSize = transactionEvents.size();
            int depoSize = depositeEvents.size();
            int num_events = transSize + depoSize;
            transactionManager.start_evaluate(thread_Id, in.getBID(), num_events);//start lazy evaluation in transaction manager.
            TRANSFER_REQUEST_CORE();
            DEPOSITE_REQUEST_CORE();
            TRANSFER_REQUEST_POST();
            DEPOSITE_REQUEST_POST();
            transactionEvents.clear();//all tuples in the holder is finished.
            depositeEvents.clear();
        } else {
            execute_ts_normal(in);
        }
    }

    protected void TRANSFER_REQUEST_CONSTRUCT(TransactionEvent event, TxnContext txnContext) throws DatabaseException {
        transactionManager.Asy_ReadRecords(txnContext,
                "accounts",
                event.getSourceAccountId()
                , event.src_account_values,//to be fill up.
                event.enqueue_time);          //asynchronously return.
        transactionManager.Asy_ReadRecords(txnContext,
                "bookEntries", event.getSourceBookEntryId(),
                event.src_asset_values
                , event.enqueue_time);   //asynchronously return.
        transactionManager.Asy_ReadRecords(txnContext,
                "accounts",
                event.getTargetAccountId()
                , event.dst_account_values,//to be fill up.
                event.enqueue_time);          //asynchronously return.
        transactionManager.Asy_ReadRecords(txnContext,
                "bookEntries",
                event.getTargetBookEntryId(),
                event.dst_asset_values
                , event.enqueue_time);   //asynchronously return.
        transactionEvents.add(event);
    }

    protected void DEPOSITE_REQUEST_CONSTRUCT(DepositEvent event, TxnContext txnContext) throws DatabaseException, InterruptedException {
        //it simply construct the operations and return.
        transactionManager.Asy_ReadRecords(txnContext, "accounts", event.getAccountId(), event.account_values, event.enqueue_time);
        transactionManager.Asy_ReadRecords(txnContext, "bookEntries", event.getBookEntryId(), event.asset_values, event.enqueue_time);
        depositeEvents.add(event);
    }

    private void DEPOSITE_REQUEST_CORE() {
        for (DepositEvent event : depositeEvents) {
            DEPOSITE_REQUEST_CORE(event);
        }
    }

    private void DEPOSITE_REQUEST_POST() throws InterruptedException {
        for (DepositEvent event : depositeEvents) {
            DEPOSITE_REQUEST_POST(event);
        }
    }

    protected void DEPOSITE_REQUEST_CORE(DepositEvent event) {
        SchemaRecord srcRecord = event.account_values.getRecord().content_.readPreValues(event.getBid());
        List<DataBox> values = srcRecord.getValues();
        //apply function to modify..
        long newAccountValue = values.get(1).getLong() + event.getAccountTransfer();
        values.get(1).setLong(newAccountValue);
        List<DataBox> asset_values = event.asset_values.getRecord().content_.readPreValues(event.getBid()).getValues();
        long newAssetValue = values.get(1).getLong() + event.getBookEntryTransfer();
        asset_values.get(1).setLong(newAssetValue);
    }

    protected void TRANSFER_REQUEST_CORE(TransactionEvent event) throws InterruptedException {
        // read
        SchemaRecord preValues = event.src_account_values.getRecord().content_.readPreValues(event.getBid());
        SchemaRecord preValues1 = event.src_asset_values.getRecord().content_.readPreValues(event.getBid());
        if (preValues == null) {
            LOG.info("Failed to read condition records[0]" + event.src_account_values.getRecord().getID());
            LOG.info("Its version size:" + ((T_StreamContent) event.src_account_values.getRecord().content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) event.src_account_values.getRecord().content_).versions.entrySet()) {
                LOG.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + event.getBid());
            }
            LOG.info("TRY reading:" + event.src_account_values.getRecord().content_.readPreValues(event.getBid()));//not modified in last round);
        }
        if (preValues1 == null) {
            LOG.info("Failed to read condition records[1]" + event.src_asset_values.getRecord().getID());
            LOG.info("Its version size:" + ((T_StreamContent) event.src_asset_values.getRecord().content_).versions.size());
            for (Map.Entry<Long, SchemaRecord> schemaRecord : ((T_StreamContent) event.src_asset_values.getRecord().content_).versions.entrySet()) {
                LOG.info("Its contents:" + schemaRecord.getKey() + " value:" + schemaRecord.getValue() + " current bid:" + event.getBid());
            }
            LOG.info("TRY reading:" + ((T_StreamContent) event.src_asset_values.getRecord().content_).versions.get(event.getBid()));//not modified in last round);
        }
        final long sourceAccountBalance = preValues.getValues().get(1).getLong();
        final long sourceAssetValue = preValues1.getValues().get(1).getLong();
        DataBox targetAccountBalance_value = event.dst_account_values.getRecord().content_.readPreValues(event.getBid()).getValues().get(1);
        final long targetAccountBalance = targetAccountBalance_value.getLong();
        DataBox targetAssetValue_value = event.dst_asset_values.getRecord().content_.readPreValues(event.getBid()).getValues().get(1);
        final long targetAssetValue = targetAssetValue_value.getLong();
        //when d_record is different from condition record
        //It may generate cross-records dependency problem.
        //Fix it later.
        // check the preconditions
        //TODO: make the condition checking more generic in future.
        if (sourceAccountBalance > event.getMinAccountBalance()
                && sourceAccountBalance > event.getAccountTransfer()
                && sourceAssetValue > event.getBookEntryTransfer()) {
            // compute the new balances
            final long newSourceBalance = sourceAccountBalance - event.getAccountTransfer();
            final long newTargetBalance = targetAccountBalance + event.getAccountTransfer();
            final long newSourceAssets = sourceAssetValue - event.getBookEntryTransfer();
            final long newTargetAssets = targetAssetValue + event.getBookEntryTransfer();
            //read
            SchemaRecord srcRecord = event.src_account_values.getRecord().content_.readPreValues(event.getBid());
            List<DataBox> values = srcRecord.getValues();
            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record
            // write back the updated values
            tempo_record.getValues().get(1).setLong(newSourceBalance);
            targetAccountBalance_value.setLong(newTargetBalance);
            targetAccountBalance_value.setLong(newSourceAssets);
            targetAssetValue_value.setLong(newTargetAssets);
            event.dst_account_values.getRecord().content_.updateMultiValues(event.getBid(), -1, false, tempo_record);//it may reduce NUMA-traffic.
            event.dst_asset_values.getRecord().content_.updateMultiValues(event.getBid(), -1, false, tempo_record);//it may reduce NUMA-traffic.
            event.transaction_result = new TransactionResult(event, true, newSourceBalance, newTargetBalance);
        } else {
//            if (operation.success[0] == true)
//                System.nanoTime();
            event.transaction_result = new TransactionResult(event, false, sourceAccountBalance, targetAccountBalance);
        }
    }

    protected void TRANSFER_REQUEST_CORE() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_CORE(event);
        }
    }

    protected void TRANSFER_REQUEST_POST() throws InterruptedException {
        for (TransactionEvent event : transactionEvents) {
            TRANSFER_REQUEST_POST(event);
        }
    }
}
