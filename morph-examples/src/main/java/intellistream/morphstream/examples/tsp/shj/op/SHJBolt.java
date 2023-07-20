package intellistream.morphstream.examples.tsp.shj.op;

import intellistream.morphstream.examples.utils.SINKCombo;
import intellistream.morphstream.examples.tsp.shj.events.SHJEvent;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import org.slf4j.Logger;

import java.util.Objects;

import static intellistream.morphstream.configuration.CONTROL.enable_app_combo;
import static intellistream.morphstream.configuration.CONTROL.enable_latency_measurement;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_ONLY;
import static intellistream.morphstream.engine.txn.content.common.CommonMetaTypes.AccessType.READ_WRITE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.BEGIN_POST_TIME_MEASURE;
import static intellistream.morphstream.engine.txn.profiler.MeasureTools.END_POST_TIME_MEASURE;

public abstract class SHJBolt extends TransactionalBolt {
    public SINKCombo sink;

    public SHJBolt(Logger log, int fid, SINKCombo sink) {
        super(log, fid);
        this.sink = sink;
        this.configPrefix = "gs";
    }

    @Override
    protected void TXN_PROCESS(long _bid) throws DatabaseException, InterruptedException {
    }

    protected void SHJ_LOCK_AHEAD(SHJEvent event, TxnContext txnContext) throws DatabaseException {
        String updateIndexTable = ""; //index to update
        String lookupIndexTable = ""; //index to lookup
        if (Objects.equals(event.getStreamID(), "r")) {
            updateIndexTable = "index_r_table";
            lookupIndexTable = "index_s_table";
        } else if (Objects.equals(event.getStreamID(), "s")) {
            updateIndexTable = "index_s_table";
            lookupIndexTable = "index_r_table";
        }
        transactionManager.lock_ahead(txnContext, updateIndexTable, event.getKey(), event.srcIndexRecordRef, READ_WRITE); //index to update
//        transactionManager.lock_ahead(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
        for (int i = 0; i < 5; ++i)
            transactionManager.lock_ahead(txnContext, lookupIndexTable,
                    String.valueOf(event.getLookupKeys()[i]), event.getLookupIndexRecords()[i], READ_ONLY); //indexes to lookup
    }

    protected void SHJ_REQUEST_NOLOCK(SHJEvent event, TxnContext txnContext) throws DatabaseException { //TODO: Follow GS, rewrite this section. Consider remove key.
        String updateIndexTable = ""; //index to update
        String lookupIndexTable = ""; //index to lookup
        if (Objects.equals(event.getStreamID(), "r")) {
            updateIndexTable = "index_r_table";
            lookupIndexTable = "index_s_table";
        } else if (Objects.equals(event.getStreamID(), "s")) {
            updateIndexTable = "index_s_table";
            lookupIndexTable = "index_r_table";
        }
        transactionManager.SelectKeyRecord_noLock(txnContext, updateIndexTable, event.getKey(), event.srcIndexRecordRef, READ_WRITE);
//        transactionManager.SelectKeyRecord_noLock(txnContext, "index_s_table", event.getKey(), event.tarIndexRecordRef, READ_WRITE);
        for (int i = 0; i < 5; ++i)
            transactionManager.SelectKeyRecord_noLock(txnContext, lookupIndexTable,
                    String.valueOf(event.getLookupKeys()[i]), event.getLookupIndexRecords()[i], READ_ONLY); //indexes to lookup
//        assert event.srcIndexRecordRef.getRecord() != null || event.tarIndexRecordRef.getRecord() != null;
    }

    protected void SHJ_REQUEST_CORE(SHJEvent event) {
        DataBox sourceIndex_addr = event.srcIndexRecordRef.getRecord().getValues().get(1);
        DataBox sourceIndex_matching_addr = event.srcIndexRecordRef.getRecord().getValues().get(2);
        DataBox targetIndex_addr = event.getLookupIndexRecords()[0].getRecord().getValues().get(1);
        final String targetIndexAddress = targetIndex_addr.getString();

        sourceIndex_addr.setString(event.getAmount(), event.getAmount().length());
        sourceIndex_matching_addr.setString(targetIndexAddress, targetIndexAddress.length());
    }

    protected void LAL_PROCESS(double _bid) throws InterruptedException, DatabaseException {
    }

    protected void POST_PROCESS(double _bid, long timestamp, int combo_bid_size) throws InterruptedException {
        BEGIN_POST_TIME_MEASURE(thread_Id);
        for (double i = _bid; i < _bid + combo_bid_size; i++) {
            ((SHJEvent) input_event).setTimestamp(timestamp);
            SHJ_POST((SHJEvent) input_event);
        }
        END_POST_TIME_MEASURE(thread_Id);
    }

    protected boolean SHJ_CORE(SHJEvent event) { //TODO: tstream
        SchemaRecordRef ref = event.srcIndexRecordRef;
        if (ref.isEmpty()) {
            return false;//not yet processed.
        }
        DataBox dataBox = ref.getRecord().getValues().get(1); //Read address of matching tuple TODO: Verify this
        event.setTurnoverRatePair(dataBox.getString());
        return true;
    }

    protected void SHJ_POST(SHJEvent event) throws InterruptedException { //TODO: tstream
        String joinResult = event.getTurnoverRatePair().toString();
        if (!enable_app_combo) {
            collector.emit(event.getBid(), true, event.getTimestamp());//the tuple is finished.
        } else {
            if (enable_latency_measurement) {
                sink.execute(new Tuple(event.getBid(), this.thread_Id, context, new GeneralMsg<>(DEFAULT_STREAM_ID, joinResult, event.getTimestamp())));
            }
        }
    }
}
