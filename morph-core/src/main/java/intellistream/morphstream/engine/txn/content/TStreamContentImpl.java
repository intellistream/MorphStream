package intellistream.morphstream.engine.txn.content;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.db.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TStreamContentImpl extends TStreamContent {
    private static final Logger LOG = LoggerFactory.getLogger(TStreamContentImpl.class);
    private final long pwid = Long.MAX_VALUE;//previous watermark id.

    @Override
    public SchemaRecord ReadAccess(FunctionContext context, CommonMetaTypes.AccessType accessType) {
        return readValues(context.getBID(), -1, false);
    }

    @Override
    public SchemaRecord ReadAccess(long ts, long previous_mark_ID, boolean clean, CommonMetaTypes.AccessType accessType) {
        SchemaRecord rt = readValues(ts, previous_mark_ID, clean);
        return rt;
    }

    @Override
    public void WriteAccess(long ts, long previous_mark_ID, boolean clean, SchemaRecord local_record_) {
        updateValues(ts, previous_mark_ID, clean, local_record_);//mvcc, value_list @ts=0
    }

    @Override
    public SchemaRecord ReadAccess(long snapshotId, boolean clean) {
        return readValues(snapshotId, clean);
    }
}
