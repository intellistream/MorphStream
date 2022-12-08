package content;

import content.common.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storage.SchemaRecord;
import transaction.context.TxnContext;

public class TStreamContentImpl extends TStreamContent {
    private static final Logger LOG = LoggerFactory.getLogger(TStreamContentImpl.class);
    private final long pwid = Long.MAX_VALUE;//previous watermark id.

    @Override
    public SchemaRecord ReadAccess(TxnContext context, CommonMetaTypes.AccessType accessType) {
        return readValues((long) context.getBID(), -1, false);
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
}
