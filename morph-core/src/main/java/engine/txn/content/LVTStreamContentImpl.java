package engine.txn.content;

import engine.txn.content.common.CommonMetaTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.storage.SchemaRecord;
import engine.txn.transaction.context.TxnContext;

/*
 * This class is used to store the content of a table for a specific version, which supports to implement the LSN Vector protocol.
 * Tuple.readLV() and Tuple.writeLV() are used to record the LSN Vector for read and write operation.
 * More detail can be found in the paper "Taurus: Lightweight Parallel Logging for In_Memory Database Management Systems".
 * Project Link: https://github.com/yuxiamit/DBx1000_logging.
 */
public class LVTStreamContentImpl extends LVTStreamContent{
    private static final Logger LOG = LoggerFactory.getLogger(LVTStreamContentImpl.class);
    @Override
    public SchemaRecord ReadAccess(TxnContext context, CommonMetaTypes.AccessType accessType) {
        return null;
    }

    @Override
    public SchemaRecord ReadAccess(long ts, long mark_ID, boolean clean, CommonMetaTypes.AccessType accessType) {
        return null;
    }

    @Override
    public SchemaRecord ReadAccess(long snapshotId, boolean clean) {
        return readValues(snapshotId, clean);
    }


    @Override
    public void WriteAccess(long commit_timestamp, long mark_ID, boolean clean, SchemaRecord local_record_) {

    }
}
