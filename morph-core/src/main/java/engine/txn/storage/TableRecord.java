package engine.txn.storage;
import engine.txn.content.*;
import engine.txn.storage.table.RowID;
import engine.txn.lock.SpinLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static engine.txn.content.LWMContentImpl.LWM_CONTENT;
import static engine.txn.content.LockContentImpl.LOCK_CONTENT;
import static engine.txn.content.TStreamContentImpl.T_STREAMCONTENT;
import static engine.txn.content.ToContentImpl.TO_CONTENT;
import static engine.txn.content.common.ContentCommon.content_type;

public class TableRecord implements Comparable<TableRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(TableRecord.class);
    final int size;
    public Content content_;
    public SchemaRecord record_;//this record may be changed by multiple threads.

    public TableRecord(SchemaRecord record, int tthread) {
        switch (content_type) {
            case LOCK_CONTENT:
                content_ = new LockContentImpl();
                break;
            case TO_CONTENT:
                content_ = new ToContentImpl();
                break;
            case LWM_CONTENT:
                content_ = new LWMContentImpl();
                content_.updateValues(0, 0, false, record);//mvcc, value_list @ts=0
                content_.updateMultiValues(0, 0, false, record);//mvcc, value_list @ts=0
                break;
            case T_STREAMCONTENT:
                content_ = new TStreamContentImpl();
                content_.updateValues(0, 0, false, record);//mvcc, value_list @ts=0
                content_.updateMultiValues(0, 0, false, record);//mvcc, value_list @ts=0
                break;
            case LVTStreamContent.LVTSTREAM_CONTENT:
                content_ = new LVTStreamContentImpl();
                content_.updateValues(0, 0, false, record);//mvcc, value_list @ts=0
                content_.updateMultiValues(0, 0, false, record);//mvcc, value_list @ts=0
                ((LVTStreamContentImpl) content_).setLVs(tthread);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        record_ = record;
        size = sizeof(record);
    }

    public TableRecord(SchemaRecord record, int pid, SpinLock[] spinlock_) {
//        if (enable_log) LOG.info(record.GetPrimaryKey() + " belongs to pid:" + pid);
        content_ = new SStoreContentImpl(spinlock_, pid);//every record has an associated partition (and a lock_ratio for sure..).
        record_ = record;
        size = sizeof(record);
    }

    private int sizeof(SchemaRecord record) {
        return 0;
    }

    @Override
    public int compareTo(TableRecord o) {
        return Math.toIntExact(record_.getId().getID() - o.record_.getId().getID());
    }

    public int getID() {
        return record_.getId().getID();
    }

    public void setID(RowID ID) {
        this.record_.setID(ID);
    }
    public String toSerializableString(long snapshotId) {
        StringBuilder stringBuilder = new StringBuilder();
        SchemaRecord snapshotRecord = this.content_.ReadAccess(snapshotId, false);
        stringBuilder.append(snapshotRecord.toString());
        return stringBuilder.toString();
    }
}
