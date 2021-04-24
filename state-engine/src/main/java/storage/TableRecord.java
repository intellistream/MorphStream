package storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import common.SpinLock;
import content.*;
import storage.table.RowID;

import static content.LWMContentImpl.LWM_CONTENT;
import static content.LockContentImpl.LOCK_CONTENT;
import static content.T_StreamContentImpl.T_STREAMCONTENT;
import static content.ToContentImpl.TO_CONTENT;
import static content.common.ContentCommon.content_type;
public class TableRecord implements Comparable<TableRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(TableRecord.class);
    final int size;
    public Content content_;
    public SchemaRecord record_;//this record may be changed by multiple threads.
    public TableRecord(SchemaRecord record) {
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
                content_ = new T_StreamContentImpl();
                content_.updateValues(0, 0, false, record);//mvcc, value_list @ts=0
                content_.updateMultiValues(0, 0, false, record);//mvcc, value_list @ts=0
                break;
            default:
                throw new UnsupportedOperationException();
        }
        record_ = record;
        size = sizeof(record);
    }
    public TableRecord(SchemaRecord record, int pid, SpinLock[] spinlock_) {
//        LOG.info(record.GetPrimaryKey() + " belongs to pid:" + pid);
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
}
