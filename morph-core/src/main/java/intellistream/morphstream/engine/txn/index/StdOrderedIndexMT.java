package intellistream.morphstream.engine.txn.index;

import intellistream.morphstream.engine.txn.lock.RWLock;
import intellistream.morphstream.engine.db.storage.TableRecord;

/**
 * MultiThread (MT) version.
 */
public class StdOrderedIndexMT extends StdOrderedIndex {
    RWLock lock_ = new RWLock();

    @Override
    public TableRecord SearchRecord(String key) {
        lock_.AcquireReadLock();
        if (!index_.containsKey(key)) {/* (index_.find(key) == index_.end()) */
            lock_.ReleaseReadLock();
            return null;
        } else {
            TableRecord ret_record = index_.get(key);
            lock_.ReleaseReadLock();
            return ret_record;
        }
    }
}
