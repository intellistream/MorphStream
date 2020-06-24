package state_engine.content;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import state_engine.Meta.MetaTypes;
import state_engine.common.SpinLock;
import state_engine.content.common.RequestEntry;
import state_engine.storage.SchemaRecord;
import state_engine.storage.datatype.DataBox;
import state_engine.utils.Utils;

import java.util.List;
/**
 * #elif TO
 * ToContentImpl content_;
 */
public class ToContentImpl extends ToContent {
    public final static String TO_CONTENT = "TO_CONTENT";
    private static final Logger LOG = LoggerFactory.getLogger(ToContentImpl.class);
    private List<DataBox> data; //	char  * data_ptr_;
    private int data_size_;
    // last read that has been issued.
    private long read_ts_;
    // last write that has been issued.
    private long write_ts_;
    // minimum read that is waiting in the queue.
    private long min_read_ts_;
    // minimum write that is waiting in the queue.
    private long min_write_ts_;
    // minimum commit that is waiting in the queue.
    //uint64_t min_commit_ts_;
    // read request queue.
    private RequestEntry read_requests_head_;
    // write request queue.
    private RequestEntry write_requests_head_;
    // commit request queue.
    private RequestEntry commit_requests_head_;
    private SpinLock spinlock_ = new SpinLock();
    public ToContentImpl() {
        read_ts_ = 0;
        write_ts_ = 0;
        min_read_ts_ = Long.MAX_VALUE;
        min_write_ts_ = Long.MAX_VALUE;
        read_requests_head_ = null;
        write_requests_head_ = null;
        commit_requests_head_ = null;
    }
    /**
     * @param timestamp
     * @param data
     * @param is_ready  use singleton array to simulate pointer.
     * @return
     */
    @Override
    public boolean RequestReadAccess(final long timestamp, List<DataBox> data, boolean[] is_ready) {
        boolean is_success = true;
        spinlock_.lock();
        // if read ts is smaller than write ts that has been committed, then abort.
        if (timestamp < write_ts_) {
            is_success = false;
        }
        // if read ts is larger than minimum write ts in the queue, then buffer.
        else if (timestamp > min_write_ts_) {
            // put into queue.
            BufferReadRequest(timestamp, data, is_ready);
        }
        // if read ts is smaller than or equal to minimum write ts in the queue, then directly return.
        else {
            // copy data here. protected by mutex.
            // data has already been allocated.
            this.data = Utils.memcpy(data);//memcpy( * data, data_ptr_, data_size_);
            // directly read.
            if (read_ts_ < timestamp) {
                read_ts_ = timestamp;
            }
            is_ready[0] = true;
        }
        spinlock_.unlock();
        return is_success;
    }
    @Override
    public boolean RequestWriteAccess(final long timestamp, List<DataBox> data) {
        boolean is_success = true;
        spinlock_.lock();
        // if write ts is smaller than read ts that has been issued, than abort.
        if (timestamp < read_ts_) {
            is_success = false;
        }
        // is write ts is larger than or equal to read ts that has been issued, than buffer.
        else {
            // thomas write rule.
            BufferWriteRequest(timestamp, data);
        }
        spinlock_.unlock();
        return is_success;
    }
    /**
     * commit write Operation.
     *
     * @param timestamp
     * @param is_ready
     */
    @Override
    public void RequestCommit(long timestamp, boolean[] is_ready) {
        spinlock_.lock();
        // thomas write rule: the write is stale. remove it from buffer and return.
        if (timestamp < write_ts_) {
            // get the write entry that can be committed.
            RequestEntry entry = DebufferWriteRequest(timestamp);
            // blocked read requests can be unblocked.
            UpdateBuffer();
            //delete entry;
            entry = null;
        }
        // if there is still some reads, then buffer the commit.
        else if (timestamp > min_read_ts_) {
            BufferCommitRequest(timestamp, is_ready);
        }
        // otherwise, we can write, and update the write ts.
        else {
            // get the matching write.
            RequestEntry entry = DebufferWriteRequest(timestamp);
            // install the value_list.
            this.data = Utils.memcpy(entry.data_);//memcpy(data_ptr_, * entry -> data_, data_size_);
            // update the write ts.
            if (write_ts_ < timestamp) {
                write_ts_ = timestamp;
            }
            UpdateBuffer();
//			delete entry;
//			entry = null;
        }
        spinlock_.unlock();
    }
    @Override
    public void RequestAbort(long timestamp) {
        spinlock_.lock();
        RequestEntry entry = DebufferWriteRequest(timestamp);
        UpdateBuffer();
        spinlock_.unlock();
        //delete entry;
        //entry = NULL;
    }
    @Override
    public SchemaRecord ReadAccess(long ts, long mark_ID, boolean clean, MetaTypes.AccessType accessType) {
        throw new UnsupportedOperationException();
    }
    @Override
    public SchemaRecord readPreValues(long ts) {
        return null;
    }
    @Override
    public void clean_map(long mark_ID) {
    }
    @Override
    public void updateMultiValues(long ts, long previous_mark_ID, boolean clean, SchemaRecord record) {
    }
    // this function is always called after write has been installed or aborted.
    void UpdateBuffer() {
        while (true) {
            // the committed write can be the minimum one. then we need to update min_write_ts.
            long new_wts = GetMinWriteTimestamp();
            assert (new_wts >= min_write_ts_);
            if (new_wts == min_write_ts_) {
                return;
            }
            min_write_ts_ = new_wts;
            // since the min_write_ts has been updated, then probably a list of read request can be issued.
            RequestEntry read_list = DebufferReadRequest();
            if (read_list == null) {
                return;
            }
            // allow these reads to be proceeded.
            RequestEntry read_entry = read_list;
            while (read_entry != null) {
                // copy data here.
                // data has already been allocated.
                read_entry.data_ = Utils.memcpy(data);//memcpy( * (read_entry -> data_), data_ptr_, data_size_);
                // directly read.
                if (read_ts_ < read_entry.timestamp_) {
                    read_ts_ = read_entry.timestamp_;
                }
                // inform the blocked threads.
                read_entry.is_ready_[0] = true;
                // destroy these read requests.
                RequestEntry tmp_entry = read_entry;
                read_entry = read_entry.next_;
//				delete tmp_entry;
//				tmp_entry = null;
            }
            // read request queue has been updated. then we need to update min_read_ts.
            long new_rts = GetMinReadTimestamp();
            assert (new_rts >= min_read_ts_);
            if (new_rts == min_read_ts_) {
                return;
            }
            min_read_ts_ = new_rts;
            RequestEntry commit_list = DebufferCommitRequest();
            if (commit_list == null) {
                return;
            }
            RequestEntry win_commit = null;
            long win_ts = 0;
            RequestEntry commit_entry = commit_list;
            while (commit_entry != null) {
                RequestEntry tmp_write_entry = DebufferWriteRequest(commit_entry.timestamp_);
                assert (tmp_write_entry != null);
                if (commit_entry.timestamp_ > win_ts) {
//					delete win_commit;
                    win_commit = commit_entry;
                    win_ts = commit_entry.timestamp_;
                } else {
//					delete commit_entry;
                    commit_entry = null;
                }
                commit_entry.is_ready_[0] = true;
                RequestEntry tmp_commit_entry = commit_entry;
                commit_entry = commit_entry.next_;
//				delete tmp_commit_entry;
                tmp_commit_entry = null;
            }
            assert (win_commit != null);
            // perform write.
            // install the value_list.
            this.data = Utils.memcpy(win_commit.data_);//memcpy(data_ptr_, * win_commit -> data_, data_size_);
            // update the write ts.
            if (write_ts_ < win_commit.timestamp_) {
                write_ts_ = win_commit.timestamp_;
            }
//			delete win_commit;
//			win_commit = null;
        }
    }
    long GetMinReadTimestamp() {
        RequestEntry tmp_entry = read_requests_head_;
        return GetMinTimestamp(tmp_entry);
    }
    long GetMinWriteTimestamp() {
        RequestEntry tmp_entry = write_requests_head_;
        return GetMinTimestamp(tmp_entry);
    }
    long GetMinTimestamp(RequestEntry tmp_entry) {
        long new_min_ts = Integer.MAX_VALUE;
        // the request list is sorted from big to small
        // so that we should retrieve the whole list
        // we want to keep the reverse sorted order because it is beneficial for DebufferRead and DebufferWrite(these functions are called more frequently)
        while (tmp_entry != null) {
            if (tmp_entry.timestamp_ < new_min_ts) {
                new_min_ts = tmp_entry.timestamp_;
            }
            tmp_entry = tmp_entry.next_;
        }
        return new_min_ts;
    }
    // we can get a list of matching request
    RequestEntry DebufferCommitRequest() {
        RequestEntry ret_entry = null;
        RequestEntry tmp_entry = commit_requests_head_;
        RequestEntry prev_entry = null;
        // get a list of read requests that can proceed now.
        while (tmp_entry != null && tmp_entry.timestamp_ > min_read_ts_) {
            prev_entry = tmp_entry;
            tmp_entry = tmp_entry.next_;
        }
        ret_entry = tmp_entry;
        if (prev_entry != null) {
            prev_entry.next_ = null;
        } else {
            commit_requests_head_ = null;
        }
        return ret_entry;
    }
    RequestEntry DebufferReadRequest() {
        RequestEntry ret_entry = null;
        RequestEntry tmp_entry = read_requests_head_;
        RequestEntry prev_entry = null;
        // get a list of read requests that can proceed now.
        while (tmp_entry != null && tmp_entry.timestamp_ > min_write_ts_) {
            prev_entry = tmp_entry;
            tmp_entry = tmp_entry.next_;
        }
        ret_entry = tmp_entry;
        if (prev_entry != null) {
            prev_entry.next_ = null;
        } else {
            read_requests_head_ = null;
        }
        return ret_entry;
    }
    // we can always get exactly one matching request.
    RequestEntry DebufferWriteRequest(final long timestamp) {
        assert (write_requests_head_ != null);
        RequestEntry ret_entry = write_requests_head_;
        RequestEntry prev_entry = null;
        while (ret_entry != null && ret_entry.timestamp_ != timestamp) {
            prev_entry = ret_entry;
            ret_entry = ret_entry.next_;
        }
        // we must find exactly one matcing request.
        assert (ret_entry != null);
        if (prev_entry != null) {
            prev_entry.next_ = ret_entry.next_;
        } else {
            write_requests_head_ = ret_entry.next_;
        }
        ret_entry.next_ = null;
        return ret_entry;
    }
    private void BufferReadRequest(final long timestamp, List<DataBox> data, boolean[] is_ready) {
        RequestEntry entry = new RequestEntry();
        entry.timestamp_ = timestamp;
        entry.is_ready_ = is_ready;
        entry.data_ = data;
		/*entry->next_ = read_requests_head_;
		// become the head of the read request queue.
		// TODO: optimization: maintain a priority queue.
		read_requests_head_ = entry;*/
        RequestEntry tmp = read_requests_head_;
        RequestEntry pre = null;
        while (tmp != null && tmp.timestamp_ > timestamp) {
            pre = tmp;
            tmp = tmp.next_;
        }
        entry.next_ = tmp;
        if (pre != null) {
            pre.next_ = entry;
        } else {
            read_requests_head_ = entry;
        }
        // update minimum ts in the read queue.
        if (timestamp < min_read_ts_) {
            min_read_ts_ = timestamp;
        }
    }
    private void BufferWriteRequest(final long timestamp, List<DataBox> data) {
        RequestEntry entry = new RequestEntry();
        entry.timestamp_ = timestamp;
        entry.data_ = data;
				/*entry->next_ = write_requests_head_;
				// become the head of the write request queue.
				// TODO: optimization: maintain a priority queue.
				write_requests_head_ = entry;*/
        RequestEntry tmp = write_requests_head_;
        RequestEntry pre = null;
        while (tmp != null && tmp.timestamp_ > timestamp) {
            pre = tmp;
            tmp = tmp.next_;
        }
        entry.next_ = tmp;
        if (pre != null) {
            pre.next_ = entry;
        } else {
            write_requests_head_ = entry;
        }
        // update minimum ts in the write queue.
        if (timestamp < min_write_ts_) {
            min_write_ts_ = timestamp;
        }
    }
    void BufferCommitRequest(final long timestamp, boolean[] is_ready) {
        RequestEntry entry = new RequestEntry();
        entry.timestamp_ = timestamp;
        entry.is_ready_ = is_ready;
				/*entry->next_ = commit_requests_head_;
				commit_requests_head_ = entry;*/
        RequestEntry tmp = commit_requests_head_;
        RequestEntry pre = null;
        while (tmp != null && tmp.timestamp_ > timestamp) {
            pre = tmp;
            tmp = tmp.next_;
        }
        entry.next_ = tmp;
        if (pre != null) {
            pre.next_ = entry;
        } else {
            commit_requests_head_ = entry;
        }
    }
}
