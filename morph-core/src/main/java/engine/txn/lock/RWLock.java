package engine.txn.lock;

public class RWLock {
    final static int NO_LOCK = 0, READ_LOCK = 1, WRITE_LOCK = 2;//enum LockType : size_t{ NO_LOCK, READ_LOCK, WRITE_LOCK };
    SpinLock spinlock_ = new SpinLock();
    volatile int reader_count_ = 0;
    volatile int lock_type_ = NO_LOCK;

    public void AcquireReadLock() {
        while (lock_type_ == WRITE_LOCK) {
            //spin sync_ratio.
        }
        spinlock_.lock();
        if (lock_type_ == WRITE_LOCK) {
            spinlock_.unlock();
        } else {
            if (lock_type_ == NO_LOCK) {
                lock_type_ = READ_LOCK;
                ++reader_count_;
            } else {
                assert (lock_type_ == READ_LOCK);
                ++reader_count_;
            }
            spinlock_.unlock();
            return;
        }
    }

    public void AcquireWriteLock() {
        while (true) {
            while (lock_type_ != NO_LOCK) {
            }
            spinlock_.lock();
            if (lock_type_ != NO_LOCK) {
                spinlock_.unlock();
            } else {
                assert (lock_type_ == NO_LOCK);
                lock_type_ = WRITE_LOCK;
                spinlock_.unlock();
                return;
            }
        }
    }

    public void ReleaseReadLock() {
        spinlock_.lock();
        --reader_count_;
        if (reader_count_ == 0) {
            lock_type_ = NO_LOCK;
        }
        spinlock_.unlock();
    }

    public boolean TryReadLock() {
        boolean rt = false;
        spinlock_.lock();
        if (lock_type_ == NO_LOCK) {
            lock_type_ = READ_LOCK;
            ++reader_count_;
            rt = true;
        } else if (lock_type_ == READ_LOCK) {
            ++reader_count_;
            rt = true;
        } else {
            rt = false;
        }
        spinlock_.unlock();
        return rt;
    }

    public boolean TryWriteLock() {
        boolean rt = false;
        spinlock_.lock();
        if (lock_type_ == NO_LOCK) {
            lock_type_ = WRITE_LOCK;
            rt = true;
        } else {
            rt = false;
        }
        spinlock_.unlock();
        return rt;
    }

    public void ReleaseWriteLock() {
        spinlock_.lock();
        lock_type_ = NO_LOCK;
        spinlock_.unlock();
    }
}
