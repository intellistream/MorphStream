package transaction.context;

import java.util.concurrent.atomic.AtomicLong;

import static content.common.CommonMetaTypes.kBatchTsNum;
import static content.common.CommonMetaTypes.kMaxThreadNum;

public abstract class GlobalTimestamp {
    static AtomicLong monotone_timestamp_ = new AtomicLong(0);
    static AtomicLong[] thread_timestamp_ = new AtomicLong[kMaxThreadNum];
    static int thread_count_;

    public static long GetMonotoneTimestamp() {
        return monotone_timestamp_.getAndIncrement();
    }

    static long GetBatchMonotoneTimestamp() {
        return monotone_timestamp_.getAndAdd(kBatchTsNum);
    }
}
