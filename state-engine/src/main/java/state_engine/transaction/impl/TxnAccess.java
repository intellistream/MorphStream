package state_engine.transaction.impl;
import state_engine.Meta.MetaTypes.AccessType;
import state_engine.storage.SchemaRecord;
import state_engine.storage.TableRecord;

import java.util.Arrays;
import java.util.Comparator;
public interface TxnAccess {
    class Access {
        public AccessType access_type_;
        public TableRecord access_record_;
        public SchemaRecord local_record_;
        public String table_id_;
        public long timestamp_;
    }
    class AccessList {
        private final int N;
        volatile public int access_count_;
        private Access[] accesses_;
        public AccessList(int N) {
            access_count_ = 0;
            accesses_ = new Access[N];
            for (int i = 0; i < N; i++) {
                accesses_[i] = new Access();
            }
            this.N = N;
        }
        public Access NewAccess() {
            assert (access_count_ < N);
            Access ret = accesses_[access_count_];
            ++access_count_;
            return ret;
        }
        public Access GetAccess(int index) {
            return (accesses_[index]);
        }
        public void Clear() {
            access_count_ = 0;
        }
        public void Sort() {
            Arrays.sort(accesses_, 0, access_count_, Comparator.comparing(lhs -> lhs.access_record_));
        }
    }
}
