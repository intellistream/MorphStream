package state_engine.Meta;
public interface MetaTypes {
    int kEventsNum = 2;
    int kMaxProcedureNum = 10;
    int kMaxThreadNum = 40;
    int kMaxAccessNum = 1024;
    int kBatchTsNum = 16;
    int kLogBufferSize = 8388608 * 2;
    int kTxnBufferSize = 8192;
    int kLogChunkSize = 10;
    int kInsert = 0;
    int kUpdate = 1;
    int kDelete = 2;
    int kAdhocTxn = 100;
    enum LockType {
        NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK
    }
    enum AccessType {
        READ_ONLY, READS_ONLY, READ_WRITE, READ_WRITE_READ, READ_WRITE_COND, READ_WRITE_COND_READ, WRITE_ONLY, INSERT_ONLY, DELETE_ONLY
    }
}
