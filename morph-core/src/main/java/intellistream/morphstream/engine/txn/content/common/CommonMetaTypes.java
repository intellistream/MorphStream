package intellistream.morphstream.engine.txn.content.common;

public interface CommonMetaTypes {
    int kEventsNum = 2;
    int kMaxProcedureNum = 10;
    int kMaxThreadNum = 50;
    int kMaxAccessNum = 1024;
    int kBatchTsNum = 16;
    int kLogBufferSize = 8388608 * 2;
    int kTxnBufferSize = 8192;
    int kLogChunkSize = 10;
    int kInsert = 0;
    int kUpdate = 1;
    int kDelete = 2;
    int kAdhocTxn = 100;
    String defaultString = "default";

    enum LockType {
        NO_LOCK, READ_LOCK, WRITE_LOCK, CERTIFY_LOCK
    }

    enum AccessType {
        READ_ONLY,
        READS_ONLY,
        READ_WRITE,
        READ_WRITE_READ,
        READ_WRITE_COND,
        READ_WRITE_COND_READ,
        WRITE_ONLY,
        INSERT_ONLY,
        DELETE_ONLY,
        GET, SET,
        READ_WRITE_COND_READN,
        WINDOWED_READ_ONLY,
        NON_READ_WRITE_COND_READN,
        READ,
        WRITE,
        WINDOW_READ,
        WINDOW_WRITE,
        NON_DETER_READ,
        NON_DETER_WRITE
    }
}
