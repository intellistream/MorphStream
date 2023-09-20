package intellistream.morphstream.api.utils;

public interface MetaTypes {
    enum AccessType {
        READ, //read only (single record)
        WRITE, //write only (single record) + read multiple records, read after write
        WINDOW_READ, //read only on single record (all versions in one window)
        WINDOW_WRITE, //write only (single record) + read multiple records (all versions in one window)
        NON_DETER_READ, //read only (single record)
        NON_DETER_WRITE //write only (single record) + read multiple records
    }

    enum ValueType {
        INT, LONG, FLOAT, DOUBLE, STRING, BOOLEAN
    }
}
