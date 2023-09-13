package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Function;

import java.util.HashMap;

public class Request {
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String table_name;
    public final String write_key;
    public final TableRecord d_record;
    public final Function function;
    public final StateAccess stateAccess;
    public final String[] read_tables;
    public final String[] read_keys;
    public final HashMap<String, TableRecord> read_records;
    public long value;
    public BaseTable[] tables;

    public Request(StateAccess stateAccess, StateAccess stateAccess1) {
        this(null, null, null, stateAccess);
    }

    //READY ONLY
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name, StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, null, null, null, stateAccess, null, null, null);
    }

    //Write-only
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name, String write_key,
                   TableRecord d_record,
                   StateAccess stateAccess, long value) {
        this(txn_context, accessType, table_name, write_key, d_record, null, stateAccess);
        this.value = value;
    }

    //no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   Function function,
                   String[] read_tables,
                   String[] read_keys,
                   HashMap<String, TableRecord> read_records,
                   StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, write_key, d_record, function, stateAccess, read_tables, read_keys, read_records);
    }

    //no condition, no ref.
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   Function function, StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, write_key, d_record, function, stateAccess, null, null, null);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   Function function,
                   StateAccess stateAccess,
                   String[] read_tables,
                   String[] read_keys,
                   HashMap<String, TableRecord> read_records) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.table_name = table_name;
        this.write_key = write_key;
        this.d_record = d_record; //record to write to (or read-from if txn only has one read request)
        this.function = function; //primitive functions or UDF
        this.stateAccess = stateAccess;
        this.read_tables = read_tables;
        this.read_keys = read_keys;
        this.read_records = read_records; //records to read from
        this.tables = baseTable; //used for non-deter, allow null input
    }

}
