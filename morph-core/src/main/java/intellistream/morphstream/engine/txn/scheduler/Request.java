package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.api.state.StateAccess;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;

import java.util.HashMap;

public class Request {
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String table_name;
    public final String write_key;
    public final TableRecord d_record;
    public final StateAccess stateAccess;
    public final String[] condition_tables;
    public final String[] condition_keys;
    public final HashMap<String, TableRecord> condition_records;
    public BaseTable[] tables;

    public Request(StateAccess stateAccess) {
        this(null, null, null, stateAccess);
    }

    //READY ONLY
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name, StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, null, null, stateAccess, null, null, null);
    }

    //no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   String[] condition_tables,
                   String[] condition_keys,
                   HashMap<String, TableRecord> condition_records,
                   StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, write_key, d_record, stateAccess, condition_tables, condition_keys, condition_records);
    }

    //no condition, no ref.
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   StateAccess stateAccess) {
        this(txn_context, null, accessType, table_name, write_key, d_record, stateAccess, null, null, null);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   StateAccess stateAccess,
                   String[] condition_tables,
                   String[] condition_keys,
                   HashMap<String, TableRecord> condition_records) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.table_name = table_name;
        this.write_key = write_key;
        this.d_record = d_record; //record to write to (or read-from if txn only has one read request)
        this.stateAccess = stateAccess;
        this.condition_tables = condition_tables;
        this.condition_keys = condition_keys;
        this.condition_records = condition_records; //records to read from in write operation
        this.tables = baseTable; //used for non-deter, allow null input
    }

}
