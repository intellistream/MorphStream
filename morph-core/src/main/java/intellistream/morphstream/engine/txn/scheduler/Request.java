package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.db.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.HashMap;

public class Request {
    public final FunctionContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String table_name;
    public final String write_key;
    public final TableRecord d_record;
    public final Function function;
    public final String[] condition_tables;
    public final String[] condition_keys;
    public final HashMap<String, TableRecord> condition_records;
    public BaseTable[] tables;

    public Request(Function function) {
        this(null, null, null, function);
    }

    //READY ONLY
    public Request(FunctionContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name, Function function) {
        this(txn_context, null, accessType, table_name, null, null, null, null, null, function);
    }

    //no column id
    public Request(FunctionContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   String[] condition_tables,
                   String[] condition_keys,
                   HashMap<String, TableRecord> condition_records,
                   Function function) {
        this(txn_context, null, accessType, table_name, write_key, d_record, condition_tables, condition_keys, condition_records, function);
    }

    //no condition, no ref.
    public Request(FunctionContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   Function function) {
        this(txn_context, null, accessType, table_name, write_key, d_record, null, null, null, function);
    }

    public Request(FunctionContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String write_key,
                   TableRecord d_record,
                   String[] condition_tables,
                   String[] condition_keys,
                   HashMap<String, TableRecord> condition_records,
                   Function function) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.table_name = table_name;
        this.write_key = write_key;
        this.d_record = d_record; //record to write to (or read-from if txn only has one read request)
        this.function = function;
        this.condition_tables = condition_tables;
        this.condition_keys = condition_keys;
        this.condition_records = condition_records; //records to read from in write operation
        this.tables = baseTable; //used for non-deter, allow null input
    }
    //no table record
    public Request(FunctionContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   HashMap<String, TableRecord> condition_records,
                   Function access) {
        this(txn_context, null, accessType, table_name, src_key, null, null, null, condition_records, access);
    }
}
