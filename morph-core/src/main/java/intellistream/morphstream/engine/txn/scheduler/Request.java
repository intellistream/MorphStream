package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Function;

public class Request {
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String table_name;
    public final String src_key;
    public final TableRecord d_record;
    public final Function function;
    public final SchemaRecordRef record_ref;
    public final String[] condition_sourceTable;
    public final String[] condition_source;
    public final TableRecord[] condition_records;
    public long value;
    public BaseTable[] tables;

    public Request() {
        this(null, null, null);
    }

    //READY ONLY
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name) {
        this(txn_context, null, accessType, table_name, null, null, null, null, null, null, null);
    }

    //Write-only
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String src_key,
                   String table_name,
                   TableRecord d_record,
                   long value) {
        this(txn_context, accessType, table_name, src_key, d_record, null, null);
        this.value = value;
    }

    //no condition. no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref) {
        this(txn_context, accessType, table_name, src_key, d_record, function, record_ref, null, null, null);
    }

    //no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   String[] condition_sourceTable,
                   String[] condition_source,
                   TableRecord[] condition_records) {
        this(txn_context, null, accessType, table_name, src_key, d_record, function, record_ref, condition_sourceTable, condition_source, condition_records);
    }

    //no condition, no ref.
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord d_record,
                   Function function) {
        this(txn_context, null, accessType, table_name, src_key, d_record, function, null, null, null, null);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   String[] condition_sourceTable,
                   String[] condition_source,
                   TableRecord[] condition_records) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.table_name = table_name;
        this.src_key = src_key;
        this.d_record = d_record; //record to write to (or read-from if txn only has one read request)
        this.function = function; //primitive functions or UDF
        this.record_ref = record_ref;
        this.condition_sourceTable = condition_sourceTable;
        this.condition_source = condition_source;
        this.condition_records = condition_records; //records to read from
        this.tables = baseTable; //used for non-deter, allow null input
    }

}
