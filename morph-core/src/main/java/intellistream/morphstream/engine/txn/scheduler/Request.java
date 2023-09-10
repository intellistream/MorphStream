package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.storage.SchemaRecordRef;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.engine.txn.transaction.function.Condition;
import intellistream.morphstream.engine.txn.transaction.function.Function;

public class Request {
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String table_name;
    public final String src_key;
    public final TableRecord s_record;
    public final TableRecord d_record;
    public final Function function;
    public final SchemaRecordRef record_ref;
    public final String[] condition_sourceTable;
    public final String[] condition_source;
    public final TableRecord[] condition_records;
    public final Condition condition;
    public final int[] success;
    public final int column_id;
    public final double[] enqueue_time;
    public long value;
    public BaseTable[] tables;

    public Request() {
        this(null, null, null);
    }

    //READY ONLY
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name) {
        this(txn_context, accessType, table_name, null);
    }

    //Write-only
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String src_key,
                   String table_name,
                   TableRecord d_record,
                   long value) {
        this(txn_context, accessType, table_name, src_key, null, d_record, null, null);
        this.value = value;
    }

    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name, double[] enqueue_time) {
        this(txn_context, null, accessType, table_name, null, null, null, null, null, null, null, null, null, null, -1, enqueue_time);
    }

    //no condition, no ref. no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function) {
        this(txn_context, accessType, table_name, src_key, s_record, d_record, function, null);
    }

    //no condition. no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref) {
        this(txn_context, accessType, table_name, src_key, s_record, d_record, function, record_ref, null, null, null, null, null);
    }

    //condition. no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   Condition condition,
                   int[] success) {
        this(txn_context, accessType, table_name, src_key, s_record, d_record, function, record_ref, null, null, null, condition, success);
    }

    //no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, int[] success) {
        this(txn_context, null, accessType, table_name, src_key, s_record, d_record, function, record_ref, condition_sourceTable, condition_source, condition_records, condition, success, -1, null);
    }

    //no column id and condition
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, int[] success) {
        this(txn_context, null, accessType, table_name, src_key, s_record, d_record, function, record_ref, condition_sourceTable, condition_source, condition_records, null, success, -1, null);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   SchemaRecordRef record_ref,
                   String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, int[] success) {
        this(txn_context, baseTable, accessType, table_name, src_key, s_record, d_record, function, record_ref, condition_sourceTable, condition_source, condition_records, null, success, -1, null);
    }

    //no condition, no ref.
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key,
                   TableRecord s_record,
                   TableRecord d_record,
                   Function function,
                   int column_id) {
        this(txn_context, null, accessType, table_name, src_key, s_record, d_record, function, null, null, null, null, null, null, column_id, null);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String table_name,
                   String src_key, TableRecord s_record, TableRecord d_record, Function function, SchemaRecordRef record_ref, String[] condition_sourceTable, String[] condition_source, TableRecord[] condition_records, Condition condition, int[] success, int column_id, double[] enqueue_time) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.table_name = table_name;
        this.src_key = src_key;
        this.s_record = s_record; //replace by r_record[]?
        this.d_record = d_record; //rename to w_record
        this.function = function; //replace with String (className & udfName)
        this.record_ref = record_ref;
        this.condition_sourceTable = condition_sourceTable;
        this.condition_source = condition_source;
        this.condition_records = condition_records;
        this.condition = condition; //remove, done in txnUDF
        this.success = success; //int[1], initialized during txnEvent creation as {0}
        this.column_id = column_id; //remove, useless
        this.enqueue_time = enqueue_time; //remove, never used
        this.tables = baseTable; //used for non-deter, allow null input
    }

}
