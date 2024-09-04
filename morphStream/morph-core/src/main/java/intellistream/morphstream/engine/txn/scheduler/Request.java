package intellistream.morphstream.engine.txn.scheduler;

import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.transaction.context.TxnContext;
import intellistream.morphstream.transNFV.common.VNFRequest;

import java.util.List;

public class Request {
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final String d_table;
    public final String d_key;
    public final int d_fieldIndex;
    public final TableRecord d_record;
    public final VNFRequest vnfRequest;
    public final String[] condition_tables;
    public final String[] condition_keys;
    public final int[] condition_fieldIndexes;
    public final List<TableRecord> condition_records;
    public BaseTable[] tables;

    public Request(VNFRequest vnfRequest) {
        this(null, null, null, null, vnfRequest);
    }

    //READY ONLY
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String d_table, Integer d_fieldIndex, VNFRequest vnfRequest) {
        this(txn_context, null, accessType, d_table, null, d_fieldIndex, null, null, null, null, null, vnfRequest);
    }

    //no column id
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String d_table,
                   String d_key,
                   int d_fieldIndex, TableRecord d_record,
                   String[] condition_tables,
                   String[] condition_keys,
                   int[] condition_fieldIndexes, List<TableRecord> condition_records,
                   VNFRequest vnfRequest) {
        this(txn_context, null, accessType, d_table, d_key, d_fieldIndex, d_record, condition_tables, condition_keys, condition_fieldIndexes, condition_records, vnfRequest);
    }

    //no condition, no ref.
    public Request(TxnContext txn_context,
                   CommonMetaTypes.AccessType accessType,
                   String d_table,
                   String d_key,
                   int d_fieldIndex, TableRecord d_record,
                   VNFRequest vnfRequest) {
        this(txn_context, null, accessType, d_table, d_key, d_fieldIndex, d_record, null, null, null, null, vnfRequest);
    }

    public Request(TxnContext txn_context,
                   BaseTable[] baseTable,
                   CommonMetaTypes.AccessType accessType,
                   String d_table,
                   String d_key,
                   int d_fieldIndex, TableRecord d_record,
                   String[] condition_tables,
                   String[] condition_keys,
                   int[] condition_fieldIndexes, List<TableRecord> condition_records,
                   VNFRequest vnfRequest) {
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.d_table = d_table;
        this.d_key = d_key;
        this.d_fieldIndex = d_fieldIndex;
        this.d_record = d_record; //record to write to (or read-from if txn only has one read request)
        this.vnfRequest = vnfRequest;
        this.condition_tables = condition_tables;
        this.condition_keys = condition_keys;
        this.condition_fieldIndexes = condition_fieldIndexes;
        this.condition_records = condition_records; //records to read from in write operation
        this.tables = baseTable; //used for non-deter, allow null input
    }

}
