package intellistream.morphstream.engine.txn.scheduler.struct;

import intellistream.morphstream.api.state.Function;
import intellistream.morphstream.engine.txn.content.common.CommonMetaTypes;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.LogRecord;
import intellistream.morphstream.engine.txn.durability.struct.Logging.DependencyLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LVCLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LoggingEntry;
import intellistream.morphstream.engine.txn.durability.struct.Logging.NativeCommandLog;
import intellistream.morphstream.engine.txn.scheduler.struct.op.WindowDescriptor;
import intellistream.morphstream.engine.db.storage.record.TableRecord;
import intellistream.morphstream.engine.txn.transaction.context.FunctionContext;

import java.util.HashMap;

import static intellistream.morphstream.engine.txn.content.common.ContentCommon.loggingRecord_type;
import static intellistream.morphstream.util.FaultToleranceConstants.*;

/**
 * TODO: clean ``state" and ``reference".
 */
public abstract class AbstractOperation {
    public boolean isReference;
    //required by READ_WRITE_and Condition.
    public final String table_name;
    public final FunctionContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final TableRecord d_record;
    public final long bid;
    //required by READ_WRITE_and Condition.
    public final String pKey;
    public volatile Function function; //carries all schemaRecords involved
    public volatile HashMap<String, TableRecord> condition_records;//client-defined record name -> TableRecord
    public boolean isCommit = true;//It means that this operation has been added to LoggingManager.
    //required by Write-ahead-logging, Dependency logging, LV logging.
    public LoggingEntry logRecord;
    public WindowDescriptor windowContext;

    public AbstractOperation(String table_name, Function function, HashMap<String, TableRecord> condition_records,
                             FunctionContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord d_record, long bid, WindowDescriptor windowContext, String pKey) {
        this.table_name = table_name;
        this.function = function;
        this.condition_records = condition_records;
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.d_record = d_record;
        this.bid = bid;
        this.windowContext = windowContext;
        this.pKey = pKey;
        if (loggingRecord_type == LOGOption_path) {
            isCommit = false;
        } else if (loggingRecord_type == LOGOption_dependency) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.size()];
                int i = 0;
                for (TableRecord tableRecord : condition_records.values()) {
                    conditions[i] = tableRecord.record_.GetPrimaryKey();
                    i++;
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new DependencyLog(bid, table_name, d_record.record_.GetPrimaryKey(),
                    (String) function.getValue("function"), conditions, function.getValueMap().toString());
            this.isCommit = false;
        } else if (loggingRecord_type == LOGOption_wal) {
            this.logRecord = new LogRecord(table_name, bid, d_record.record_.GetPrimaryKey());
        } else if (loggingRecord_type == LOGOption_lv) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.size()];
                int i = 0;
                for (TableRecord tableRecord : condition_records.values()) {
                    conditions[i] = tableRecord.record_.GetPrimaryKey();
                    i++;
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new LVCLog(bid, table_name, d_record.record_.GetPrimaryKey(),
                    (String) function.getValue("function"), conditions, function.getValueMap().toString());
            this.isCommit = false;
        } else if (loggingRecord_type == LOGOption_command) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.size()];
                int i = 0;
                for (TableRecord tableRecord : condition_records.values()) {
                    conditions[i] = tableRecord.record_.GetPrimaryKey();
                    i++;
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new NativeCommandLog(bid, table_name, d_record.record_.GetPrimaryKey(),
                    (String) function.getValue("function"), conditions, function.getValueMap().toString());
            this.isCommit = false;
        }
    }
}
