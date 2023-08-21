package scheduler.struct;

import content.common.CommonMetaTypes;
import durability.logging.LoggingEntry.LogRecord;
import durability.struct.Logging.DependencyLog;
import durability.struct.Logging.LVCLog;
import durability.struct.Logging.LoggingEntry;
import durability.struct.Logging.NativeCommandLog;
import scheduler.struct.op.WindowDescriptor;
import storage.SchemaRecordRef;
import storage.TableRecord;
import storage.TableRecordRef;
import storage.datatype.DataBox;
import transaction.context.TxnContext;
import transaction.function.Condition;
import transaction.function.Function;

import java.util.List;

import static content.common.ContentCommon.loggingRecord_type;
import static utils.FaultToleranceConstants.*;

/**
 * TODO: clean ``state" and ``reference".
 */
public abstract class AbstractOperation {

    //required by READ_WRITE_and Condition.
    public final Function function;
    public final String table_name;
    public final TxnContext txn_context;
    public final CommonMetaTypes.AccessType accessType;
    public final TableRecord d_record;
    public final long bid;
    //required by READ_WRITE_and Condition.
    public final String pKey;
    public volatile TableRecordRef records_ref;//for cross-record dependency.
    public volatile SchemaRecordRef record_ref;//required by read-only: the placeholder of the reading d_record.
    public List<DataBox> value_list;//required by write-only: the value_list to be used to update the d_record.
    //only update corresponding column.
    public long value;
    //required by READ_WRITE.
    public volatile TableRecord s_record;//only if it is different from d_record.
    public volatile TableRecord[] condition_records;
    public Condition condition;
    public final int[] success;
    public boolean isCommit = true;//It means that this operation has been added to LoggingManager.
    //required by Write-ahead-logging, Dependency logging, LV logging.
    public LoggingEntry logRecord;
    public WindowDescriptor windowContext;

    public AbstractOperation(Function function, String table_name, SchemaRecordRef record_ref, TableRecord[] condition_records, Condition condition, int[] success,
                             TxnContext txn_context, CommonMetaTypes.AccessType accessType, TableRecord s_record, TableRecord d_record, long bid, WindowDescriptor windowContext, String pKey) {
        this.function = function;
        this.table_name = table_name;
        this.record_ref = record_ref;//this holds events' record_ref.
        this.condition_records = condition_records;
        this.condition = condition;
        this.success = success;
        this.txn_context = txn_context;
        this.accessType = accessType;
        this.s_record = s_record;
        this.d_record = d_record;
        this.bid = bid;
        this.windowContext = windowContext;
        this.pKey = pKey;
        if (loggingRecord_type == LOGOption_path) {
            isCommit = false;
        } else if (loggingRecord_type == LOGOption_dependency) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.length];
                for (int i = 0; i < condition_records.length; i++) {
                    conditions[i] = condition_records[i].record_.GetPrimaryKey();
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new DependencyLog(bid, table_name, d_record.record_.GetPrimaryKey(), function.getClass().getName(), conditions, function.toString());
            this.isCommit = false;
        } else if (loggingRecord_type == LOGOption_wal) {
            this.logRecord = new LogRecord(table_name, bid, d_record.record_.GetPrimaryKey());
        } else if (loggingRecord_type == LOGOption_lv) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.length];
                for (int i = 0; i < condition_records.length; i++) {
                    conditions[i] = condition_records[i].record_.GetPrimaryKey();
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new LVCLog(bid, table_name, d_record.record_.GetPrimaryKey(), function.getClass().getName(), conditions, function.toString());
            this.isCommit = false;
        } else if (loggingRecord_type == LOGOption_command) {
            String[] conditions;
            if (condition_records != null) {
                conditions = new String[condition_records.length];
                for (int i = 0; i < condition_records.length; i++) {
                    conditions[i] = condition_records[i].record_.GetPrimaryKey();
                }
            } else {
                conditions = new String[0];
            }
            this.logRecord = new NativeCommandLog(bid, table_name, d_record.record_.GetPrimaryKey(), function.getClass().getName(), conditions, function.toString());
            this.isCommit = false;
        }
    }
}
