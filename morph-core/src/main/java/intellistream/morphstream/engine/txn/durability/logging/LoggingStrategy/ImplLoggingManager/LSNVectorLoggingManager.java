package intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager;

import intellistream.morphstream.common.io.ByteIO.DataInputView;
import intellistream.morphstream.common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import intellistream.morphstream.common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingEntry.LVLogRecord;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResource.ImplLoggingResources.LSNVectorLoggingResources;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.Attachment;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingHandler;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOLSNVectorStreamFactory;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.recovery.histroyviews.HistoryViews;
import intellistream.morphstream.engine.txn.durability.recovery.lsnvector.CSContext;
import intellistream.morphstream.engine.txn.durability.recovery.lsnvector.CommandPrecedenceGraph;
import intellistream.morphstream.engine.txn.durability.snapshot.LoggingOptions;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LVCLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LoggingEntry;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.storage.SchemaRecord;
import intellistream.morphstream.engine.txn.storage.TableRecord;
import intellistream.morphstream.engine.txn.storage.datatype.DataBox;
import intellistream.morphstream.engine.txn.storage.table.BaseTable;
import intellistream.morphstream.engine.txn.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.transaction.function.AVG;
import intellistream.morphstream.engine.txn.transaction.function.DEC;
import intellistream.morphstream.engine.txn.transaction.function.INC;
import intellistream.morphstream.engine.txn.transaction.function.SUM;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.AppConfig;
import java.util.concurrent.ConcurrentHashMap;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static intellistream.morphstream.util.FaultToleranceConstants.CompressionType.None;
import static java.nio.file.StandardOpenOption.READ;

public class LSNVectorLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(LSNVectorLoggingManager.class);
    private final ConcurrentHashMap<Integer, AtomicInteger> num = new ConcurrentHashMap<>();
    public ConcurrentHashMap<Integer, LVLogRecord> threadToLVLogRecord = new ConcurrentHashMap<>();
    //Used when recovery
    public CommandPrecedenceGraph cpg = new CommandPrecedenceGraph();
    protected String loggingPath;
    protected int app;
    protected LoggingOptions loggingOptions;
    protected int parallelNum;
    protected Map<String, BaseTable> tables;

    public LSNVectorLoggingManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        app = configuration.getInt("app");
        for (int i = 0; i < parallelNum; i++) {
            this.threadToLVLogRecord.put(i, new LVLogRecord(i));
            this.num.put(i, new AtomicInteger(0));
        }
    }

    public LSNVectorLoggingResources syncPrepareResource(int partitionId) {
        return new LSNVectorLoggingResources(partitionId, this.threadToLVLogRecord.get(partitionId));
    }

    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        LVCLog lvcLog = (LVCLog) logRecord;
        this.num.get(lvcLog.threadId).addAndGet(1);
        LVLogRecord lvLogRecord = threadToLVLogRecord.get(lvcLog.threadId);
        TableRecord tableRecord = this.tables.get(lvcLog.tableName).SelectKeyRecord(lvcLog.key);
        TableRecord[] conditions = new TableRecord[lvcLog.condition.length];
        for (int i = 0; i < lvcLog.condition.length; i++) {
            conditions[i] = this.tables.get(lvcLog.tableName).SelectKeyRecord(lvcLog.condition[i]);
        }
        lvLogRecord.addLog(lvcLog, tableRecord, parallelNum, conditions);
    }

    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        this.num.get(partitionId).set(0);
        NIOLSNVectorStreamFactory lsnVectorStreamFactory = new NIOLSNVectorStreamFactory(loggingPath);
        LSNVectorLoggingResources resources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = lsnVectorStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(lsnVectorStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = resources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }

    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
        this.cpg.addContext(redoLogResult.threadId, new CSContext(redoLogResult.threadId));
        for (int i = 0; i < redoLogResult.redoLogPaths.size(); i++) {
            MeasureTools.BEGIN_TPG_CONSTRUCTION_TIME_MEASURE(redoLogResult.threadId);
            Path walPath = Paths.get(redoLogResult.redoLogPaths.get(i));
            AsynchronousFileChannel afc = AsynchronousFileChannel.open(walPath, READ);
            int fileSize = (int) afc.size();
            ByteBuffer dataBuffer = ByteBuffer.allocate(fileSize);
            Future<Integer> result = afc.read(dataBuffer, 0);
            result.get();
            DataInputView inputView;
            if (loggingOptions.getCompressionAlg() != None) {
                inputView = new SnappyDataInputView(dataBuffer);//Default to use Snappy compression
            } else {
                inputView = new NativeDataInputView(dataBuffer);
            }
            byte[] object = inputView.readFullyDecompression();
            String[] strings = new String(object, StandardCharsets.UTF_8).split(" ");
            for (String log : strings) {
                LVCLog lvcLog = LVCLog.getLVCLogFromString(log);
                this.cpg.addTask(redoLogResult.threadId, lvcLog);
            }
            LOG.info("Thread " + redoLogResult.threadId + " has finished reading logs");
            MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(redoLogResult.threadId);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(redoLogResult.threadId);
            start_evaluate(this.cpg.getContext(redoLogResult.threadId));
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(redoLogResult.threadId);
            MeasureTools.SCHEDULE_TIME_RECORD(redoLogResult.threadId, 0);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
        }
    }

    private void start_evaluate(CSContext context) {
        INITIALIZE(context);
        do {
            EXPLORE(context);
            PROCESS(context);
        } while (!context.isFinished());
        RESET(context);
    }

    private void INITIALIZE(CSContext context) {
        this.cpg.init_globalLv(context);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
    }

    private void EXPLORE(CSContext context) {
        LVCLog lvcLog = context.tasks.pollFirst();
        while (!this.cpg.canEvaluate(lvcLog)) {
            this.cpg.numberOfLocks.put(context.threadId, this.cpg.numberOfLocks.get(context.threadId) + 1);
        }
        context.readyTask = lvcLog;
    }

    private void PROCESS(CSContext context) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.threadId);
        switch (app) {
            case 0:
                GSExecute(context.readyTask);
                break;
            case 3:
            case 2:
                TPExecute(context.readyTask);
                break;
            case 1:
                SLExecute(context.readyTask);
                break;
        }
        context.scheduledTaskCount++;
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.threadId);
        this.cpg.updateGlobalLV(context);
    }

    private void RESET(CSContext context) {
        MeasureTools.BEGIN_SCHEDULE_WAIT_TIME_MEASURE(context.threadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
        MeasureTools.END_SCHEDULE_WAIT_TIME_MEASURE(context.threadId);
        this.cpg.reset(context);
    }

    private void SLExecute(LVCLog task) {
//        if (task == null || task.isAborted)
//            return;
//        String table = task.tableName;
//        String pKey = task.key;
//        long bid = task.bid;
//        if (task.condition.length > 0) {
//            SchemaRecord preValue = this.tables.get(table).SelectKeyRecord(task.condition[0]).content_.readPreValues(bid);
//            long sourceAccountBalance = preValue.getValues().get(1).getLong();
//            AppConfig.randomDelay();
//            SchemaRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey).record_;
//            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
//            if (task.OperationFunction.equals(INC.class.getName())) {
//                tempo_record.getValues().get(1).incLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
//            } else if (task.OperationFunction.equals(DEC.class.getName())) {
//                tempo_record.getValues().get(1).decLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
//            }
//            this.tables.get(table).SelectKeyRecord(pKey).content_.updateMultiValues(bid, 0, false, tempo_record);
//        } else {
//            TableRecord src = this.tables.get(table).SelectKeyRecord(pKey);
//            SchemaRecord srcRecord = src.content_.readPreValues(bid);
//            List<DataBox> values = srcRecord.getValues();
//            AppConfig.randomDelay();
//            SchemaRecord tempo_record;
//            tempo_record = new SchemaRecord(values);//tempo record
//            tempo_record.getValues().get(1).incLong(Long.parseLong(task.parameter));//compute.
//            src.content_.updateMultiValues(bid, 0, false, tempo_record);
//        }
    }

    private void GSExecute(LVCLog task) {
//        if (task == null || task.isAborted)
//            return;
//        String table = task.tableName;
//        String pKey = task.key;
//        long bid = task.bid;
//        int keysLength = task.condition.length;
//        SchemaRecord[] preValues = new SchemaRecord[keysLength];
//        long sum = 0;
//        AppConfig.randomDelay();
//        for (int i = 0; i < keysLength; i++) {
//            preValues[i] = this.tables.get(table).SelectKeyRecord(task.condition[i]).content_.readPreValues(bid);
//            sum += preValues[i].getValues().get(1).getLong();
//        }
//        sum /= keysLength;
//        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
//        SchemaRecord schemaRecord = srcRecord.content_.readPreValues(bid);
//        SchemaRecord tempo_record = new SchemaRecord(schemaRecord);//tempo record
//        if (task.OperationFunction.equals(SUM.class.getName())) {
//            tempo_record.getValues().get(1).setLong(sum);//compute.
//        } else
//            throw new UnsupportedOperationException();
    }

    private void TPExecute(LVCLog task) {
//        if (task == null || task.isAborted)
//            return;
//        String table = task.tableName;
//        String pKey = task.key;
//        AppConfig.randomDelay();
//        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
//        if (task.OperationFunction.equals(AVG.class.getName())) {
//            double latestAvgSpeeds = srcRecord.record_.getValues().get(1).getDouble();
//            double lav;
//            if (latestAvgSpeeds == 0) {//not initialized
//                lav = Double.parseDouble(task.parameter);
//            } else
//                lav = (latestAvgSpeeds + Double.parseDouble(task.parameter)) / 2;
//
//            srcRecord.record_.getValues().get(1).setDouble(lav);//write to state.
//        } else {
//            HashSet cnt_segment = srcRecord.record_.getValues().get(1).getHashSet();
//            cnt_segment.add(Integer.parseInt(task.parameter));
//        }
    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean inspectAbortView(long groupId, int threadId, long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int inspectAbortNumber(long groupId, int threadId) {
        throw new UnsupportedOperationException("does not support inspectAbortNumber");
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void selectiveLoggingPartition(int partitionId) {
        throw new UnsupportedOperationException();
    }
}
