package intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager;

import intellistream.morphstream.common.io.ByteIO.DataInputView;
import intellistream.morphstream.common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import intellistream.morphstream.common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResource.ImplLoggingResources.DependencyLoggingResources;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.Attachment;
import intellistream.morphstream.engine.txn.durability.logging.LoggingResult.LoggingHandler;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import intellistream.morphstream.engine.txn.durability.logging.LoggingStream.ImplLoggingStreamFactory.NIODependencyStreamFactory;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.recovery.dependency.CSContext;
import intellistream.morphstream.engine.txn.durability.recovery.dependency.CommandPrecedenceGraph;
import intellistream.morphstream.engine.txn.durability.recovery.dependency.CommandTask;
import intellistream.morphstream.engine.txn.durability.recovery.histroyviews.HistoryViews;
import intellistream.morphstream.engine.txn.durability.snapshot.LoggingOptions;
import intellistream.morphstream.engine.txn.durability.struct.Logging.DependencyLog;
import intellistream.morphstream.engine.txn.durability.struct.Logging.LoggingEntry;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.db.storage.table.BaseTable;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;

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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static intellistream.morphstream.util.FaultToleranceConstants.CompressionType.None;
import static java.nio.file.StandardOpenOption.READ;

public class DependencyLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(DependencyLoggingManager.class);
    protected final int num_items;
    protected final int delta;
    //Used when recovery
    public CommandPrecedenceGraph cpg = new CommandPrecedenceGraph();
    protected String loggingPath;
    protected LoggingOptions loggingOptions;
    protected int parallelNum;
    protected Map<String, BaseTable> tables;
    protected int app;
    protected ConcurrentHashMap<Integer, Vector<DependencyLog>> threadToDependencyLog = new ConcurrentHashMap<>();

    public DependencyLoggingManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        num_items = configuration.getInt("NUM_ITEMS");
        app = configuration.getInt("app");
        delta = num_items / parallelNum;
        this.cpg.delta = delta;
        for (int i = 0; i < parallelNum; i++) {
            this.threadToDependencyLog.put(i, new Vector<>());
        }
    }

    public DependencyLoggingResources syncPrepareResource(int partitionId) throws IOException {
        return new DependencyLoggingResources(partitionId, this.threadToDependencyLog.get(partitionId));
    }

    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        DependencyLog dependencyLog = (DependencyLog) logRecord;
        this.threadToDependencyLog.get(getPartitionId(dependencyLog.key)).add(dependencyLog);
    }


    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIODependencyStreamFactory dependencyStreamFactory = new NIODependencyStreamFactory(loggingPath);
        DependencyLoggingResources dependencyLoggingResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = dependencyStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(dependencyStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = dependencyLoggingResources.createWriteBuffer(loggingOptions);
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
                DependencyLog dependencyLog = DependencyLog.getDependencyFromString(log);
                this.cpg.addTask(redoLogResult.threadId, new CommandTask(dependencyLog));
            }
            LOG.info("Thread " + redoLogResult.threadId + " has finished reading logs");
            MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(redoLogResult.threadId);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(redoLogResult.threadId);
            start_evaluate(this.cpg.threadToCSContextMap.get(redoLogResult.threadId));
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
        } while (!context.finished());
        RESET(context);
    }

    private void INITIALIZE(CSContext context) {
        MeasureTools.BEGIN_RECOVERY_CONSTRUCT_GRAPH_MEASURE(context.threadId);
        this.cpg.construct_graph(context);
        MeasureTools.END_RECOVERY_CONSTRUCT_GRAPH_MEASURE(context.threadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
    }

    private void EXPLORE(CSContext context) {
        CommandTask next = Next(context);
        while (next == null && !context.finished()) {//current level is all processed at the current thread.
            SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
            ProcessedToNextLevel(context);
            next = Next(context);
        }
        context.readyTask = next;
    }

    private void PROCESS(CSContext context) {
        MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(context.threadId);
        CommandTask commandTask = next(context);
        switch (app) {
            case 0:
                GSExecute(commandTask);
                break;
            case 3:
            case 2:
                TPExecute(commandTask);
                break;
            case 1:
                SLExecute(commandTask);
                break;
        }
        MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(context.threadId);
    }

    private void RESET(CSContext context) {
        MeasureTools.BEGIN_SCHEDULE_WAIT_TIME_MEASURE(context.threadId);
        SOURCE_CONTROL.getInstance().waitForOtherThreads(context.threadId);
        MeasureTools.END_SCHEDULE_WAIT_TIME_MEASURE(context.threadId);
        context.reset();
        this.cpg.reset(context);
    }

    private CommandTask next(CSContext context) {
        CommandTask commandTask = context.readyTask;
        context.readyTask = null;
        return commandTask;
    }

    protected CommandTask Next(CSContext context) {
        ArrayList<CommandTask> tasks = context.CurrentLayer();
        CommandTask commandTask = null;
        if (tasks != null && context.currentLevelIndex < tasks.size()) {
            commandTask = tasks.get(context.currentLevelIndex);
            context.currentLevelIndex++;
            context.scheduledTaskCount++;
        }
        return commandTask;
    }

    protected void ProcessedToNextLevel(CSContext context) {
        context.currentLevel++;
        context.currentLevelIndex = 0;
        //if (context.currentLevel == context.maxLevel)
        //IOUtils.println("Thread " + context.threadId + " has finished processing level " + (context.currentLevel - 1));
        //IOUtils.println("Thread " + context.threadId + " has " + context.scheduledTaskCount + " tasks in total");
    }

    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support registerTable");
    }

    @Override
    public boolean inspectAbortView(long groupId, int threadId, long bid) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectAbortView");
    }

    @Override
    public int inspectAbortNumber(long groupId, int threadId) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectAbortNumber");
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectDependencyView");
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support inspectTaskPlacing");
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException("DependencyLoggingManager does not support getHistoryViews");
    }

    @Override
    public void selectiveLoggingPartition(int partitionId) {
        throw new UnsupportedOperationException();
    }

    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

    private void SLExecute(CommandTask task) {
//        if (task == null || task.dependencyLog.isAborted) return;
//        String table = task.dependencyLog.tableName;
//        String pKey = task.dependencyLog.key;
//        double value = Double.parseDouble(task.dependencyLog.id);
//        long bid = (long) Math.floor(value);
//        if (task.dependencyLog.condition.length > 0) {
//            SchemaRecord preValue = this.tables.get(table).SelectKeyRecord(task.dependencyLog.condition[0]).content_.readPreValues(bid);
//            long sourceAccountBalance = preValue.getValues().get(1).getLong();
//            AppConfig.randomDelay();
//            SchemaRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey).record_;
//            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
//            if (task.dependencyLog.OperationFunction.equals(INC.class.getName())) {
//                tempo_record.getValues().get(1).incLong(sourceAccountBalance, Long.parseLong(task.dependencyLog.parameter));//compute.
//            } else if (task.dependencyLog.OperationFunction.equals(DEC.class.getName())) {
//                tempo_record.getValues().get(1).decLong(sourceAccountBalance, Long.parseLong(task.dependencyLog.parameter));//compute.
//            }
//            this.tables.get(table).SelectKeyRecord(pKey).content_.updateMultiValues(bid, 0, false, tempo_record);
//        } else {
//            TableRecord src = this.tables.get(table).SelectKeyRecord(pKey);
//            SchemaRecord srcRecord = src.content_.readPreValues(bid);
//            List<DataBox> values = srcRecord.getValues();
//            AppConfig.randomDelay();
//            SchemaRecord tempo_record;
//            tempo_record = new SchemaRecord(values);//tempo record
//            tempo_record.getValues().get(1).incLong(Long.parseLong(task.dependencyLog.parameter));//compute.
//            src.content_.updateMultiValues(bid, 0, false, tempo_record);
//        }
    }

    private void GSExecute(CommandTask task) {
//        if (task == null || task.dependencyLog.isAborted) return;
//        String table = task.dependencyLog.tableName;
//        String pKey = task.dependencyLog.key;
//        double value = Double.parseDouble(task.dependencyLog.id);
//        long bid = (long) Math.floor(value);
//        int keysLength = task.dependencyLog.condition.length;
//        SchemaRecord[] preValues = new SchemaRecord[keysLength];
//        long sum = 0;
//        AppConfig.randomDelay();
//        for (int i = 0; i < keysLength; i++) {
//            preValues[i] = this.tables.get(table).SelectKeyRecord(task.dependencyLog.condition[i]).content_.readPreValues(bid);
//            sum += preValues[i].getValues().get(1).getLong();
//        }
//        sum /= keysLength;
//        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
//        SchemaRecord schemaRecord = srcRecord.content_.readPreValues(bid);
//        SchemaRecord tempo_record = new SchemaRecord(schemaRecord);//tempo record
//        if (task.dependencyLog.OperationFunction.equals(SUM.class.getName())) {
//            tempo_record.getValues().get(1).setLong(sum);//compute.
//        } else
//            throw new UnsupportedOperationException();
    }

    private void TPExecute(CommandTask task) {
//        if (task == null || task.dependencyLog.isAborted) return;
//        String table = task.dependencyLog.tableName;
//        String pKey = task.dependencyLog.key;
//        double value = Double.parseDouble(task.dependencyLog.id);
//        long bid = (long) Math.floor(value);
//        AppConfig.randomDelay();
//        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
//        if (task.dependencyLog.OperationFunction.equals(AVG.class.getName())) {
//            double latestAvgSpeeds = srcRecord.record_.getValues().get(1).getDouble();
//            double lav;
//            if (latestAvgSpeeds == 0) {//not initialized
//                lav = Double.parseDouble(task.dependencyLog.parameter);
//            } else
//                lav = (latestAvgSpeeds + Double.parseDouble(task.dependencyLog.parameter)) / 2;
//
//            srcRecord.record_.getValues().get(1).setDouble(lav);//write to state.
//        } else {
//            HashSet cnt_segment = srcRecord.record_.getValues().get(1).getHashSet();
//            cnt_segment.add(Integer.parseInt(task.dependencyLog.parameter));
//        }
    }
}
