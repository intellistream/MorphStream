package engine.txn.durability.logging.LoggingStrategy.ImplLoggingManager;

import common.collections.Configuration;
import common.collections.OsUtils;
import common.io.ByteIO.DataInputView;
import common.io.ByteIO.InputWithDecompression.NativeDataInputView;
import common.io.ByteIO.InputWithDecompression.SnappyDataInputView;
import engine.txn.durability.ftmanager.FTManager;
import engine.txn.durability.logging.LoggingResource.ImplLoggingResources.CommandLoggingResources;
import engine.txn.durability.logging.LoggingResult.Attachment;
import engine.txn.durability.logging.LoggingResult.LoggingHandler;
import engine.txn.durability.logging.LoggingStrategy.LoggingManager;
import engine.txn.durability.logging.LoggingStream.ImplLoggingStreamFactory.NIOCommandStreamFactory;
import engine.txn.durability.recovery.RedoLogResult;
import engine.txn.durability.recovery.command.CommandPrecedenceGraph;
import engine.txn.durability.recovery.histroyviews.HistoryViews;
import engine.txn.durability.snapshot.LoggingOptions;
import engine.txn.durability.struct.Logging.LoggingEntry;
import engine.txn.durability.struct.Logging.NativeCommandLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import engine.txn.profiler.MeasureTools;
import engine.txn.storage.SchemaRecord;
import engine.txn.storage.TableRecord;
import engine.txn.storage.datatype.DataBox;
import engine.txn.storage.table.BaseTable;
import engine.txn.storage.table.RecordSchema;
import engine.txn.transaction.function.AVG;
import engine.txn.transaction.function.DEC;
import engine.txn.transaction.function.INC;
import engine.txn.transaction.function.SUM;
import util.AppConfig;
import engine.txn.utils.SOURCE_CONTROL;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static java.nio.file.StandardOpenOption.READ;
import static util.FaultToleranceConstants.CompressionType.None;

public class CommandLoggingManager implements LoggingManager {
    private static final Logger LOG = LoggerFactory.getLogger(CommandLoggingManager.class);
    @Nonnull
    protected String loggingPath;
    @Nonnull protected LoggingOptions loggingOptions;
    protected int parallelNum;
    protected final int num_items;
    protected final int delta;
    protected Map<String, BaseTable> tables;
    protected int app;
    protected ConcurrentHashMap<Integer, Vector<NativeCommandLog>> threadToCommandLog = new ConcurrentHashMap<>();
    protected CommandPrecedenceGraph cpg = new CommandPrecedenceGraph();
    public CommandLoggingManager(Map<String, BaseTable> tables, Configuration configuration) {
        this.tables = tables;
        loggingPath = configuration.getString("rootFilePath") + OsUtils.OS_wrapper("logging");
        parallelNum = configuration.getInt("parallelNum");
        loggingOptions = new LoggingOptions(parallelNum, configuration.getString("compressionAlg"));
        num_items = configuration.getInt("NUM_ITEMS");
        app = configuration.getInt("app");
        delta = num_items / parallelNum;
        for (int i = 0; i < parallelNum; i++) {
            threadToCommandLog.put(i, new Vector<>());
        }
    }
    public CommandLoggingResources syncPrepareResource(int partitionId) {
        return new CommandLoggingResources(partitionId, threadToCommandLog.get(partitionId));
    }
    @Override
    public void addLogRecord(LoggingEntry logRecord) {
        NativeCommandLog nativeCommandLog = (NativeCommandLog) logRecord;
        this.threadToCommandLog.get(getPartitionId(nativeCommandLog.key)).add(nativeCommandLog);
    }
    @Override
    public void commitLog(long groupId, int partitionId, FTManager ftManager) throws IOException {
        NIOCommandStreamFactory commandStreamFactory = new NIOCommandStreamFactory(loggingPath);
        CommandLoggingResources commandLoggingResources = syncPrepareResource(partitionId);
        AsynchronousFileChannel afc = commandStreamFactory.createLoggingStream();
        Attachment attachment = new Attachment(commandStreamFactory.getPath(), groupId, partitionId, afc, ftManager);
        ByteBuffer dataBuffer = commandLoggingResources.createWriteBuffer(loggingOptions);
        afc.write(dataBuffer, 0, attachment, new LoggingHandler());
    }
    @Override
    public void syncRetrieveLogs(RedoLogResult redoLogResult) throws IOException, ExecutionException, InterruptedException {
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
                NativeCommandLog nativeCommandLog = NativeCommandLog.getNativeCommandLog(log);
                this.cpg.addTask(nativeCommandLog);
            }
            LOG.info("Thread " + redoLogResult.threadId + " has finished reading logs");
            MeasureTools.END_TPG_CONSTRUCTION_TIME_MEASURE(redoLogResult.threadId);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
            MeasureTools.BEGIN_SCHEDULE_EXPLORE_TIME_MEASURE(redoLogResult.threadId);
            MeasureTools.BEGIN_SCHEDULE_USEFUL_TIME_MEASURE(redoLogResult.threadId);
            if (redoLogResult.threadId == 0) {//Only one thread will redo the command log serially
                LOG.info("Total tasks: " + this.cpg.tasks.size());
                start_evaluate();
            }
            MeasureTools.END_SCHEDULE_USEFUL_TIME_MEASURE(redoLogResult.threadId);
            MeasureTools.BEGIN_SCHEDULE_WAIT_TIME_MEASURE(redoLogResult.threadId);
            SOURCE_CONTROL.getInstance().waitForOtherThreads(redoLogResult.threadId);
            MeasureTools.END_SCHEDULE_WAIT_TIME_MEASURE(redoLogResult.threadId);
            MeasureTools.END_SCHEDULE_EXPLORE_TIME_MEASURE(redoLogResult.threadId);
            MeasureTools.SCHEDULE_TIME_RECORD(redoLogResult.threadId, 0);
        }
    }
    private void start_evaluate () {
        for (NativeCommandLog nativeCommandLog : this.cpg.tasks) {
            PROCESS(nativeCommandLog);
        }
        this.cpg.tasks.clear();
    }
    private void PROCESS(NativeCommandLog nativeCommandLog) {
        switch (app) {
            case 0:
                GSExecute(nativeCommandLog);
                break;
            case 3:
            case 2:
                TPExecute(nativeCommandLog);
                break;
            case 1:
                SLExecute(nativeCommandLog);
                break;
        }
    }
    @Override
    public void registerTable(RecordSchema recordSchema, String tableName) {
        throw new UnsupportedOperationException();
    }
    public int getPartitionId(String primary_key) {
        int key = Integer.parseInt(primary_key);
        return key / delta;
    }

    @Override
    public boolean inspectAbortView(long groupId, int threadId, long bid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int inspectAbortNumber(long groupId, int threadId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object inspectDependencyView(long groupId, String table, String from, String to, long bid) {
       throw new UnsupportedOperationException();
    }

    @Override
    public HashMap<String, List<Integer>> inspectTaskPlacing(long groupId, int threadId) {
        throw  new UnsupportedOperationException();
    }

    @Override
    public HistoryViews getHistoryViews() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void selectiveLoggingPartition(int partitionId) {
        throw  new UnsupportedOperationException();
    }

    private void SLExecute(NativeCommandLog task) {
        if (task == null) return;
        String table = task.tableName;
        String pKey = task.key;
        double value = Double.parseDouble(task.id);
        long bid = (long) Math.floor(value);
        if (task.condition.length > 0) {
            SchemaRecord preValue = this.tables.get(table).SelectKeyRecord(task.condition[0]).content_.readPreValues(bid);
            long sourceAccountBalance = preValue.getValues().get(1).getLong();
            AppConfig.randomDelay();
            SchemaRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey).record_;
            SchemaRecord tempo_record = new SchemaRecord(srcRecord);//tempo record
            if (task.OperationFunction.equals(INC.class.getName())) {
                tempo_record.getValues().get(1).incLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
            } else if (task.OperationFunction.equals(DEC.class.getName())) {
                tempo_record.getValues().get(1).decLong(sourceAccountBalance, Long.parseLong(task.parameter));//compute.
            }
            this.tables.get(table).SelectKeyRecord(pKey).content_.updateMultiValues(bid, 0, false, tempo_record);
        } else {
            TableRecord src = this.tables.get(table).SelectKeyRecord(pKey);
            SchemaRecord srcRecord = src.content_.readPreValues(bid);
            List<DataBox> values = srcRecord.getValues();
            AppConfig.randomDelay();
            SchemaRecord tempo_record;
            tempo_record = new SchemaRecord(values);//tempo record
            tempo_record.getValues().get(1).incLong(Long.parseLong(task.parameter));//compute.
            src.content_.updateMultiValues(bid, 0, false, tempo_record);
        }
    }
    private void GSExecute(NativeCommandLog task) {
        if (task == null || task.isAborted) return;
        String table = task.tableName;
        String pKey = task.key;
        double value = Double.parseDouble(task.id);
        long bid = (long) Math.floor(value);
        int keysLength = task.condition.length;
        SchemaRecord[] preValues = new SchemaRecord[keysLength];
        long sum = 0;
        AppConfig.randomDelay();
        for (int i = 0; i < keysLength; i++) {
            preValues[i] = this.tables.get(table).SelectKeyRecord(task.condition[i]).content_.readPreValues(bid);
            sum += preValues[i].getValues().get(1).getLong();
        }
        sum /= keysLength;
        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
        SchemaRecord schemaRecord = srcRecord.content_.readPreValues(bid);
        SchemaRecord tempo_record = new SchemaRecord(schemaRecord);//tempo record
        if (task.OperationFunction.equals(SUM.class.getName())) {
            tempo_record.getValues().get(1).setLong(sum);//compute.
        } else
            throw new UnsupportedOperationException();
    }
    private void TPExecute(NativeCommandLog task) {
        if (task == null || task.isAborted) return;
        String table = task.tableName;
        String pKey = task.key;
        double value = Double.parseDouble(task.id);
        AppConfig.randomDelay();
        TableRecord srcRecord = this.tables.get(table).SelectKeyRecord(pKey);
        if (task.OperationFunction.equals(AVG.class.getName())) {
            double latestAvgSpeeds = srcRecord.record_.getValues().get(1).getDouble();
            double lav;
            if (latestAvgSpeeds == 0) {//not initialized
                lav = Double.parseDouble(task.parameter);
            } else
                lav = (latestAvgSpeeds + Double.parseDouble(task.parameter)) / 2;

            srcRecord.record_.getValues().get(1).setDouble(lav);//write to state.
        } else {
            HashSet cnt_segment = srcRecord.record_.getValues().get(1).getHashSet();
            cnt_segment.add(Integer.parseInt(task.parameter));
        }

    }
}
