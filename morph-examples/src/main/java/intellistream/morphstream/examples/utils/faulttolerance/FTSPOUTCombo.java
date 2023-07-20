package intellistream.morphstream.examples.utils.faulttolerance;

import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.context.TopologyContext;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalBolt;
import intellistream.morphstream.engine.stream.components.operators.api.TransactionalSpout;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.collector.OutputCollector;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.util.FaultToleranceConstants.*;

public abstract class FTSPOUTCombo extends TransactionalSpout implements FaultTolerance {
    private static Logger LOG;
    public final String split_exp = ";";
    public int the_end;
    public int global_cnt;
    public int num_events_per_thread;
    public long[] mybids;
    public Object[] myevents;
    public int counter;
    public Tuple tuple;
    public Tuple marker;
    public GeneralMsg generalMsg;
    public int tthread;
    public FTSINKCombo sink = new FTSINKCombo();
    protected int totalEventsPerBatch = 0;
    protected TransactionalBolt bolt;//compose the bolt here.
    int start_measure;
    Random random = new Random();

    public FTSPOUTCombo(Logger log, int i) {
        super(log, i);
        LOG = log;
        this.scalable = false;
    }

    public abstract void loadEvent(String file_name, Configuration config, TopologyContext context, OutputCollector collector) throws FileNotFoundException;

    @Override
    public void nextTuple() throws InterruptedException {
        try {
            if (ftOption == FTOption_ISC) {
                nextTuple_ISC();
            } else if (ftOption == FTOption_WSC) {
                nextTuple_WSC();
            } else if (ftOption == FTOption_PATH) {
                nextTuple_PATH();
            } else if (ftOption == FTOption_LV) {
                nextTuple_LV();
            } else if (ftOption == FTOption_Dependency) {
                nextTuple_Dependency();
            } else if (ftOption == FTOption_Command) {
                nextTuple_Command();
            } else {
                nextTuple_Native();
            }
        } catch (IOException | ExecutionException | BrokenBarrierException | DatabaseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer default_scale(Configuration conf) {
        return 1;//4 for 7 sockets
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        if (enable_log) LOG.info("Spout initialize is being called");
        long start = System.nanoTime();
        taskId = getContext().getThisTaskIndex();//context.getThisTaskId(); start from 0..
        long pid = OsUtils.getPID();
        if (enable_log) LOG.info("JVM PID  = " + pid);
        long end = System.nanoTime();
        if (enable_log) LOG.info("spout initialize takes (ms):" + (end - start) / 1E6);
        ccOption = config.getInt("CCOption", 0);
        ftOption = config.getInt("FTOption", 0);
        bid = 0;
        counter = 0;

        punctuation_interval = config.getInt("checkpoint");
        snapshot_interval = punctuation_interval * config.getInt("snapshotInterval");
        arrivalControl = config.getBoolean("arrivalControl");
        inputStoreRootPath = config.getString("rootFilePath") + OsUtils.OS_wrapper("inputStore");
        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
        switch (config.getString("compressionAlg")) {
            case "None":
                this.compressionType = FaultToleranceConstants.CompressionType.None;
                break;
            case "XOR":// XOR GorillaV2
                this.compressionType = FaultToleranceConstants.CompressionType.XOR;
                break;
            case "Delta2Delta":// Delta-Delta GorillaV1
                this.compressionType = FaultToleranceConstants.CompressionType.Delta2Delta;
                break;
            case "Delta":// Delta DeltaBinary
                this.compressionType = FaultToleranceConstants.CompressionType.Delta;
                break;
            case "RLE":// Rle Rle
                this.compressionType = FaultToleranceConstants.CompressionType.RLE;
                break;
            case "Dictionary":// Dictionary
                this.compressionType = FaultToleranceConstants.CompressionType.Dictionary;
                break;
            case "Snappy":// Snappy Snappy
                this.compressionType = FaultToleranceConstants.CompressionType.Snappy;
                break;
            case "Zigzag":// Zigzag Zigzag
                this.compressionType = FaultToleranceConstants.CompressionType.Zigzag;
                break;
            case "Optimize":// Optimize Scabbard
                this.compressionType = FaultToleranceConstants.CompressionType.Optimize;
                break;
        }
        isRecovery = config.getBoolean("isRecovery");
        if (isRecovery) {
            remainTime = (long) (config.getInt("failureTime") * 1E6);
        }
        // set up the checkpoint interval for measurement
        sink.punctuation_interval = punctuation_interval;

        target_Hz = (int) config.getDouble("targetHz", 10000000);

        totalEventsPerBatch = config.getInt("totalEvents");
        tthread = config.getInt("tthread");

        num_events_per_thread = totalEventsPerBatch / tthread;

        if (enable_log) LOG.info("total events... " + totalEventsPerBatch);
        if (enable_log) LOG.info("total events per thread = " + num_events_per_thread);
        if (enable_log) LOG.info("checkpoint_interval = " + punctuation_interval);

        start_measure = CONTROL.MeasureStart;

        mybids = new long[num_events_per_thread];
        myevents = new Object[num_events_per_thread];
        the_end = num_events_per_thread;

        if (config.getInt("CCOption", 0) == CCOption_SStore) {
            global_cnt = (the_end) * tthread;
        } else {
            global_cnt = (the_end - CONTROL.MeasureStart) * tthread;
        }
    }

    @Override
    public boolean snapshot(int counter) throws InterruptedException, BrokenBarrierException {
        return (counter % snapshot_interval == 0);
    }

    @Override
    public boolean input_store(long currentOffset) throws IOException, ExecutionException, InterruptedException {
        this.inputDurabilityHelper.storeInput(this.myevents, currentOffset, punctuation_interval, inputStoreCurrentPath);
        return true;
    }

    @Override
    public long recoverData() throws IOException, ExecutionException, InterruptedException {
        SnapshotResult snapshotResult = (SnapshotResult) this.ftManager.spoutAskRecovery(this.taskId, 0L);
        if (snapshotResult != null) {
            this.bolt.db.syncReloadDB(snapshotResult);
        } else {
            snapshotResult = new SnapshotResult(0L, this.taskId, null);
        }
        if (ftOption == FTOption_WSC) {
            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
            if (redoLogResult.redoLogPaths.size() != 0) {
                this.db.syncRetrieveLogs(redoLogResult);
                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
                counter = (int) redoLogResult.lastedGroupId;
            } else {
                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
                counter = (int) snapshotResult.snapshotId;
            }
        } else if (ftOption == FTOption_PATH) {
            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
            if (redoLogResult.redoLogPaths.size() != 0) {
                this.db.syncRetrieveLogs(redoLogResult);
                this.inputDurabilityHelper.historyViews = this.db.getLoggingManager().getHistoryViews();
            }
            input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
            counter = (int) snapshotResult.snapshotId;
            int abort = this.db.getLoggingManager().inspectAbortNumber(counter + punctuation_interval, this.taskId);
            counter = counter + abort;
        } else if (ftOption == FTOption_Dependency) {
            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
            if (redoLogResult.redoLogPaths.size() != 0) {
                this.db.syncRetrieveLogs(redoLogResult);
                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
                counter = (int) redoLogResult.lastedGroupId;
            } else {
                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
                counter = (int) snapshotResult.snapshotId;
            }
        } else if (ftOption == FTOption_LV) {
            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
            if (redoLogResult.redoLogPaths.size() != 0) {
                this.db.syncRetrieveLogs(redoLogResult);
                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
                counter = (int) redoLogResult.lastedGroupId;
            } else {
                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
                counter = (int) snapshotResult.snapshotId;
            }
        } else if (ftOption == FTOption_Command) {
            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
            if (redoLogResult.redoLogPaths.size() != 0) {
                this.db.syncRetrieveLogs(redoLogResult);
                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
                counter = (int) redoLogResult.lastedGroupId;
            } else {
                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
                counter = (int) snapshotResult.snapshotId;
            }
        } else if (ftOption == FTOption_ISC) {
            input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
            counter = (int) snapshotResult.snapshotId;
        }
        this.sink.lastTask = this.ftManager.sinkAskLastTask(this.taskId);
        this.sink.startRecovery = snapshotResult.snapshotId;
        return snapshotResult.snapshotId;
    }

    @Override
    public boolean input_reload(long snapshotOffset, long redoOffset) throws IOException, ExecutionException, InterruptedException {
        File file = new File(inputStoreRootPath + OsUtils.OS_wrapper(Long.toString(snapshotOffset)) + OsUtils.OS_wrapper(taskId + ".input"));
        if (file.exists()) {
            inputDurabilityHelper.reloadInput(file, recoveryInput, redoOffset, snapshotOffset, punctuation_interval);
            return true;
        } else {
            return false;
        }
    }

    private void nextTuple_Native() throws BrokenBarrierException, IOException, InterruptedException, DatabaseException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration));
                    bolt.execute(marker);
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_ISC() throws BrokenBarrierException, IOException, InterruptedException, DatabaseException, ExecutionException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
            if (isRecovery) {
                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
                recoverData();
                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
                } else {
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
                    this.sink.stopRecovery = true;
                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
                    isRecovery = false;
                    inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                    if (counter != the_end)
                        input_store(counter);
                    if (counter == the_end) {
                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                        if (taskId == 0)
                            sink.end(global_cnt);
                    }
                }
            } else {
                input_store(counter);
            }
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "", counter));
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_WSC() throws InterruptedException, BrokenBarrierException, IOException, DatabaseException, ExecutionException {
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
                    }
                    if (this.taskId == 0) {
                        this.loggingManager.spoutRegister(counter, "");
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_PATH() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
            if (isRecovery) {
                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
                long lastSnapshotId = recoverData();
                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
                } else {
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
                    this.sink.stopRecovery = true;
                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
                    isRecovery = false;
                    if (snapshot(counter) && counter != lastSnapshotId) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                        bolt.execute(marker);
                        if (counter != the_end)
                            input_store(counter);
                    }
                    if (counter == the_end) {
                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                        if (taskId == 0)
                            sink.end(global_cnt);
                    }
                }
            } else {
                input_store(counter);
            }
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
                bid = ((TxnEvent) event).getBid();
                if (CONTROL.enable_latency_measurement) {
                    long time;
                    if (arrivalControl) {
                        time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                    } else {
                        time = System.nanoTime();
                    }
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
                } else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }
                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);
            } else {
                event = myevents[counter];
                long bid = mybids[counter];
                if (CONTROL.enable_latency_measurement) {
                    long time;
                    if (arrivalControl) {
                        time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                    } else {
                        time = System.nanoTime();
                    }
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
                } else {
                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
                }
                tuple = new Tuple(bid, this.taskId, context, generalMsg);
                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            }
            counter++;
            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot_early", counter));
                        } else {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        }
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_early", counter));
                        } else {
                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "", counter));
                        }
                    }
                    if (this.taskId == 0) {
                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                            this.loggingManager.spoutRegister(counter, "");
                        } else {
                            LOG.info("Recovery epoch: " + counter);
                        }
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                    counter = counter + this.db.getLoggingManager().inspectAbortNumber(counter + punctuation_interval, this.taskId);
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_Dependency() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
            if (isRecovery) {
                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
                long lastSnapshotId = recoverData();
                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
                } else {
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
                    this.sink.stopRecovery = true;
                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
                    isRecovery = false;
                    if (snapshot(counter) && counter != lastSnapshotId) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                        bolt.execute(marker);
                        if (counter != the_end)
                            input_store(counter);
                    }
                    if (counter == the_end) {
                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                        if (taskId == 0)
                            sink.end(global_cnt);
                    }
                }
            } else {
                input_store(counter);
            }
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
                    }
                    if (this.taskId == 0) {
                        this.loggingManager.spoutRegister(counter, "");
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_LV() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
            if (isRecovery) {
                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
                long lastSnapshotId = recoverData();
                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
                } else {
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
                    this.sink.stopRecovery = true;
                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
                    isRecovery = false;
                    if (snapshot(counter) && counter != lastSnapshotId) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                        bolt.execute(marker);
                        if (counter != the_end)
                            input_store(counter);
                    }
                    if (counter == the_end) {
                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                        if (taskId == 0)
                            sink.end(global_cnt);
                    }
                }
            } else {
                input_store(counter);
            }
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
                    }
                    if (this.taskId == 0) {
                        this.loggingManager.spoutRegister(counter, "");
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

    private void nextTuple_Command() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
        if (counter == start_measure) {
            if (taskId == 0) {
                sink.start();
            }
            this.systemStartTime = System.nanoTime();
            sink.previous_measure_time = System.nanoTime();
            if (isRecovery) {
                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
                long lastSnapshotId = recoverData();
                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
                } else {
                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
                    this.sink.stopRecovery = true;
                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
                    isRecovery = false;
                    if (snapshot(counter) && counter != lastSnapshotId) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                        bolt.execute(marker);
                        if (counter != the_end)
                            input_store(counter);
                    }
                    if (counter == the_end) {
                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                        if (taskId == 0)
                            sink.end(global_cnt);
                    }
                }
            } else {
                input_store(counter);
            }
        }
        if (counter < num_events_per_thread) {
            Object event;
            if (recoveryInput.size() != 0) {
                event = recoveryInput.poll();
            } else {
                event = myevents[counter];
            }
            long bid = mybids[counter];
            if (CONTROL.enable_latency_measurement) {
                long time;
                if (arrivalControl) {
                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
                } else {
                    time = System.nanoTime();
                }
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
            } else {
                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
            }

            tuple = new Tuple(bid, this.taskId, context, generalMsg);
            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
            counter++;

            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
                if (model_switch(counter)) {
                    if (snapshot(counter)) {
                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
                        if (this.taskId == 0) {
                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
                        }
                    } else {
                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
                    }
                    if (this.taskId == 0) {
                        this.loggingManager.spoutRegister(counter, "");
                    }
                    bolt.execute(marker);
                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
                        input_store(counter);
                    }
                }
            }
            if (counter == the_end) {
                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
                if (taskId == 0)
                    sink.end(global_cnt);
            }
        }
    }

}
