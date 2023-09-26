package intellistream.morphstream.api.operator.spout.ft;

import intellistream.morphstream.api.launcher.MorphStreamEnv;
import intellistream.morphstream.api.operator.bolt.SStoreBolt;
import intellistream.morphstream.api.operator.bolt.ft.MorphStreamBoltFT;
import intellistream.morphstream.configuration.CONTROL;
import intellistream.morphstream.configuration.Configuration;
import intellistream.morphstream.engine.stream.components.operators.api.FaultTolerance;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutCombo;
import intellistream.morphstream.engine.stream.components.operators.api.spout.AbstractSpoutComboFT;
import intellistream.morphstream.engine.stream.execution.ExecutionGraph;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Tuple;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs.GeneralMsg;
import intellistream.morphstream.engine.txn.TxnEvent;
import intellistream.morphstream.engine.txn.db.DatabaseException;
import intellistream.morphstream.engine.txn.durability.ftmanager.FTManager;
import intellistream.morphstream.engine.txn.durability.inputStore.InputDurabilityHelper;
import intellistream.morphstream.engine.txn.durability.recovery.RedoLogResult;
import intellistream.morphstream.engine.txn.durability.snapshot.SnapshotResult.SnapshotResult;
import intellistream.morphstream.engine.txn.profiler.MeasureTools;
import intellistream.morphstream.engine.txn.profiler.Metrics;
import intellistream.morphstream.engine.txn.transaction.TxnDescription;
import intellistream.morphstream.engine.txn.utils.SOURCE_CONTROL;
import intellistream.morphstream.util.FaultToleranceConstants;
import intellistream.morphstream.util.OsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;

import static intellistream.morphstream.configuration.CONTROL.enable_log;
import static intellistream.morphstream.configuration.Constants.*;
import static intellistream.morphstream.configuration.Constants.DEFAULT_STREAM_ID;
import static intellistream.morphstream.util.FaultToleranceConstants.*;
import static intellistream.morphstream.util.FaultToleranceConstants.FTOption_ISC;

public class ApplicationSpoutComboISC extends AbstractSpoutComboFT {
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationSpoutComboISC.class);
    private HashMap<String, TxnDescription> TxnDescriptionHashMap;
    private Configuration conf = MorphStreamEnv.get().configuration();
    public ApplicationSpoutComboISC(HashMap<String, TxnDescription> txnDescriptionHashMap) throws Exception {
        super(LOG, 0);
        this.TxnDescriptionHashMap = txnDescriptionHashMap;
    }

    @Override
    public void initialize(int thread_Id, int thisTaskId, ExecutionGraph graph) {
        super.initialize(thread_Id, thisTaskId, graph);
        switch (config.getInt("CCOption", 0)) {
            case CCOption_MorphStream: {//T-Stream
                bolt = new MorphStreamBoltFT(TxnDescriptionHashMap, 0, this.sink);
                break;
            }
            case CCOption_SStore:
                bolt = new SStoreBolt(TxnDescriptionHashMap, 0, this.sink);
            default:
                if (enable_log) LOG.error("Please select correct CC option!");
        }
    }

    @Override
    public void nextTuple() throws InterruptedException {

    }
    @Override
    public long recoverData() throws IOException, ExecutionException, InterruptedException {
//        SnapshotResult snapshotResult = (SnapshotResult) this.ftManager.spoutAskRecovery(this.taskId, 0L);
//        if (snapshotResult != null) {
//            this.bolt.db.syncReloadDB(snapshotResult);
//        } else {
//            snapshotResult = new SnapshotResult(0L, this.taskId, null);
//        }
//        if (ftOption == FTOption_WSC) {
//            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
//            if (redoLogResult.redoLogPaths.size() != 0) {
//                this.db.syncRetrieveLogs(redoLogResult);
//                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
//                counter = (int) redoLogResult.lastedGroupId;
//            } else {
//                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//                counter = (int) snapshotResult.snapshotId;
//            }
//        } else if (ftOption == FTOption_PATH) {
//            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
//            if (redoLogResult.redoLogPaths.size() != 0) {
//                this.db.syncRetrieveLogs(redoLogResult);
//                this.inputDurabilityHelper.historyViews = this.db.getLoggingManager().getHistoryViews();
//            }
//            input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//            counter = (int) snapshotResult.snapshotId;
//            int abort = this.db.getLoggingManager().inspectAbortNumber(counter + punctuation_interval, this.taskId);
//            counter = counter + abort;
//        } else if (ftOption == FTOption_Dependency) {
//            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
//            if (redoLogResult.redoLogPaths.size() != 0) {
//                this.db.syncRetrieveLogs(redoLogResult);
//                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
//                counter = (int) redoLogResult.lastedGroupId;
//            } else {
//                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//                counter = (int) snapshotResult.snapshotId;
//            }
//        } else if (ftOption == FTOption_LV) {
//            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
//            if (redoLogResult.redoLogPaths.size() != 0) {
//                this.db.syncRetrieveLogs(redoLogResult);
//                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
//                counter = (int) redoLogResult.lastedGroupId;
//            } else {
//                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//                counter = (int) snapshotResult.snapshotId;
//            }
//        } else if (ftOption == FTOption_Command) {
//            RedoLogResult redoLogResult = (RedoLogResult) this.loggingManager.spoutAskRecovery(this.taskId, snapshotResult.snapshotId);
//            if (redoLogResult.redoLogPaths.size() != 0) {
//                this.db.syncRetrieveLogs(redoLogResult);
//                input_reload(snapshotResult.snapshotId, redoLogResult.lastedGroupId);
//                counter = (int) redoLogResult.lastedGroupId;
//            } else {
//                input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//                counter = (int) snapshotResult.snapshotId;
//            }
//        } else if (ftOption == FTOption_ISC) {
//            input_reload(snapshotResult.snapshotId, snapshotResult.snapshotId);
//            counter = (int) snapshotResult.snapshotId;
//        }
//        this.sink.lastTask = this.ftManager.sinkAskLastTask(this.taskId);
//        this.sink.startRecovery = snapshotResult.snapshotId;
//        return snapshotResult.snapshotId;
        return 0;
    }
    private void nextTuple_ISC() throws BrokenBarrierException, IOException, InterruptedException, DatabaseException, ExecutionException {
//        if (counter == start_measure) {
//            if (taskId == 0) {
//                sink.start();
//            }
//            this.systemStartTime = System.nanoTime();
//            sink.previous_measure_time = System.nanoTime();
//            if (isRecovery) {
//                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
//                recoverData();
//                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
//                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
//                } else {
//                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
//                    this.sink.stopRecovery = true;
//                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
//                    isRecovery = false;
//                    inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                    if (counter != the_end)
//                        input_store(counter);
//                    if (counter == the_end) {
//                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                        if (taskId == 0)
//                            sink.end(global_cnt);
//                    }
//                }
//            } else {
//                input_store(counter);
//            }
//        }
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//            } else {
//                event = myevents[counter];
//            }
//            long bid = mybids[counter];
//            if (CONTROL.enable_latency_measurement) {
//                long time;
//                if (arrivalControl) {
//                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                } else {
//                    time = System.nanoTime();
//                }
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//            } else {
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//            }
//
//            tuple = new Tuple(bid, this.taskId, context, generalMsg);
//            bolt.execute(tuple);
//            counter++;
//
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "", counter));
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }

    private void nextTuple_WSC() throws InterruptedException, BrokenBarrierException, IOException, DatabaseException, ExecutionException {
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//            } else {
//                event = myevents[counter];
//            }
//            long bid = mybids[counter];
//            if (CONTROL.enable_latency_measurement) {
//                long time;
//                if (arrivalControl) {
//                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                } else {
//                    time = System.nanoTime();
//                }
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//            } else {
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//            }
//
//            tuple = new Tuple(bid, this.taskId, context, generalMsg);
//            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//            counter++;
//
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
//                    }
//                    if (this.taskId == 0) {
//                        this.loggingManager.spoutRegister(counter, "");
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }

    private void nextTuple_PATH() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
//        if (counter == start_measure) {
//            if (taskId == 0) {
//                sink.start();
//            }
//            this.systemStartTime = System.nanoTime();
//            sink.previous_measure_time = System.nanoTime();
//            if (isRecovery) {
//                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
//                long lastSnapshotId = recoverData();
//                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
//                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
//                } else {
//                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
//                    this.sink.stopRecovery = true;
//                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
//                    isRecovery = false;
//                    if (snapshot(counter) && counter != lastSnapshotId) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                        bolt.execute(marker);
//                        if (counter != the_end)
//                            input_store(counter);
//                    }
//                    if (counter == the_end) {
//                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                        if (taskId == 0)
//                            sink.end(global_cnt);
//                    }
//                }
//            } else {
//                input_store(counter);
//            }
//        }
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//                bid = ((TxnEvent) event).getBid();
//                if (CONTROL.enable_latency_measurement) {
//                    long time;
//                    if (arrivalControl) {
//                        time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                    } else {
//                        time = System.nanoTime();
//                    }
//                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//                } else {
//                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//                }
//                tuple = new Tuple(bid, this.taskId, context, generalMsg);
//                bolt.execute(tuple);
//            } else {
//                event = myevents[counter];
//                long bid = mybids[counter];
//                if (CONTROL.enable_latency_measurement) {
//                    long time;
//                    if (arrivalControl) {
//                        time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                    } else {
//                        time = System.nanoTime();
//                    }
//                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//                } else {
//                    generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//                }
//                tuple = new Tuple(bid, this.taskId, context, generalMsg);
//                bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//            }
//            counter++;
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot_early", counter));
//                        } else {
//                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        }
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_early", counter));
//                        } else {
//                            marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "", counter));
//                        }
//                    }
//                    if (this.taskId == 0) {
//                        if (Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                            this.loggingManager.spoutRegister(counter, "");
//                        } else {
//                            LOG.info("Recovery epoch: " + counter);
//                        }
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                    counter = counter + this.db.getLoggingManager().inspectAbortNumber(counter + punctuation_interval, this.taskId);
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }

    private void nextTuple_Dependency() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
//        if (counter == start_measure) {
//            if (taskId == 0) {
//                sink.start();
//            }
//            this.systemStartTime = System.nanoTime();
//            sink.previous_measure_time = System.nanoTime();
//            if (isRecovery) {
//                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
//                long lastSnapshotId = recoverData();
//                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
//                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
//                } else {
//                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
//                    this.sink.stopRecovery = true;
//                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
//                    isRecovery = false;
//                    if (snapshot(counter) && counter != lastSnapshotId) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                        bolt.execute(marker);
//                        if (counter != the_end)
//                            input_store(counter);
//                    }
//                    if (counter == the_end) {
//                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                        if (taskId == 0)
//                            sink.end(global_cnt);
//                    }
//                }
//            } else {
//                input_store(counter);
//            }
//        }
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//            } else {
//                event = myevents[counter];
//            }
//            long bid = mybids[counter];
//            if (CONTROL.enable_latency_measurement) {
//                long time;
//                if (arrivalControl) {
//                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                } else {
//                    time = System.nanoTime();
//                }
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//            } else {
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//            }
//
//            tuple = new Tuple(bid, this.taskId, context, generalMsg);
//            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//            counter++;
//
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
//                    }
//                    if (this.taskId == 0) {
//                        this.loggingManager.spoutRegister(counter, "");
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }

    private void nextTuple_LV() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
//        if (counter == start_measure) {
//            if (taskId == 0) {
//                sink.start();
//            }
//            this.systemStartTime = System.nanoTime();
//            sink.previous_measure_time = System.nanoTime();
//            if (isRecovery) {
//                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
//                long lastSnapshotId = recoverData();
//                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
//                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
//                } else {
//                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
//                    this.sink.stopRecovery = true;
//                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
//                    isRecovery = false;
//                    if (snapshot(counter) && counter != lastSnapshotId) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                        bolt.execute(marker);
//                        if (counter != the_end)
//                            input_store(counter);
//                    }
//                    if (counter == the_end) {
//                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                        if (taskId == 0)
//                            sink.end(global_cnt);
//                    }
//                }
//            } else {
//                input_store(counter);
//            }
//        }
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//            } else {
//                event = myevents[counter];
//            }
//            long bid = mybids[counter];
//            if (CONTROL.enable_latency_measurement) {
//                long time;
//                if (arrivalControl) {
//                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                } else {
//                    time = System.nanoTime();
//                }
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//            } else {
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//            }
//
//            tuple = new Tuple(bid, this.taskId, context, generalMsg);
//            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//            counter++;
//
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
//                    }
//                    if (this.taskId == 0) {
//                        this.loggingManager.spoutRegister(counter, "");
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }

    private void nextTuple_Command() throws InterruptedException, IOException, ExecutionException, BrokenBarrierException, DatabaseException {
//        if (counter == start_measure) {
//            if (taskId == 0) {
//                sink.start();
//            }
//            this.systemStartTime = System.nanoTime();
//            sink.previous_measure_time = System.nanoTime();
//            if (isRecovery) {
//                MeasureTools.BEGIN_RECOVERY_TIME_MEASURE(this.taskId);
//                long lastSnapshotId = recoverData();
//                if (counter < num_events_per_thread && mybids[counter] < this.sink.lastTask) {
//                    MeasureTools.BEGIN_REPLAY_MEASURE(this.taskId);
//                } else {
//                    MeasureTools.END_RECOVERY_TIME_MEASURE(this.taskId);
//                    this.sink.stopRecovery = true;
//                    Metrics.RecoveryPerformance.stopRecovery[this.taskId] = true;
//                    isRecovery = false;
//                    if (snapshot(counter) && counter != lastSnapshotId) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                        bolt.execute(marker);
//                        if (counter != the_end)
//                            input_store(counter);
//                    }
//                    if (counter == the_end) {
//                        SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                        SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                        if (taskId == 0)
//                            sink.end(global_cnt);
//                    }
//                }
//            } else {
//                input_store(counter);
//            }
//        }
//        if (counter < num_events_per_thread) {
//            Object event;
//            if (recoveryInput.size() != 0) {
//                event = recoveryInput.poll();
//            } else {
//                event = myevents[counter];
//            }
//            long bid = mybids[counter];
//            if (CONTROL.enable_latency_measurement) {
//                long time;
//                if (arrivalControl) {
//                    time = this.systemStartTime + ((TxnEvent) event).getTimestamp() - remainTime;
//                } else {
//                    time = System.nanoTime();
//                }
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event, time);
//            } else {
//                generalMsg = new GeneralMsg(DEFAULT_STREAM_ID, event);
//            }
//
//            tuple = new Tuple(bid, this.taskId, context, generalMsg);
//            bolt.execute(tuple);  // public Tuple(long bid, int sourceId, TopologyContext context, Message message)
//            counter++;
//
//            if (ccOption == CCOption_MorphStream || ccOption == CCOption_SStore) {// This is only required by T-Stream.
//                if (model_switch(counter)) {
//                    if (snapshot(counter)) {
//                        inputStoreCurrentPath = inputStoreRootPath + OsUtils.OS_wrapper(Integer.toString(counter));
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit_snapshot", counter));
//                        if (this.taskId == 0) {
//                            this.ftManager.spoutRegister(counter, inputStoreCurrentPath);
//                        }
//                    } else {
//                        marker = new Tuple(bid, this.taskId, context, new Marker(DEFAULT_STREAM_ID, -1, bid, myiteration, "commit", counter));
//                    }
//                    if (this.taskId == 0) {
//                        this.loggingManager.spoutRegister(counter, "");
//                    }
//                    bolt.execute(marker);
//                    if (counter != the_end && Metrics.RecoveryPerformance.stopRecovery[this.taskId]) {
//                        input_store(counter);
//                    }
//                }
//            }
//            if (counter == the_end) {
//                SOURCE_CONTROL.getInstance().oneThreadCompleted(taskId); // deregister all barriers
//                SOURCE_CONTROL.getInstance().finalBarrier(taskId);//sync for all threads to come to this line.
//                if (taskId == 0)
//                    sink.end(global_cnt);
//            }
//        }
    }


}
