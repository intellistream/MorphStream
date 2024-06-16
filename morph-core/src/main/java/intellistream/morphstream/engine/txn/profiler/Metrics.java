package intellistream.morphstream.engine.txn.profiler;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class Metrics {
    private static final int kMaxThreadNum = 64;
    static class DriverRuntime {
        public static long[] prepareStartTime = new long[kMaxThreadNum];
        public static long[] prepareEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] prepareTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rdmaStartSendEventTime = new long[kMaxThreadNum];
        public static long[] rdmaEndSendEventTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaSendTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rdmaStartRecvEventTime = new long[kMaxThreadNum];
        public static long[] rdmaEndRecvEventTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRecvTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] finishStartTime = new long[kMaxThreadNum];
        public static long[] finishEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] finishTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                prepareStartTime[i] = 0;
                prepareEndTime[i] = 0;
                prepareTimeStatistics[i] = new DescriptiveStatistics();
                rdmaStartSendEventTime[i] = 0;
                rdmaEndSendEventTime[i] = 0;
                rdmaSendTimeStatistics[i] = new DescriptiveStatistics();
                rdmaStartRecvEventTime[i] = 0;
                rdmaEndRecvEventTime[i] = 0;
                rdmaRecvTimeStatistics[i] = new DescriptiveStatistics();
                finishStartTime[i] = 0;
                finishEndTime[i] = 0;
                finishTimeStatistics[i] = new DescriptiveStatistics();
            }
        }

    }
    static class WorkerRuntime {
        public static long[] rdmaRecvStartEventTime = new long[kMaxThreadNum];
        public static long[] rdmaRecvEndEventTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRecvTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] prepareStartTime = new long[kMaxThreadNum];
        public static long[] prepareEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] prepareTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] finishStartTime = new long[kMaxThreadNum];
        public static long[] finishEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] finishTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rdmaSendResultStartTime = new long[kMaxThreadNum];
        public static long[] rdmaSendResultEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaSendResultTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRounds = new DescriptiveStatistics[kMaxThreadNum];
        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                rdmaRecvStartEventTime[i] = 0;
                rdmaRecvEndEventTime[i] = 0;
                rdmaRecvTimeStatistics[i] = new DescriptiveStatistics();
                prepareStartTime[i] = 0;
                prepareEndTime[i] = 0;
                prepareTimeStatistics[i] = new DescriptiveStatistics();
                finishStartTime[i] = 0;
                finishEndTime[i] = 0;
                finishTimeStatistics[i] = new DescriptiveStatistics();
                rdmaSendResultStartTime[i] = 0;
                rdmaSendResultEndTime[i] = 0;
                rdmaSendResultTimeStatistics[i] = new DescriptiveStatistics();
                rdmaRounds[i] = new DescriptiveStatistics();
            }
        }
    }
    static class DSRuntime {
        public static long[] rdmaRecvStartOwnershipTableTime = new long[kMaxThreadNum];
        public static long[] rdmaRecvEndOwnershipTableTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRecvOwnershipTableTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] prepareCacheStartTime = new long[kMaxThreadNum];
        public static long[] prepareCacheEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] prepareCacheTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rdmaStartRemoteOperationTime = new long[kMaxThreadNum];
        public static long[] rdmaEndRemoteOperationTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRemoteOperationTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] setupDependenciesStartTime = new long[kMaxThreadNum];
        public static long[] setupDependenciesEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] setupDependenciesTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] exploreStartTime = new long[kMaxThreadNum];
        public static long[] exploreEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] exploreTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] executeStartTime = new long[kMaxThreadNum];
        public static long[] executeEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] executeTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rdmaAccessStartEventTime = new long[kMaxThreadNum];
        public static long[] rdmaAccessEndEventTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaAccessTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] commitStartTime = new long[kMaxThreadNum];
        public static long[] commitEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] commitTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static DescriptiveStatistics[] rdmaRounds = new DescriptiveStatistics[kMaxThreadNum];
        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                rdmaRecvStartOwnershipTableTime[i] = 0;
                rdmaRecvEndOwnershipTableTime[i] = 0;
                rdmaRecvOwnershipTableTimeStatistics[i] = new DescriptiveStatistics();
                prepareCacheStartTime[i] = 0;
                prepareCacheEndTime[i] = 0;
                prepareCacheTimeStatistics[i] = new DescriptiveStatistics();
                rdmaStartRemoteOperationTime[i] = 0;
                rdmaEndRemoteOperationTime[i] = 0;
                rdmaRemoteOperationTimeStatistics[i] = new DescriptiveStatistics();
                setupDependenciesStartTime[i] = 0;
                setupDependenciesEndTime[i] = 0;
                setupDependenciesTimeStatistics[i] = new DescriptiveStatistics();
                exploreStartTime[i] = 0;
                exploreEndTime[i] = 0;
                exploreTimeStatistics[i] = new DescriptiveStatistics();
                executeStartTime[i] = 0;
                executeEndTime[i] = 0;
                executeTimeStatistics[i] = new DescriptiveStatistics();
                rdmaAccessStartEventTime[i] = 0;
                rdmaAccessEndEventTime[i] = 0;
                rdmaAccessTimeStatistics[i] = new DescriptiveStatistics();
                rdmaRounds[i] = new DescriptiveStatistics();
                commitStartTime[i] = 0;
                commitEndTime[i] = 0;
                commitTimeStatistics[i] = new DescriptiveStatistics();
            }
        }
    }
    static class RemoteLockRuntime{
        public static long[] lockStartTime = new long[kMaxThreadNum];
        public static long[] lockEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] lockTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] rmaAndExecutionStartTime = new long[kMaxThreadNum];
        public static long[] rmaAndExecutionEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rmaAndExecutionTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] executeStartTime = new long[kMaxThreadNum];
        public static long[] executeEndTime = new long[kMaxThreadNum];
        public static long[] executeACCTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] executeTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] commitStartTime = new long[kMaxThreadNum];
        public static long[] commitEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] commitTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] unlockStartTime = new long[kMaxThreadNum];
        public static long[] unlockEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] unlockTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                lockStartTime[i] = 0;
                lockEndTime[i] = 0;
                lockTimeStatistics[i] = new DescriptiveStatistics();
                rmaAndExecutionStartTime[i] = 0;
                rmaAndExecutionEndTime[i] = 0;
                rmaAndExecutionTimeStatistics[i] = new DescriptiveStatistics();
                executeStartTime[i] = 0;
                executeEndTime[i] = 0;
                executeACCTime[i] = 0;
                executeTimeStatistics[i] = new DescriptiveStatistics();
                commitStartTime[i] = 0;
                commitEndTime[i] = 0;
                commitTimeStatistics[i] = new DescriptiveStatistics();
                unlockStartTime[i] = 0;
                unlockEndTime[i] = 0;
                unlockTimeStatistics[i] = new DescriptiveStatistics();
            }
        }
    }
    static class RemoteOCCRuntime {
        public static long[] rmaAndExecutionStartTime = new long[kMaxThreadNum];
        public static long[] rmaAndExecutionEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] rmaAndExecutionTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] executeStartTime = new long[kMaxThreadNum];
        public static long[] executeEndTime = new long[kMaxThreadNum];
        public static long[] executionACCTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] executeTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] validateStartTime = new long[kMaxThreadNum];
        public static long[] validateEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] validateTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] redoStartTime = new long[kMaxThreadNum];
        public static long[] redoEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] redoTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] commitStartTime = new long[kMaxThreadNum];
        public static long[] commitEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] commitTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static long[] unlockStartTime = new long[kMaxThreadNum];
        public static long[] unlockEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] unlockTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
        public static void Initialize() {
            for (int i = 0; i < kMaxThreadNum; i++) {
                rmaAndExecutionStartTime[i] = 0;
                rmaAndExecutionEndTime[i] = 0;
                rmaAndExecutionTimeStatistics[i] = new DescriptiveStatistics();
                executeStartTime[i] = 0;
                executeEndTime[i] = 0;
                executionACCTime[i] = 0;
                executeTimeStatistics[i] = new DescriptiveStatistics();
                validateStartTime[i] = 0;
                validateEndTime[i] = 0;
                validateTimeStatistics[i] = new DescriptiveStatistics();
                redoStartTime[i] = 0;
                redoEndTime[i] = 0;
                redoTimeStatistics[i] = new DescriptiveStatistics();
                commitStartTime[i] = 0;
                commitEndTime[i] = 0;
                commitTimeStatistics[i] = new DescriptiveStatistics();
                unlockStartTime[i] = 0;
                unlockEndTime[i] = 0;
                unlockTimeStatistics[i] = new DescriptiveStatistics();
            }
        }
    }
}
