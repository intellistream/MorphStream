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
        public static long[] executeStartTime = new long[kMaxThreadNum];
        public static long[] executeEndTime = new long[kMaxThreadNum];
        public static DescriptiveStatistics[] executeTimeStatistics = new DescriptiveStatistics[kMaxThreadNum];
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
                executeStartTime[i] = 0;
                executeEndTime[i] = 0;
                executeTimeStatistics[i] = new DescriptiveStatistics();
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

}
