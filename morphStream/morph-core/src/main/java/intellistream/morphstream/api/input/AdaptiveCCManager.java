package intellistream.morphstream.api.input;

public class AdaptiveCCManager {

    CacheCCManager cacheCCManager;
    PartitionCCManager lockCCManager;
    OffloadCCManager offloadCCManager;
    PatternMonitor patternMonitor;

    public AdaptiveCCManager() {
        cacheCCManager = new CacheCCManager();
        lockCCManager = new PartitionCCManager();
        offloadCCManager = new OffloadCCManager();
        patternMonitor = new PatternMonitor();
    }

    public void initialize() {
        cacheCCManager.initialize();
        lockCCManager.initialize();
        offloadCCManager.initialize();
        patternMonitor.initialize();
    }

    //TODO: Optimization: Initialize CC123 managers as multi-threaded processes
}
