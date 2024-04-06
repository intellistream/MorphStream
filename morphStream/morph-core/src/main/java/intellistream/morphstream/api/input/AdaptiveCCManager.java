package intellistream.morphstream.api.input;

import java.io.IOException;

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

    public void initialize() throws IOException {
        cacheCCManager.initialize();
        lockCCManager.initialize();
        offloadCCManager.initialize();
        patternMonitor.initialize();
    }

}
