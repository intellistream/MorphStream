package engine.txn.durability.recovery;

import engine.txn.durability.struct.Result.persistResult;

import java.util.ArrayList;
import java.util.List;

public class RedoLogResult implements persistResult {
    public int threadId;
    public List<String> redoLogPaths = new ArrayList<>();
    public List<Long> groupIds = new ArrayList<>();
    public long lastedGroupId;
    public void addPath(String path, long groupId) {
        this.redoLogPaths.add(path);
        this.groupIds.add(groupId);
    }

    public void setLastedGroupId(long lastedGroupId) {
        this.lastedGroupId = lastedGroupId;
    }
}
