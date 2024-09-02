package intellistream.morphstream.transNFV.data;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Transaction {
    private final List<Operation> operations = new ArrayList<>();
    private final Set<String> acquiredLocks = new HashSet<>();
    private final long timestamp;

    public Transaction(long timestamp) {
        this.timestamp = timestamp;
    }

    public void addOperation(String key, boolean isWrite) {
        operations.add(new Operation(key, isWrite));
    }

    public List<Operation> getOperations() {
        return operations;
    }

    public Set<String> getAcquiredLocks() {
        return acquiredLocks;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
