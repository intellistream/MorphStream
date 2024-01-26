package intellistream.morphstream.engine.txn.scheduler.struct.recovery;

import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;
import lombok.Getter;

public class OperationChain implements Comparable<OperationChain> {
    public final MyList<Operation> operations;
    @Getter
    private final String tableName;
    @Getter
    private final String primaryKey;
    public int level = 0;

    public OperationChain(String tableName, String primaryKey) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.operations = new MyList<>(tableName, primaryKey);
    }

    @Override
    public int compareTo(OperationChain o) {
        if (o.toString().equals(toString()))
            return 0;
        else
            return -1;
    }

    public void addOperation(Operation operation) {
        operations.add(operation);
    }

    public boolean isFinished() {
        assert level <= operations.size();
        return level == operations.size();
    }
}
