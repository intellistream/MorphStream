package intellistream.morphstream.engine.txn.scheduler.struct.recovery;

import intellistream.morphstream.engine.txn.transaction.impl.ordered.MyList;

public class OperationChain implements Comparable<OperationChain> {
    public final MyList<Operation> operations;
    private final String tableName;
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

    public String getTableName() {
        return tableName;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void addOperation(Operation operation) {
        operations.add(operation);
    }

    public boolean isFinished() {
        assert level <= operations.size();
        return level == operations.size();
    }
}
