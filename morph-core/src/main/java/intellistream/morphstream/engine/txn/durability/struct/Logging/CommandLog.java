package intellistream.morphstream.engine.txn.durability.struct.Logging;

public abstract class CommandLog implements LoggingEntry, Comparable<CommandLog> {
    public long LSN;
    public String tableName;
    public String key;
    public String[] condition;

    public CommandLog(long LSN, String tableName, String key, String[] condition) {
        this.LSN = LSN;
        this.tableName = tableName;
        this.key = key;
        this.condition = condition;
    }

    @Override
    public int compareTo(CommandLog o) {
        return Long.compare(this.LSN, o.LSN);
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
