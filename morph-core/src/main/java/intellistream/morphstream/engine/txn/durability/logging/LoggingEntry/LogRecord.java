package intellistream.morphstream.engine.txn.durability.logging.LoggingEntry;

import intellistream.morphstream.engine.txn.durability.struct.Logging.LoggingEntry;
import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;

import java.util.Objects;

public class LogRecord implements LoggingEntry, Comparable {
    public String tableName;
    public long bid;
    public String key;
    public Object update;
    public MetaTypes.OperationStateType vote;

    public LogRecord(String tableName, long bid, String key) {
        this.bid = bid;
        this.tableName = tableName;
        this.key = key;
    }

    public void addUpdate(Object update) {
        this.update = update;
    }

    public void setVote(MetaTypes.OperationStateType vote) {
        this.vote = vote;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogRecord entry = (LogRecord) o;

        if (bid != entry.bid) return false;
        if (!key.equals(((LogRecord) o).key)) return false;
        return Objects.equals(update, entry.update);
    }

    public String toString() {
        return bid + ";" + update.toString();
    }

    @Override
    public int compareTo(Object obj) {
        if (this.getClass() != obj.getClass()) {
            return 0;
        }
        LogRecord other = (LogRecord) obj;
        if (this.bid != other.bid) {
            return Long.compare(this.bid, other.bid);
        } else {
            return Integer.compare(Integer.parseInt(key), Integer.parseInt(other.key));
        }
    }
}
