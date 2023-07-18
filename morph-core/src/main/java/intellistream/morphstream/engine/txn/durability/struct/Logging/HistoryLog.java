package intellistream.morphstream.engine.txn.durability.struct.Logging;

import intellistream.morphstream.engine.txn.scheduler.struct.MetaTypes;

public class HistoryLog implements LoggingEntry {
    public int id;
    public String table;
    public String from;
    public String to;
    public long bid;
    public Object value;

    public HistoryLog(int taskId, String table, String from, String to, long bid, Object value) {
        this.id = taskId;
        this.table = table;
        this.from = from;
        this.to = to;
        this.bid = bid;
        this.value = value;
    }

    @Override
    public void setVote(MetaTypes.OperationStateType vote) {

    }
}
