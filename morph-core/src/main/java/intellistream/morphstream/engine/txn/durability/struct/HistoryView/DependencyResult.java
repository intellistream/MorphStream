package intellistream.morphstream.engine.txn.durability.struct.HistoryView;

import java.io.Serializable;

public class DependencyResult implements Serializable {
    public long bid;
    public Object value;

    public DependencyResult(long bid, Object value) {
        this.bid = bid;
        this.value = value;
    }

    public String toString() {
        return bid + "/" + value.toString();
    }
}
