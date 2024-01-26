package intellistream.morphstream.engine.txn.transaction.context;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.List;

public class FunctionContext {
    public final int thread_Id;
    @Getter
    private final long BID;
    @Getter @Setter
    private HashMap<String, List<String>> transactionCombo = new HashMap<>();
    public FunctionContext(int thread_Id, long bid) {
        this.thread_Id = thread_Id;
        this.BID = bid;
    }
}
