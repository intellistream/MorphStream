package intellistream.morphstream.api.input;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 */

public class TransactionalVNFEvent extends TransactionalEvent {
    private String[] keys;
    private long txnRequestID; //Unique ID for each txn request, encoded by VNF instance
    private String flag; //E.g., "Deposit" or "Transfer"
    private boolean isAbort = false;

    public TransactionalVNFEvent(long bid, long txnRequestID, String[] keys, String flag, boolean isAbort) {
        super(bid);
        this.txnRequestID = txnRequestID;
        this.keys = keys;
        this.flag = flag;
        this.isAbort = isAbort;
    }

    public TransactionalVNFEvent(long bid) {
        super(bid);
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String[] getKeys() {
        return this.keys;
    }
    @Override
    public String getKey(int keyIndex) {
        return this.keys[keyIndex];
    }
    @Override
    public long getTxnRequestID() {
        return this.txnRequestID;
    }

    public String getFlag() {
        return this.flag;
    }

    public boolean isAbort() {
        return isAbort;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (String key : keys) {
            stringBuilder.append(key).append(":");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        stringBuilder.append(flag);
        stringBuilder.append(";");
        stringBuilder.append(isAbort);
        return stringBuilder.toString();
    }
}
