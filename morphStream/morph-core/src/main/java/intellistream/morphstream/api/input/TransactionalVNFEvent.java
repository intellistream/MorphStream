package intellistream.morphstream.api.input;

/**
 * This class is used in refined-API, to convert raw inputEvent to txnEvent
 * Differs from the original TxnEvent, this class only requires user to specify (keys, values, flags)
 * Currently, it inherits from TxnEvent to match the old TxnEvent logic (which involves partition information)
 */

public class TransactionalVNFEvent extends TransactionalEvent {
    private String tupleID;
    private long txnRequestID; //Unique ID for each txn request, encoded by VNF instance
    private int txnID; //E.g., "txn1", txnID
    private String saID; //E.g., "Deposit" or "Transfer", txnFlag
    private int saType; // 0: Read, 1: Write, 2: RW
    private boolean isAbort;
    private int instanceID;
    private int puncID;

    public TransactionalVNFEvent(int saType, int instanceID, long timestamp, long txnRequestID, int tupleID, int txnID, int saID, int isAbort, int puncID) {
        super(timestamp);
        this.saType = saType;
        this.instanceID = instanceID;
        this.txnRequestID = txnRequestID;
        this.tupleID = String.valueOf(tupleID);
        this.txnID = txnID;
        this.saID = String.valueOf(saID);
        this.isAbort = (isAbort == 1);
        this.puncID = puncID;
    }

    public TransactionalVNFEvent(long bid) {
        super(bid);
    }
    public int getInstanceID() {
        return this.instanceID;
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public void setFlag(String flag) {
        this.saID = flag;
    }

    public String getTupleID() {
        return this.tupleID;
    }
    @Override
    public String getKey(int keyIndex) {
        return this.tupleID;
    }
    @Override
    public long getTxnRequestID() {
        return this.txnRequestID;
    }

    public String getFlag() {
        return this.saID;
    }

    public boolean isAbort() {
        return isAbort;
    }
    public void setSaType(int saType) {
        this.saType = saType;
    }
    public int getSaType() {
        return this.saType;
    }
    public int getPuncID() {
        return this.puncID;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(tupleID).append(":");
        stringBuilder.deleteCharAt(stringBuilder.length() -1);
        stringBuilder.append(";");
        stringBuilder.append(saID);
        stringBuilder.append(";");
        stringBuilder.append(isAbort);
        return stringBuilder.toString();
    }
}
