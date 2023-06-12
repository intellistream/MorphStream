package benchmark.datagenerator.apps.LB.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class LBEvent extends Event {
    private final int id;
    private final int[] keys;
    private final boolean isNewConn;
    private final int connID;

    public LBEvent(int id, int[] keys, boolean isNewConn, int connID) {
        this.id = id;
        this.keys = keys;
        this.isNewConn = isNewConn;
        this.connID = connID;
    }

    public int getKey() {
        return connID;
    }


    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        for (int key : keys) {
            str.append(",").append(key);
        }
        str.append(",").append(isNewConn);
        str.append(",").append(connID);
        return str.toString();
    }

}
