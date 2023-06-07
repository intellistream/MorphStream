package benchmark.datagenerator.apps.LB.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class LBEvent extends Event {
    private final int id;
    private final int connID;
    private final String srcAddr;
    private final String srcPort;
    private String destAddr;
    private String desrPort;

    public LBEvent(int id, int connID, String srcAddr, String srcPort, String destAddr, String destPort) {
        this.id = id;
        this.connID = connID;
        this.srcAddr = srcAddr;
        this.srcPort = srcPort;
        this.destAddr = destAddr;
        this.desrPort = destPort;
    }

    public int getKey() {
        return connID;
    }

    public void setDestAddr(String newDestAddr) {
        this.destAddr = newDestAddr;
    }

    public void setDestPort(String newDestPort) {
        this.desrPort = newDestPort;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        str.append(",").append(connID);
        str.append(",").append(srcAddr);
        str.append(",").append(srcPort);
        str.append(",").append(destAddr);
        str.append(",").append(desrPort);
        return str.toString();
    }

}
