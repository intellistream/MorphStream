package benchmark.datagenerator.apps.SHJ.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class SHJEvent extends Event {
    private final int id;
    private final int rKey;
    private final String streamID;
    private final String address;
    private final int[] sKeys;

    public SHJEvent(int id, int rKey, String streamID, String address, int[] sKeys) {
        this.id = id;
        this.rKey = rKey;
        this.streamID = streamID;
        this.address = address;
        this.sKeys = sKeys;
    }

    public int getrKey() {
        return rKey;
    }

    public String getStreamID() {
        return streamID;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder(String.valueOf(id));
        str.append(",").append(rKey);
        str.append(",").append(streamID);
        str.append(",").append(address);
        for (int sKey : sKeys) {
            str.append(",").append(sKey);
        }
        return str.toString();
    }

}