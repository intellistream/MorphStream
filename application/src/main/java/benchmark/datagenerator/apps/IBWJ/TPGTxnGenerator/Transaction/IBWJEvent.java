package benchmark.datagenerator.apps.IBWJ.TPGTxnGenerator.Transaction;

import benchmark.datagenerator.Event;

/**
 * Streamledger related transaction data
 */
public class IBWJEvent extends Event {
    private final int id;
    private final int key;
    private final String streamID;
    private final String address;

    public IBWJEvent(int id, int key, String streamID, String address) {
        this.id = id;
        this.key = key;
        this.streamID = streamID;
        this.address = address;
    }

    public int getKey() {
        return key;
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
        str.append(",").append(key);
        str.append(",").append(streamID);
        str.append(",").append(address);
        return str.toString();
    }

}
