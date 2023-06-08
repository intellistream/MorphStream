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
    private final int[] lookupKeys;

    public IBWJEvent(int id, int key, String streamID, String address, int[] lookupKeys) {
        this.id = id;
        this.key = key;
        this.streamID = streamID;
        this.address = address;
        this.lookupKeys = lookupKeys;
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
        for (int lookupKey : lookupKeys) {
            str.append(",").append(lookupKey);
        }
        return str.toString();
    }

}
