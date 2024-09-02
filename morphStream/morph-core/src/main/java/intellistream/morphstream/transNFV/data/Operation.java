package intellistream.morphstream.transNFV.data;

public class Operation {
    private final String key;
    private final boolean isWrite;

    public Operation(String key, boolean isWrite) {
        this.key = key;
        this.isWrite = isWrite;
    }

    public String getKey() {
        return key;
    }

    public boolean isWrite() {
        return isWrite;
    }
}
