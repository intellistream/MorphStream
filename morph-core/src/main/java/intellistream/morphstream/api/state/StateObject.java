package intellistream.morphstream.api.state;


/**
 * This class might be used as a data carrier for SchemaRecord, it contains all data fields of SchemaRecord and allow client to retrieve in an abstract way.
 * Client does not need to remember the indexes of each field, instead just need to specify field name to retrieve state data.
 */
public class StateObject {
    private final String tableName;
    private final String keyName;

    public StateObject(String tableName, String keyName) {
        this.tableName = tableName;
        this.keyName = keyName;
    }
}
