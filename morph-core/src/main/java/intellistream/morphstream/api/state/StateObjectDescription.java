package intellistream.morphstream.api.state;


import intellistream.morphstream.api.utils.MetaTypes.AccessType;
import lombok.Getter;

import java.io.Serializable;

/**
 * Used as a data carrier for SchemaRecord, provides simplified retrieval-by-columnName APIs.
 */
@Getter
public class StateObjectDescription implements Serializable {
    private final String name;
    private final AccessType type;
    private final int keyIndex;
    private final String tableName;
    private final String keyName;

    public StateObjectDescription(String name, AccessType type, String tableName, String keyName, int keyIndex) {
        this.name = name;
        this.tableName = tableName;
        this.keyName = keyName;
        this.type = type;
        this.keyIndex = keyIndex;
    }

}
