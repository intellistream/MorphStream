package cli;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class VNFJsonClass {
    private List<App> apps;

    public List<App> getApps() {
        return apps;
    }

    public void setApps(List<App> apps) {
        this.apps = apps;
    }
}

class App {
    private String name;
    private List<Transaction> transactions;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Transaction> getTransactions() {
        return transactions;
    }

    public void setTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }
}

class Transaction {
    @JsonProperty("StateAccesses")
    private List<StateAccess> stateAccesses;

    public List<StateAccess> getStateAccesses() {
        return stateAccesses;
    }

    public void setStateAccesses(List<StateAccess> stateAccesses) {
        this.stateAccesses = stateAccesses;
    }
}

class StateAccess {
    @JsonProperty("TableName")
    private String tableName;
    @JsonProperty("consistency_requirement")
    private String consistencyRequirement;
    @JsonProperty("fieldTableIndex")
    private int fieldTableIndex;
    @JsonProperty("keyIndexInEvent")
    private int keyIndexInEvent;
    @JsonProperty("max_entries")
    private int maxEntries;
    @JsonProperty("type")
    private String type;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getConsistencyRequirement() {
        return consistencyRequirement;
    }

    public void setConsistencyRequirement(String consistencyRequirement) {
        this.consistencyRequirement = consistencyRequirement;
    }

    public int getFieldTableIndex() {
        return fieldTableIndex;
    }

    public void setFieldTableIndex(int fieldTableIndex) {
        this.fieldTableIndex = fieldTableIndex;
    }

    public int getKeyIndexInEvent() {
        return keyIndexInEvent;
    }

    public void setKeyIndexInEvent(int keyIndexInEvent) {
        this.keyIndexInEvent = keyIndexInEvent;
    }

    public int getMaxEntries() {
        return maxEntries;
    }

    public void setMaxEntries(int maxEntries) {
        this.maxEntries = maxEntries;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
