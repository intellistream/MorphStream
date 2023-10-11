package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TPGNode {
    private String operationID;
    private String txnType;
    private String targetTable;
    private String targetKey;

    public TPGNode(String operationID, String txnType, String targetTable, String targetKey) {
        this.operationID = operationID;
        this.txnType = txnType;
        this.targetTable = targetTable;
        this.targetKey = targetKey;
    }
}
