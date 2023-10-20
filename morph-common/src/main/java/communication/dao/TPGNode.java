package communication.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class TPGNode {
    private String operationID;
    private String txnType;
    private String targetTable;
    private String targetKey;
    private List<TPGEdge> edges = new ArrayList<>();

    public TPGNode(String operationID, String txnType, String targetTable, String targetKey) {
        this.operationID = operationID;
        this.txnType = txnType;
        this.targetTable = targetTable;
        this.targetKey = targetKey;
    }
}
