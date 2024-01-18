package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class TPGEdge {
    private String srcOperatorID;
    private String dstOperatorID;
    private String dependencyType;

    public TPGEdge(String srcOperatorID, String dstOperatorID, String dependencyType) {
        this.srcOperatorID = srcOperatorID;
        this.dstOperatorID = dstOperatorID;
        this.dependencyType = dependencyType;
    }
}
