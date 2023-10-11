package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TPGEdge {
    private final String srcOperatorID;
    private final String dstOperatorID;
    private final String dependencyType;

    public TPGEdge(String srcOperatorID, String dstOperatorID, String dependencyType) {
        this.srcOperatorID = srcOperatorID;
        this.dstOperatorID = dstOperatorID;
        this.dependencyType = dependencyType;
    }
}
