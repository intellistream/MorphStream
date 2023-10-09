package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchRuntimeData {
    private String appId;
    private String operatorID;
    private double throughput;
    private double minLatency;
    private double maxLatency;
    private double avgLatency;
    private long batchSize;
//    private TotalTimeBreakdown totalTimeBreakdown;
//    private SchedulerTimeBreakdown schedulerTimeBreakdown;
//    private TPG tpg;
    public BatchRuntimeData(String appId, String operatorID, double throughput, double minLatency, double maxLatency, double avgLatency, long batchSize) {
        this.appId = appId;
        this.operatorID = operatorID;
        this.throughput = throughput;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.avgLatency = avgLatency;
        this.batchSize = batchSize;
    }

}