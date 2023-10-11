package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

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
    private long batchDuration;
    private OverallTimeBreakdown overallTimeBreakdown;
//    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private ConcurrentHashMap<TPGNode, List<TPGEdge>> tpg;
    public BatchRuntimeData(String appId, String operatorID, double throughput,
                            double minLatency, double maxLatency, double avgLatency, long batchSize, long batchDuration,
                            OverallTimeBreakdown overallTimeBreakdown, ConcurrentHashMap<TPGNode, List<TPGEdge>> tpg) {
        this.appId = appId;
        this.operatorID = operatorID;
        this.throughput = throughput;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.avgLatency = avgLatency;
        this.batchSize = batchSize;
        this.batchDuration = batchDuration;
        this.overallTimeBreakdown = overallTimeBreakdown;
        this.tpg = tpg;
    }
}
