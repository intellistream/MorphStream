package communication.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class Batch {
    private int batchId;
    private String jobId;
    private String operatorID;
    private double throughput;
    private double minLatency;
    private double maxLatency;
    private double avgLatency;
    private long batchSize;
    private long batchDuration;
    private int latestBatchId;
    private double accumulativeLatency;
    private double accumulativeThroughput;
    private OverallTimeBreakdown overallTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private String scheduler;
    private List<TPGNode> tpg;
    public Batch(String jobId, String operatorID, double throughput,
                 double minLatency, double maxLatency, double avgLatency, long batchSize, long batchDuration,
                 double accumulativeLatency, double accumulativeThroughput,
                 OverallTimeBreakdown overallTimeBreakdown, SchedulerTimeBreakdown schedulerTimeBreakdown,
                 String scheduler, ConcurrentHashMap<TPGNode, List<TPGEdge>> tpg, int latestBatchId) {
        this.batchId = latestBatchId;
        this.jobId = jobId;
        this.operatorID = operatorID;
        this.throughput = throughput;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.avgLatency = avgLatency;
        this.batchSize = batchSize;
        this.batchDuration = batchDuration;
        this.accumulativeLatency = accumulativeLatency;
        this.accumulativeThroughput = accumulativeThroughput;
        this.overallTimeBreakdown = overallTimeBreakdown;
        this.schedulerTimeBreakdown = schedulerTimeBreakdown;
        this.scheduler = scheduler;
        this.tpg = new ArrayList<>();
        this.latestBatchId = latestBatchId;
        tpg.forEach((node, edges) -> {
            node.setEdges(edges);
            this.tpg.add(node);
        });
    }
}
