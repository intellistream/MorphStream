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
//    private TotalTimeBreakdown totalTimeBreakdown;
//    private SchedulerTimeBreakdown schedulerTimeBreakdown;
//    private TPG tpg;


    public void setAppId(String appId) {
        this.appId = appId;
    }

    public void setOperatorID(String operatorID) {
        this.operatorID = operatorID;
    }

    public void setThroughput(double throughput) {
        this.throughput = throughput;
    }

    public void setMinLatency(double minLatency) {
        this.minLatency = minLatency;
    }

    public void setMaxLatency(double maxLatency) {
        this.maxLatency = maxLatency;
    }

    public void setAvgLatency(double avgLatency) {
        this.avgLatency = avgLatency;
    }

}