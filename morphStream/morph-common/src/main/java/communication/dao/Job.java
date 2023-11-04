package communication.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {
    private Integer jobId;
    private String name;
    private Integer nthreads;
    private String CPU;
    private String RAM;
    private String startTime;
    private String Duration;
    private Boolean isRunning;
    private Integer nEvents;
    private Double minProcessTime;
    private Double maxProcessTime;
    private Double meanProcessTime;
    private Double latency;
    private Double throughput;
    private Integer ncore;
    private Operator[] operators;
    private OverallTimeBreakdown overallTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
}
