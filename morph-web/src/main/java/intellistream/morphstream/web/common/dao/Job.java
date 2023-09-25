package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {
    private Integer appId;
    private String name;
    private Integer nthreads;
    private String CPU;
    private String RAM;
    private String startTime;   // TODO: Consider change to LocalDataTime
    private String Duration;    // TODO: Consider change to Duration
    private Boolean isRunning;
    private Integer nEvents;
    private Double minProcessTime;  // optional
    private Double maxProcessTime;  // optional
    private Double meanProcessTime; // optional
    private Double latency;
    private Double throughput;
    private Integer ncore;
    private Operator[] operators;
    private TotalTimeBreakdown totalTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
}
