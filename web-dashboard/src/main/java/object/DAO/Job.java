package object.DAO;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

import java.util.List;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Job {
    private String appId;
    private String name;
    private String nthreads;
    private String CPU;
    private String RAM;
    private String startTime;   // TODO: consider change to LocalDataTime
    private String Duration;    // consider change to LocalDataTime
    private Boolean isRunning;
    private Integer nEvents;
    private Double minProcessTime;
    private Double maxProcessTime;
    private Double meanProcessTime;
    private Double latency;
    private Double throughput;
    private Integer ncore;
    private Operator[] operators;
    private TotalTimeBreakdown totalTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
}
