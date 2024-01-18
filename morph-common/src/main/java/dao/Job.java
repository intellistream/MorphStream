package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
public class Job {
    private int jobId;
    private String name;
    private int nThreads;
    private String cpu;
    private String ram;
    private String startTime;
    private String duration;
    private Boolean isRunning;
    private int nEvents;
    private double minProcessTime;
    private double maxProcessTime;
    private double meanProcessTime;
    private double latency;
    private double throughput;
    private int nCore;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
    private List<Operator> operators;
    private OverallTimeBreakdown overallTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
}
