package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
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

    public Job(int JobId, String name) {
        this.jobId = JobId;
        this.name = name;
        this.nThreads = 0;
        this.cpu = "NA";
        this.ram = "NA";
        this.startTime = "NA";
        this.duration = "NA";
        this.isRunning = false;
        this.nEvents = 0;
        this.minProcessTime = 0;
        this.maxProcessTime = 0;
        this.meanProcessTime = 0;
        this.latency = 0;
        this.throughput = 0;
        this.nCore = 0;
        this.periodicalThroughput = new ArrayList<>();
        this.periodicalLatency = new ArrayList<>();
        this.operators = new ArrayList<>();
        this.overallTimeBreakdown = new OverallTimeBreakdown(0, 0, 0, 0);
        this.schedulerTimeBreakdown = new SchedulerTimeBreakdown(0, 0, 0, 0, 0);
    }
}
