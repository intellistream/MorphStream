package object.DAO;

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
    private Float minProcessTime;
    private Float maxProcessTime;
    private Float meanProcessTime;
    private Float latency;
    private Float throughput;
    private Integer ncore;

    private Operator[] operators;
}
