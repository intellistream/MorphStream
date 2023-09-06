package object.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import object.DAO.Operator;
import object.DAO.SchedulerTimeBreakdown;
import object.DAO.TotalTimeBreakdown;

import java.util.List;


@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DetailedInfoResponse extends AbstractResponse {
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
    private TotalTimeBreakdown totalTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
}
