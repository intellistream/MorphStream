package intellistream.morphstream.web.common.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import intellistream.morphstream.web.common.dao.Operator;
import intellistream.morphstream.web.common.dao.SchedulerTimeBreakdown;
import intellistream.morphstream.web.common.dao.OverallTimeBreakdown;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

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
    private OverallTimeBreakdown overallTimeBreakdown;
    private SchedulerTimeBreakdown schedulerTimeBreakdown;
    private List<Double> periodicalThroughput;
    private List<Double> periodicalLatency;
}
