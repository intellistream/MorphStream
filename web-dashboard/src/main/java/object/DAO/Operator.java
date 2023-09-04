package object.DAO;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
public class Operator {
    private String id;
    private String name;
    private String numOfInstances;
    private Float throughput;
    private Float latency;
    private String explorationStrategy;
    private String schedulingGranularity;
    private String abortHandling;
}
