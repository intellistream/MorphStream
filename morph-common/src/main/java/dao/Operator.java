package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class Operator {
    private String id;
    private String name;
    private int numOfInstances;
    private double throughput;
    private double latency;
    private String explorationStrategy;
    private String schedulingGranularity;
    private String abortHandling;
}
