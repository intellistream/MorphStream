package dao.config;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;

/**
 * The configuration of a job
 */
@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@AllArgsConstructor
@NoArgsConstructor
public class JobConfiguration {
    private String name;
    private List<Table> tables;
    private Event event;
    private int totalEvents;
    private int checkpointInterval;
    private int parallelism;
    private List<OperatorDescription> operatorDescription;
}
