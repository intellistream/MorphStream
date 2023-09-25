package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SchedulerTimeBreakdown {
    private Double exploreTime;
    private Double usefulTime;
    private Double abortTime;
    private Double constructTime;
    private Double trackingTime;

    public SchedulerTimeBreakdown (Double exploreTime, Double usefulTime, Double abortTime, Double constructTime, Double trackingTime) {
        this.exploreTime = exploreTime;
        this.usefulTime = usefulTime;
        this.abortTime = abortTime;
        this.constructTime = constructTime;
        this.trackingTime = trackingTime;
    }

    public SchedulerTimeBreakdown () {}
}