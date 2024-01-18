package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
@AllArgsConstructor
public class SchedulerTimeBreakdown {
    private double exploreTime;
    private double usefulTime;
    private double abortTime;
    private double constructTime;
    private double trackingTime;
}
