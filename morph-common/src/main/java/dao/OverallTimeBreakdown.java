package dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
@NoArgsConstructor
public class OverallTimeBreakdown {
    private long totalTime;
    private long streamTime;
    private long txnTime;
    private long overheadTime;

    public OverallTimeBreakdown(long totalTime, long streamTime, long txnTime, long overheadTime) {
        this.totalTime = totalTime;
        this.streamTime = streamTime;
        this.txnTime = txnTime;
        this.overheadTime = overheadTime;
    }
}
