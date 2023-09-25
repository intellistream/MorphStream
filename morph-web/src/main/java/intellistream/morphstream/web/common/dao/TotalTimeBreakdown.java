package intellistream.morphstream.web.common.dao;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class TotalTimeBreakdown {
    private Double totalTime;
    private Double serializeTime;
    private Double persistTime;
    private Double streamProcessTime;
    private Double overheadTime;
    private Double txnProcessingTime;

    public TotalTimeBreakdown(Double totalTime, Double serializeTime, Double persistTime, Double streamProcessTime, Double overheadTime, Double txnProcessingTime) {
        this.totalTime = totalTime;
        this.serializeTime = serializeTime;
        this.persistTime = persistTime;
        this.streamProcessTime = streamProcessTime;
        this.overheadTime = overheadTime;
        this.txnProcessingTime = txnProcessingTime;
    }

    public TotalTimeBreakdown() {}
}
