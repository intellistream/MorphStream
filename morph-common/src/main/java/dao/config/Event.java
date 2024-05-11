package dao.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {
    String eventTypes;
    String tableNameForEvents;
    String keyNumberForEvents;
    String valueForEvents;
    String eventRatio;
    String ratioMultiPartitionTransactionsForEvents;
    String stateAccessSkewnessForEvents;
    String abortRatioForEvents;
}
