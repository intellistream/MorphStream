package intellistream.morphstream.api.input.chc;

/**
 * Based on the access pattern, the database can use different strategies to update the data.
 */
public class Strategy {
    public enum StrategyType {
        PER_FLOW,
        CROSS_FLOW_HEAVY_READ,
        CROSS_FLOW_READ_AND_WRITE,
    }

    /**
     * Get the strategy type based on the access pattern.
     *
     * @param pattern the access pattern
     * @return the strategy type
     */
    public static StrategyType getStrategy(Pattern pattern) {
        if (pattern.getStateType() == Pattern.StateType.PER_FLOW) {
            // Per-flow
            return StrategyType.PER_FLOW;
        } else {
            // Cross-flow
            if (pattern.getAccessType() == Pattern.AccessType.READ) {
                return StrategyType.CROSS_FLOW_HEAVY_READ;
            } else {
                return StrategyType.CROSS_FLOW_READ_AND_WRITE;
            }
        }
    }
}
