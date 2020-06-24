package sesame.components.grouping;
import sesame.execution.runtime.tuple.impl.Fields;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public class MarkerShuffleGrouping extends Grouping {
    private static final long serialVersionUID = 7558192140873152714L;
    public MarkerShuffleGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }
    public MarkerShuffleGrouping(String componentId) {
        super(componentId);
    }
    @Override
    public Fields getFields() {
        return null;
    }
}
