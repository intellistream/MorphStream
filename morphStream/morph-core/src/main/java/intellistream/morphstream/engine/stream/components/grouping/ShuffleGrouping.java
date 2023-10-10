package intellistream.morphstream.engine.stream.components.grouping;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public class ShuffleGrouping extends Grouping {
    private static final long serialVersionUID = 2906605250134845742L;

    public ShuffleGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }

    public ShuffleGrouping(String componentId) {
        super(componentId);
    }

    @Override
    public Fields getFields() {
        return null;
    }
}
