package engine.stream.components.grouping;

import engine.stream.execution.runtime.tuple.impl.Fields;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public class AllGrouping extends Grouping {
    private static final long serialVersionUID = -2721357732532321681L;

    public AllGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }

    public AllGrouping(String componentId) {
        super(componentId);
    }

    @Override
    public Fields getFields() {
        return null;
    }
}
