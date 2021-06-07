package components.grouping;

import execution.runtime.tuple.impl.Fields;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public class GlobalGrouping extends Grouping {
    private static final long serialVersionUID = -7376300455364041870L;

    public GlobalGrouping(String componentId, String streamID) {
        super(componentId, streamID);
    }

    public GlobalGrouping(String componentId) {
        super(componentId);
    }

    @Override
    public Fields getFields() {
        return null;
    }
}
