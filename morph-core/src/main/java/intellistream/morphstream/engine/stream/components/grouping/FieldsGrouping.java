package intellistream.morphstream.engine.stream.components.grouping;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Fields;

/**
 * Created by shuhaozhang on 12/7/16.
 */
public class FieldsGrouping extends Grouping {
    private static final long serialVersionUID = 6314571320061895100L;
    /**
     * The fields maintained in FieldsGrouping are "keys"...
     */
    private Fields fields;

    public FieldsGrouping(String componentId, String streamID, Fields fields) {
        super(componentId, streamID);
        this.fields = fields;
    }

    public FieldsGrouping(String componentId, Fields fields) {
        super(componentId);
        this.fields = fields;
    }

    public Fields getFields() {
        return fields;
    }

    public void setFields(Fields fields) {
        this.fields = fields;
    }
}
