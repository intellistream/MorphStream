package sesame.components.grouping;
import sesame.execution.runtime.tuple.impl.Fields;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public class PartialKeyGrouping extends Grouping {
    private static final long serialVersionUID = 642110085528132186L;
    /**
     * The fields maintained in FieldsGrouping are "keys"...
     */
    private Fields fields;
    public PartialKeyGrouping(String componentId, String streamID, Fields fields) {
        super(componentId, streamID);
        this.fields = fields;
    }
    public PartialKeyGrouping(String componentId, Fields fields) {
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
