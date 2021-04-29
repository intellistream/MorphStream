package components;
import execution.runtime.tuple.impl.Fields;

import java.io.Serializable;
/**
 * Created by shuhaozhang on 13/7/16.
 */
public class streaminfo implements Serializable {
    private static final long serialVersionUID = 44L;
    /**
     * fields of the output stream..
     * Don't be confused with fields maintained in FieldsGrouping --> those are "keys" declared by user application.
     */
    private Fields fields;
    /**
     * TODO: add direct output type in future.
     *
     * @param fields
     * @param direct
     */
    public streaminfo(Fields fields, boolean direct) {
        this.fields = fields;
    }
    public Fields getFields() {
        return fields;
    }
    public void setFields(Fields fields) {
        this.fields = fields;
    }
}
