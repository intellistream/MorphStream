package sesame.execution.runtime.tuple.impl.msgs;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.execution.runtime.tuple.impl.Message;
public class StringLongMsg extends Message {
    private static final long serialVersionUID = -285716889341771284L;
    public final char[] str;
    public final long value;
    public StringLongMsg(String streamId, char[] str, long value) {
        super(streamId, 2);
        this.str = str;
        this.value = value;
    }
    @Override
    public Object getValue() {
        return str + "" + value;
    }
    @Override
    public Object getValue(int index_fields) {
        switch (index_fields) {
            case 0:
                return str;
            case 1:
                return value;
            default:
                throw new IndexOutOfBoundsException(String.valueOf(index_fields));
        }
    }
    @Override
    public boolean isMarker() {
        return false;
    }
    @Override
    public Marker getMarker() {
        return null;
    }
}
