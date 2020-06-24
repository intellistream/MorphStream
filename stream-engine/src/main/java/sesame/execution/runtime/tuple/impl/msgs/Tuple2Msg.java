package sesame.execution.runtime.tuple.impl.msgs;
import sesame.execution.runtime.tuple.impl.Marker;
import sesame.execution.runtime.tuple.impl.Message;
public class Tuple2Msg<T0, T1> extends Message {
    private static final long serialVersionUID = -1296552220431181613L;
    public final T0 value0;
    public final T1 value1;
    public Tuple2Msg(String streamId, T0 value0, T1 value1) {
        super(streamId, 2);
        this.value0 = value0;
        this.value1 = value1;
    }
    @Override
    public Object getValue() {
        return value0 + "" + value1;
    }
    @Override
    public Object getValue(int index_fields) {
        switch (index_fields) {
            case 0:
                return value0;
            case 1:
                return value1;
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
