package engine.stream.execution.runtime.tuple.impl.msgs;

import engine.stream.execution.runtime.tuple.impl.Message;
import engine.stream.execution.runtime.tuple.impl.Marker;

public class IntDoubleDoubleMsg extends Message {
    private static final long serialVersionUID = -285716889341771284L;
    public final int v1;
    public final double v2;
    public final double v3;

    public IntDoubleDoubleMsg(String streamId, int _v1, double _v2, double _v3) {
        super(streamId, 3);
        v1 = _v1;
        v2 = _v2;
        v3 = _v3;
    }

    @Override
    public Object getValue() {
        return v1 + "" + v2 + "" + v3;
    }

    @Override
    public Object getValue(int index_fields) {
        switch (index_fields) {
            case 0:
                return v1;
            case 1:
                return v2;
            case 2:
                return v3;
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
