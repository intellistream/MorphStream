package engine.stream.execution.runtime.tuple.impl.msgs;

import engine.stream.execution.runtime.tuple.impl.Marker;
import engine.stream.execution.runtime.tuple.impl.Message;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * Created by tony on 5/30/2017.
 * <p>
 * General Message.
 * For performance consideration, do not use this.
 */
public class GeneralMsg<T> extends Message {
    private static final long serialVersionUID = -1840662997355503904L;
    public final LinkedList<T> values = new LinkedList<>();

    public GeneralMsg(String streamId, T... values) {
        super(streamId, values.length);
        this.values.addAll(Arrays.asList(values));
    }

    public GeneralMsg(String streamId, T values) {
        super(streamId, 1);
        this.values.add(values);
    }

    @Override
    public Marker getMarker() {
        return null;
    }

    @Override
    public Object getValue() {
        return values;
    }

    @Override
    public T getValue(int index_fields) {
        return values.get(index_fields);
    }

    public boolean isMarker() {
        return false;
    }
}
