package intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.msgs;

import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Marker;
import intellistream.morphstream.engine.stream.execution.runtime.tuple.impl.Message;

public class StringMsg extends Message {
    private static final long serialVersionUID = -8382182763339974896L;
    public final char[] str;

    public StringMsg(String streamId, char[] str) {
        super(streamId, 1);
        this.str = str;
    }

    @Override
    public char[] getValue() {
        return str;
    }

    @Override
    public char[] getValue(int index_fields) {
        return str;
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
