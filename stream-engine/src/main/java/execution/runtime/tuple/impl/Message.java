package execution.runtime.tuple.impl;

import java.io.Serializable;

public abstract class Message implements Serializable {
    private static final long serialVersionUID = 17L;
    //three components are being serialized.
    //public int sourceId;  //Executor who owns this tuple. Executor who receives this Brisk.execution.runtime.tuple would be then be able to know "who" creats and sents this Brisk.execution.runtime.tuple.
    public final String streamId;
    final int field_size;

    protected Message(String streamId, int field_size) {
//		this.sourceId = sourceId;
        this.streamId = streamId;
        this.field_size = field_size;
    }

    public abstract Object getValue();

    public abstract Object getValue(int index_fields);

    //	public abstract Object getMsg();
    public String getStreamId() {
        return streamId;
    }

    public abstract boolean isMarker();

    public abstract Marker getMarker();
}
