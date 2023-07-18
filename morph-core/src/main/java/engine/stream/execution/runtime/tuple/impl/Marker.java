package engine.stream.execution.runtime.tuple.impl;

public class Marker extends Message {
    private static final long serialVersionUID = 7346698183205439095L;
    public final long msgId;//this records the ancestor message id of this message.
    public final long timeStampNano;//
    private final int myiteration;
    private long acknowledge_time;
    private String message;
    private long snapshotId;

    public Marker(String streamId, long timeStamp, long msgId, int myiteration) {
        super(streamId, 0);
        this.timeStampNano = timeStamp;
        this.msgId = msgId;
        this.myiteration = myiteration;
    }
    public Marker(String streamId, long timeStamp, long msgId, int myiteration, String message, long snapshotId) {
        super(streamId, 0);
        this.timeStampNano = timeStamp;
        this.msgId = msgId;
        this.myiteration = myiteration;
        this.message = message;
        this.snapshotId = snapshotId;
    }

    public long getAcknowledge_time() {
        return acknowledge_time;
    }

    public void setAcknowledge_time(long acknowledge_time) {
        this.acknowledge_time = acknowledge_time;
    }

    public int getMyiteration() {
        return myiteration;
    }

    @Override
    public Object getValue() {
        return null;
    }

    @Override
    public Object getValue(int index_fields) {
        return null;
    }

    @Override
    public boolean isMarker() {
        return true;
    }

    @Override
    public Marker getMarker() {
        return this;
    }
    public String getMessage() {
        return message;
    }

    public long getSnapshotId() {
        return snapshotId;
    }
}