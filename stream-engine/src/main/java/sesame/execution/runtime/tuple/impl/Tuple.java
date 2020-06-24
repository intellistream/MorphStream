package sesame.execution.runtime.tuple.impl;

import sesame.components.context.TopologyContext;
import state_engine.storage.datatype.TimestampType;

import java.util.Collection;

/**
 * Created by shuhaozhang on 10/7/16.
 * TODO:Make it generic!!
 *
 * @Idea: Intelligent Brisk.execution.runtime.tuple: By accessing context, every Brisk.execution.runtime.tuple knows the global Brisk.execution condition (in a much cheap way compare to cluster)!
 * It's possible to make each Brisk.execution.runtime.tuple intelligent!!
 */
public class Tuple {

    private final int sourceId;
    //context is not going to be serialized.
    private TopologyContext context;

    //	private boolean tickerMark = false;
    private long bid;
    private Message message;
    private long[] partition_bid;


    public Tuple(long bid, int sourceId, TopologyContext context, Message message) {

        this.bid = bid;
        this.sourceId = sourceId;
        this.context = context;
        this.message = message;
    }

    public Tuple(long bid, long[] p_bid, int sourceId, TopologyContext context, Message message) {
        this.bid = bid;
        this.sourceId = sourceId;
        this.context = context;
        this.message = message;
        partition_bid = p_bid;
    }


    public TopologyContext getContext() {
        return context;
    }

    /**
     * Gets the id of the component that created this Brisk.execution.runtime.tuple.
     */
    public String getSourceComponent() {
        return context.getComponent(sourceId).getId();
    }

    public String getSourceString() {
        return context.getComponent(sourceId).getId();
    }

    public int getSourceTask() {
        return sourceId;
    }

    /**
     * Returns the number of fields in this Brisk.execution.runtime.tuple.
     */
    public int fieldSize() {
        return message.field_size;
    }

    /**
     * Gets fields in this Brisk.execution.runtime.tuple.
     */
    public Fields getFields() {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId());
    }

    /**
     * Gets the id of the stream that this Brisk.execution.runtime.tuple was emitted to.
     */
    public String getSourceStreamId() {
        return message.streamId;
    }

    private int fieldIndex(String field) {
        return getFields().fieldIndex(field);
    }

//	/**
//	 * API for obtain values of this Brisk.execution.runtime.tuple, compatible to Storm API.
//	 */
//	public LinkedList getValues() {
//		return message.getValues();
//	}

    public Object getValueByField(String field) {
        return message.getValue((fieldIndex(field)));
    }

    public Object getValue(int i) {
        return message.getValue(i);
    }

    public String getString(int i) {
        return new String(getCharArray(i));
    }

    public int getInt(int i) {
        return (int) getValue(i);
    }

    public boolean getBoolean(int i) {
        return (boolean) getValue(i);
    }

    public Long getLong(int i) {
        return (Long) getValue(i);
    }

    public Double getDoubleByField(String field) {
        return (Double) getValue(fieldIndex(field));
    }


    public String getStringByField(String field) {
        return (String) getValue(fieldIndex(field));
    }

    public short getShortByField(String field) {
        return (short) getValue(fieldIndex(field));
    }

    public int getIntegerByField(String field) {
        return (int) getValue(fieldIndex(field));
    }

    public long getLongByField(String field) {
        return (long) getValue(fieldIndex(field));
    }

    public boolean getBooleanByField(String field) {
        return (boolean) getValue(fieldIndex(field));
    }

    public double getDouble(int i) {
        return (double) getValue(i);
    }

    public TimestampType getTimestampType(int i) {
        return (TimestampType) getValue(i);
    }

//	public boolean isTickerMark() {
//		return tickerMark;
//	}
//
//	public void setTickerMark(boolean tickerMark) {
//		this.tickerMark = tickerMark;
//	}

    public boolean isMarker() {
        return this.message.isMarker();
    }

    public long getBID() {
        return bid;
    }

    public long[] getPartitionBID() {
        return partition_bid;
    }

    public char[] getCharArray(int index_field) {
        return (char[]) getValue(index_field);
    }

    public Marker getMarker() {
        return (Marker) this.message;
    }


    public Collection getValues() {
        return (Collection) this.getValue(0);
    }

    public Short getShort(int timeIdx) {

        return (short) getValue(timeIdx);
    }
}
