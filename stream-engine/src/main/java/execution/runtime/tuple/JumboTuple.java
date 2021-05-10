package execution.runtime.tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import components.context.TopologyContext;
import execution.runtime.tuple.impl.Marker;
import execution.runtime.tuple.impl.Message;
import execution.runtime.tuple.impl.Tuple;
/**
 * Created by shuhaozhang on 10/7/16.
 * TODO:Make it generic!!
 * UPDATE: now use generic value_list list in JumboTuple. -- shuhao.
 *
 * @Idea: Intelligent Brisk.execution.runtime.tuple: By accessing context, every Brisk.execution.runtime.tuple knows the global Brisk.execution condition (in a much cheap way compare to cluster)!
 * It's possible to make each Brisk.execution.runtime.tuple intelligent!!
 */
public class JumboTuple implements Comparable<JumboTuple> {
    private static final Logger LOG = LoggerFactory.getLogger(JumboTuple.class);
    public final Message[] msg;
    protected final int sourceId;
    private final TopologyContext context;//context given by producer
    //	private final int targetTasks;//this is no longer required as we avoid the usage of multi-consumer based implementation.
    public int length;//length of batch
    private final long bid;
    //context is not going to be serialized.
    public JumboTuple(JumboTuple clone) {
        this.bid = clone.bid;
        this.sourceId = clone.sourceId;
        this.length = clone.length;
        this.context = clone.context;
        msg = new Message[length];
        System.arraycopy(clone.msg, 0, msg, 0, length);
    }
    public JumboTuple(int sourceId, long bid, int length, TopologyContext context) {
        this.sourceId = sourceId;
//		this.targetTasks = targetTasks;
        this.length = length;
        this.msg = new Message[length];
        this.context = context;
        this.bid = bid;
    }
    public JumboTuple(int sourceId, long bid, int msg_size, TopologyContext context, Message... msg) {
        this.sourceId = sourceId;
//		this.targetTasks = targetTasks;
        this.context = context;
        this.length = msg_size;//the actual batch size in this tuple
        this.msg = msg;
        this.bid = bid;
    }
    /**
     * used in normal Tuple.
     *
     * @param bid
     * @param sourceId
     * @param context
     * @param message
     */
    public JumboTuple(long bid, int sourceId, TopologyContext context, Message message) {
        this.bid = bid;
        this.sourceId = sourceId;
        this.context = context;
        this.msg = new Message[1];
        this.msg[0] = message;
    }
    public int getSourceTask() {
        return sourceId;
    }
    public Tuple getTuple(int i) {
        return new Tuple(bid, sourceId, context, msg[i]);
    }
    public TopologyContext getContext() {
        return context;
    }
    public void add(Integer p, Message message) {
        msg[p] = message;
    }
    public String getSourceComponent() {
        return context.getComponent(sourceId).getId();
    }
    public String getSourceStreamId(int index_msg) {
        return msg[index_msg].streamId;
    }
    private int fieldIndex(String field, int index_msg) {
        return context.getComponentOutputFields(getSourceComponent(), getSourceStreamId(index_msg)).fieldIndex(field);
    }
    public Marker getMarker(int i) {
        return msg[i].getMarker();
    }
    public Object getValue(int index_field, int index_msg) {
        return msg[index_msg].getValue(index_field);
    }
    public Message getMsg(int index_msg) {
        return msg[index_msg];
    }
    public Object getValueByField(String field, int index_msg) {
        return msg[index_msg].getValue(fieldIndex(field, index_msg));
    }
    /**
     * @param index_field
     * @param index_msg
     * @return
     */
    public String getString(int index_field, int index_msg) {
        return (String) msg[index_msg].getValue(index_field);
    }
    public char[] getCharArray(int index_field, int index_msg) {
        return (char[]) msg[index_msg].getValue(index_field);
    }
    public int getInt(int index_field, int index_msg) {
        return (int) getValue(index_field, index_msg);
    }
    public long getLong(int index_field, int index_msg) {
        return (long) getValue(index_field, index_msg);
    }
    public double getDouble(int index_field, int index_msg) {
        return (double) getValue(index_field, index_msg);
    }
    public String getStringByField(String field, int index_msg) {
        return (String) getValueByField(field, index_msg);
    }
    public double getDoubleByField(String field, int index_msg) {
        return (double) getValueByField(field, index_msg);
    }
    public int getIntegerByField(String field, int index_msg) {
        return (int) getValueByField(field, index_msg);
    }
    public long getLongByField(String field, int index_msg) {
        return (long) getValueByField(field, index_msg);
    }
    public boolean getBooleanByField(String field, int index_msg) {
        return (boolean) getValueByField(field, index_msg);
    }
    public long getBID() {
        return bid;
    }
    @Override
    public int compareTo(JumboTuple o) {
        return Long.compare(this.bid, o.bid);
    }
}
