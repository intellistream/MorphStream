package sesame.execution.runtime.collector.impl;
public class Meta {
    public final int src_id;
    public int index;//which index I have arrived.
    public Meta(int taskId) {
        this.src_id = taskId;
        index = 0;
    }
}
