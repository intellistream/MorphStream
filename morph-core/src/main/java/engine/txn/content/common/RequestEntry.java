package engine.txn.content.common;

import engine.txn.storage.datatype.DataBox;

import java.util.List;

public class RequestEntry {
    //		~RequestEntry(){
    //			is_ready_ = null;
    //			data_ = null;
    //			next_ = null;
    //		}
    public long timestamp_;
    public volatile boolean[] is_ready_;// = new boolean[1];
    public List<DataBox> data_;//		char **data_;
    public RequestEntry next_;

    public RequestEntry() {
        is_ready_ = null;
        data_ = null;
        next_ = null;
    }
}
