package components.grouping;
import execution.runtime.tuple.impl.Fields;

import java.io.Serializable;

import static common.Constants.DEFAULT_STREAM_ID;
/**
 * Created by shuhaozhang on 12/7/16.
 */
public abstract class Grouping implements Serializable {
    private static final long serialVersionUID = 2501125911542722246L;
    //source operator of this Grouping
    private final String componentId;
    //source stream of this Grouping
    private String streamID = DEFAULT_STREAM_ID;
    Grouping(String componentId, String streamID) {
        this.componentId = componentId;
        this.streamID = streamID;
    }
    Grouping(String componentId) {
        this.componentId = componentId;
    }
    public String getStreamID() {
        return streamID;
    }
    public String getComponentId() {
        return componentId;
    }
    /**
     * Obtain keys of this Grouping.
     */
    public abstract Fields getFields();
    /**
     * TODO: This is due to partition parent constraint...
     * A operator can only have one type of Grouping since it register to only one upstream op.
     */
    public boolean isShuffle() {
        return this instanceof ShuffleGrouping;
    }
    public boolean isMarkerShuffle() {
        return this instanceof MarkerShuffleGrouping;
    }
    public boolean isFields() {
        return this instanceof FieldsGrouping;
    }
    public boolean isAll() {
        return this instanceof AllGrouping;
    }
    public boolean isPartial() {
        return this instanceof PartialKeyGrouping;
    }
    public boolean isGlobal() {
        return this instanceof GlobalGrouping;
    }
}
