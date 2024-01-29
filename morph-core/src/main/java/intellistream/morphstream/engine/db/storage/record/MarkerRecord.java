package intellistream.morphstream.engine.db.storage.record;

import java.util.ArrayList;

/**
 * An empty d_record used to delineate groups in the GroupByOperator.
 */
public class MarkerRecord extends SchemaRecord {
    private static final MarkerRecord record = new MarkerRecord();

    private MarkerRecord() {
        super(new ArrayList<>());
    }

    public static MarkerRecord getMarker() {
        return MarkerRecord.record;
    }
}
