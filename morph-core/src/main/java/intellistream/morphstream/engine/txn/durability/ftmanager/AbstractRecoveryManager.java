package intellistream.morphstream.engine.txn.durability.ftmanager;

import intellistream.morphstream.engine.db.storage.record.SchemaRecord;
import intellistream.morphstream.engine.db.storage.datatype.DataBox;
import intellistream.morphstream.engine.db.storage.table.RecordSchema;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class AbstractRecoveryManager {
    public static SchemaRecord getRecord(RecordSchema recordSchema, byte[] recordString) throws UnsupportedEncodingException {
        String tableRecord = new String(recordString, StandardCharsets.UTF_8);
        String[] values = tableRecord.split(",");
        List<DataBox> boxes = recordSchema.getFieldTypes();
        for (int i = 0; i < boxes.size(); i++) {
            switch (boxes.get(i).type()) {
                case INT:
                    boxes.get(i).setInt(Integer.parseInt(values[i]));
                    break;
                case FLOAT:
                    boxes.get(i).setDouble(Double.parseDouble(values[i]));
                    break;
                case LONG:
                    boxes.get(i).setLong(Long.parseLong(values[i]));
                    break;
                case STRING:
                    boxes.get(i).setString(values[i], values[i].length());
                    break;
                case OTHERS:
                    String[] ints = values[i].split(" ");
                    for (String s : ints) {
                        boxes.get(i).getHashSet().add(Integer.parseInt(s));
                    }
                    break;
            }
        }
        return new SchemaRecord(boxes);
    }

    public static SchemaRecord getRecord(RecordSchema recordSchema, String recordString) throws UnsupportedEncodingException {
        String[] values = recordString.split(",");
        List<DataBox> boxes = recordSchema.getFieldTypes();
        for (int i = 0; i < boxes.size(); i++) {
            switch (boxes.get(i).type()) {
                case INT:
                    boxes.get(i).setInt(Integer.parseInt(values[i]));
                    break;
                case FLOAT:
                    boxes.get(i).setDouble(Double.parseDouble(values[i]));
                    break;
                case LONG:
                    boxes.get(i).setLong(Long.parseLong(values[i]));
                    break;
                case STRING:
                    boxes.get(i).setString(values[i], values[i].length());
                    break;
                case OTHERS:
                    String[] ints = values[i].split(" ");
                    for (String s : ints) {
                        boxes.get(i).getHashSet().add(Integer.parseInt(s));
                    }
                    break;
            }
        }
        return new SchemaRecord(boxes);
    }
}
