package storage;
import java.util.concurrent.RejectedExecutionException;
/**
 * A hack ref to SchemaRecord, simulating C++ pointer.
 */
public class SchemaRecordRef {
    public int cnt = 0;
    private volatile SchemaRecord record;
    private String name;
    public boolean isEmpty() {
        return cnt == 0;
    }
    public SchemaRecord getRecord() {
        try {
            if (record == null) {
                throw new RejectedExecutionException();
            }
        } catch (RejectedExecutionException e) {
            System.out.println(record.getId());
            System.out.println("The record has not being assigned yet!");
//            e.printStackTrace();
        }
//        while (record == null) {
//            System.out.println("The record has not being assigned yet!" + cnt);
//        }
        return record;
    }
    public void setRecord(SchemaRecord record) {
        this.record = record;
        cnt++;
    }
    /**
     * Read how many times.
     * @param name
     */
//    public void inc(String name) {
//        cnt++;
//
//        if (cnt != 1) {
//            System.nanoTime();
//        }
//        this.name = name;
//    }
}
