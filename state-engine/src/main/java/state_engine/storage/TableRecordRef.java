package state_engine.storage;
import java.util.concurrent.RejectedExecutionException;
public class TableRecordRef {
    public int cnt = 0;
    private volatile TableRecord record;
    private String name;
    public boolean isEmpty() {
        return cnt == 0;
    }
    public TableRecord getRecord() {
        try {
            if (record == null) {
                throw new RejectedExecutionException();
            }
        } catch (RejectedExecutionException e) {
            System.out.println(record.getID());
            System.out.println("The record has not being assigned yet!");
//            e.printStackTrace();
        }
//        while (record == null) {
//            System.out.println("The record has not being assigned yet!" + cnt);
//        }
        return record;
    }
    public void setRecord(TableRecord record) {
        this.record = record;
        cnt++;
    }
}
