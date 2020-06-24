package state_engine;
import state_engine.storage.EventManager;
import state_engine.storage.StorageManager;
import state_engine.storage.TableRecord;
public class CavaliaDatabase extends Database {
    public CavaliaDatabase(String path) {
        storageManager = new StorageManager();
        eventManager = new EventManager();
    }
    /**
     * @param table
     * @param record
     * @throws DatabaseException
     */
    @Override
    public void InsertRecord(String table, TableRecord record) throws DatabaseException {
        storageManager.InsertRecord(table, record);
    }
}
