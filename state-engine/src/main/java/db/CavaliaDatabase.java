package db;

import storage.EventManager;
import storage.StorageManager;
import storage.TableRecord;

/**
 * original designer for CavaliaDatabase: Yingjun Wu.
 */
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
