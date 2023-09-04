package intellistream.morphstream.engine.txn.transaction;

import intellistream.morphstream.api.state.StateObject;
import intellistream.morphstream.api.utils.ClientSideMetaTypes.AccessType;
import intellistream.morphstream.engine.txn.transaction.function.Condition;
import intellistream.morphstream.engine.txn.transaction.function.Function;

import java.util.ArrayList;

//TODO: For now, assume each event carries one transaction (txn_length==1)
public class TxnDescription {
    private String name; //Txn identifier, used in engine (OPScheduler) to distinguish different transactions
    private ArrayList<String> tablesToRead;
    private ArrayList<String> keyNamesToRead; //Used to retrieve condition or source keys from TransactionEvent.conditionKeyMap.get("conditionKeyName")
    private String tableToWrite;
    private String keyNameToWrite;
    private String tableToModify;
    private String keyNameToModify;
    private Function function; //TODO: Rename & abstract to user-defined function? Use java.Reflection to pass-in method

    public TxnDescription() {

    }

    public void addStateAccess(AccessType type, String tableName, String keyName) {
        if (type == AccessType.READ) {
            tablesToRead.add(tableName);
            keyNamesToRead.add(keyName);
            //TODO: keyName refers to the keyName in TransactionalEvent. System auto read key's value from TransEvent using keyName.
            // Alternatively, consider expose TransactionalEvent to client, let him directly input keyValue.
        } else if (type == AccessType.WRITE) {
            tableToWrite = tableName;
            keyNameToWrite = keyName;
        } else if (type == AccessType.MODIFY) {
            tableToModify = tableName;
            keyNameToModify = keyName;
        }
    }




}
